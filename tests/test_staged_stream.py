"""Phased stage-engine streaming: the hub-owned answer -> (optional validation) -> in-depth
flow that the chartsearchai controller relays. Stage helpers are stubbed so these assert the
generator's CONTRACT — event sequence, conditional validation, and hub-side reference resolution —
not the LLM. A separate test pins the cancellation invariant (a cancelled _chat frees the router lock)."""

import asyncio
import json
from contextvars import ContextVar

import pytest

import server.openai_compat as openai_compat
import server.engine as engine
import server.team as team
from server.context_sources import InsufficientContextError
from server.levels_loader import get_profile
from tests.factories import (
    make_profile,
    patient_source_registry,
    run_profile,
    stream_profile,
)

_MAPPINGS = [
    {
        "index": 1,
        "resourceType": "obs",
        "resourceUuid": "u1",
        "date": "2025-01-01",
        "text": "observed data",
    },
    {
        "index": 2,
        "resourceType": "order",
        "resourceUuid": "u2",
        "date": "2025-02-02",
        "text": "different order",
    },
]
_TEST_SOURCE = patient_source_registry("[1] obs\n[2] order\n", _MAPPINGS)


def _stub_common(monkeypatch):
    def fake_gate(**k):
        return (
            k["answer_text"],
            k["citations"],
            k["blocks"],
            {"mode": "off", "status": "ok", "applied": "none"},
            None,
        )

    async def fake_indepth(*_a, **_k):
        return (["claim one [1]", "claim two [2]"], {"level": "green", "note": ""})

    async def fake_unreviewed_indepth(*_a, **_k):
        return ["claim one [1]", "claim two [2]"]

    async def fake_ground(_client, _model, _answer, references, _mappings):
        # Deterministic stand-in for the real entailment call: these sequencing/wiring tests care
        # that grounding runs exactly once, after review, on the FINAL references — not what a real
        # LLM would verdict. The entailment call itself has its own dedicated tests below.
        out = []
        for ref in references:
            r = dict(ref)
            r["grounded"] = True
            r["groundingStatus"] = "verified"
            out.append(r)
        return out

    monkeypatch.setattr(team, "_apply_temporal_gate", fake_gate)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_unreviewed_indepth)
    monkeypatch.setattr(team, "_merge_temporal_gate_conf", lambda conf, _gate: conf)
    monkeypatch.setattr(team, "_write_trace", lambda *_a, **_k: None)
    monkeypatch.setattr(team, "_ground_references", fake_ground)


def _product_profile(
    answer_model="M",
    review_model=None,
    *,
    orchestrator_model=None,
    expert_model=None,
):
    stages = ["context"]
    models = {}
    prompts = {}
    topology = "single"
    if orchestrator_model:
        topology = "team"
        stages.append("gather")
        models["orchestrator"] = orchestrator_model
        prompts["orchestrator"] = "orchestrator"
        if expert_model:
            models["expert"] = expert_model
            prompts["expert"] = "medical_expert"
    stages.extend(["answer", "gate", "resolve_refs"])
    models["answer"] = answer_model
    prompts["answer"] = "synthesis-answer"
    if review_model:
        stages.extend(["review", "gate"])
        models["review"] = review_model
        prompts["review"] = "validation-rewrite"
    stages.extend(["final_resolve_refs", "ground_verdicts", "indepth", "indepth_gate"])
    models["grounding"] = answer_model
    models["indepth"] = answer_model
    prompts["indepth"] = "synthesis-indepth"
    return make_profile(
        topology=topology,
        stages=stages,
        models=models,
        prompts=prompts,
        output="product",
        capabilities={"staged": True, "validation": bool(review_model)},
    )


def _collect(profile):
    async def _run():
        out = []
        async for name, data in stream_profile(
            profile,
            [{"role": "user", "content": "q?"}],
            patient="p",
            context={"temporal": False},
            model_label="lvl",
            source_registry=_TEST_SOURCE,
        ):
            out.append((name, json.loads(data)))
        return out

    return asyncio.run(_run())


def test_staged_stream_with_validator_emits_full_phase_sequence(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    async def fake_validate(_client, **_k):
        assert "validator_model" not in _k
        return (
            _k["answer_text"],
            _k["citations"],
            _k["blocks"],
            {"level": "green", "note": ""},
        )

    rewrite_calls = 0

    async def fake_rewrite(*_args, **_kwargs):
        nonlocal rewrite_calls
        rewrite_calls += 1
        if rewrite_calls == 1:
            return {
                "answer_ok": False,
                "errors": [{"chart": "Use record 2."}],
                "corrected_answer": "Ans fixed [2].",
            }
        return {"answer_ok": True, "errors": []}

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_validate)
    monkeypatch.setattr(team, "_validate_answer_rewrite", fake_rewrite)

    events = _collect(_product_profile(review_model="V"))
    names = [n for n, _ in events]
    assert names == [
        "answer_done",
        "answer_validation",
        "indepth_pending",
        "indepth_done",
        "done",
    ]

    ev = dict(events)
    # answer_done: fast answer, marked "validating", references RESOLVED by the hub (not indices)
    assert ev["answer_done"]["answerValidation"]["status"] == "validating"
    fast_ref = ev["answer_done"]["references"][0]
    assert fast_ref["index"] == 1
    assert fast_ref["sourceId"] == "test:obs:u1"
    assert fast_ref["resourceUuid"] == "u1"
    assert fast_ref["resolutionStatus"] == "resolved"
    assert fast_ref["groundingStatus"] == "checking"
    assert fast_ref["usage"] == [{"location": "answer", "text": "Ans [1]."}]
    # answer_validation: correction and final grounding land atomically for the post-review answer.
    assert ev["answer_validation"]["answerValidation"]["status"] == "edited"
    assert ev["answer_validation"]["answerValidation"]["originalAnswer"] == "Ans [1]."
    checked_ref = ev["answer_validation"]["references"][0]
    assert checked_ref["index"] == 2
    assert checked_ref["resourceUuid"] == "u2"
    assert checked_ref["resolutionStatus"] == "resolved"
    assert checked_ref["groundingStatus"] == "verified"
    # done: in-depth complete
    assert ev["done"]["inDepth"]["status"] == "complete"
    assert "claim one" in ev["done"]["inDepth"]["answer"]
    assert ev["indepth_done"]["inDepth"] == ev["done"]["inDepth"]
    assert ev["indepth_done"]["answer"] == ev["done"]["answer"]
    assert "status" not in ev["indepth_done"]
    assert ev["done"]["inDepth"]["error"] == ""
    assert ev["done"]["model"] == "lvl"
    final_ref = ev["done"]["references"][0]
    assert final_ref["index"] == 2
    assert final_ref["groundingStatus"] == "verified"
    assert final_ref["grounded"] is True


def test_post_review_punctuation_rewrite_preserves_usable_answer_and_needs_review(
    monkeypatch,
):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Useful answer [1].", [1], []

    async def fake_validate(_client, **kwargs):
        assert "validator_model" not in kwargs
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def punctuation_rewrite(*_args, **_kwargs):
        return {
            "answer_ok": False,
            "errors": [{"chart": "The rewrite is unusable."}],
            "corrected_answer": ".",
        }

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_validate)
    monkeypatch.setattr(team, "_validate_answer_rewrite", punctuation_rewrite)

    events = dict(_collect(_product_profile(review_model="V")))

    assert events["done"]["answer"] == "Useful answer [1]."
    assert events["done"]["answerValidation"]["status"] == "needs_review"
    assert events["done"]["confidence"]["answer"]["level"] == "red"


def test_preliminary_problem_stays_validating_until_configured_review_finishes(
    monkeypatch,
):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Claim with a missing source [99].", [99], []

    async def fake_validate(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def clean_review(*_args, **_kwargs):
        return {"answer_ok": True, "errors": []}

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_validate)
    monkeypatch.setattr(team, "_validate_answer_rewrite", clean_review)

    events = dict(_collect(_product_profile(review_model="V")))

    fast_validation = events["answer_done"]["answerValidation"]
    assert fast_validation["status"] == "validating"
    assert any(issue["id"] == "citation_resolution" for issue in fast_validation["issues"])
    assert events["answer_validation"]["answerValidation"]["status"] == "needs_review"


def test_indepth_unresolved_citation_is_not_displayed(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Useful answer [1].", [1], []

    async def fake_indepth(*_args, **_kwargs):
        return ["Supported context [1].", "Invented context [99]."], {
            "level": "green",
            "note": "",
        }

    async def unchanged(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)

    events = dict(_collect(_product_profile(review_model="V")))

    assert "Supported context [1]." in events["done"]["inDepth"]["answer"]
    assert "[99]" not in events["done"]["inDepth"]["answer"]
    reference = next(ref for ref in events["done"]["references"] if ref["index"] == 1)
    assert any(usage["location"] == "indepth" for usage in reference["usage"])


def test_staged_stream_without_validator_skips_validation_phase(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)

    async def substance_only(_client, **kwargs):
        assert "validator_model" not in kwargs
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    monkeypatch.setattr(team, "_ensure_substantive_answer", substance_only)

    events = _collect(_product_profile())
    names = [n for n, _ in events]
    assert names == ["answer_done", "indepth_pending", "indepth_done", "done"]
    # No LLM review event, but final grounding still completes before the answer settles.
    assert dict(events)["answer_done"]["answerValidation"]["status"] == "validating"
    assert dict(events)["answer_done"]["references"][0]["groundingStatus"] == "checking"
    assert dict(events)["indepth_pending"]["answerValidation"]["status"] == "checked"
    assert dict(events)["indepth_pending"]["references"][0]["groundingStatus"] == "verified"
    assert dict(events)["done"]["references"][0]["groundingStatus"] == "verified"
    assert dict(events)["done"]["inDepth"]["status"] == "complete"


def test_answer_done_timing_separates_answer_work_from_pipeline_overhead(monkeypatch):
    _stub_common(monkeypatch)
    traces = []

    async def fake_answer(*_args, **_kwargs):
        return ("Ans [1].", [1], [])

    async def fake_validate(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_validate)
    monkeypatch.setattr(
        team, "_write_trace", lambda *_args, **kwargs: traces.append(kwargs)
    )

    _collect(_product_profile())

    timing = next(
        step for step in traces[0]["steps"] if step["role"] == "answer_timing"
    )
    assert timing["answer_stage_ms"] >= 0
    assert timing["answer_to_done_ms"] >= timing["answer_stage_ms"]
    assert timing["pipeline_overhead_ms"] == (
        timing["answer_to_done_ms"] - timing["answer_stage_ms"]
    )
    assert 0 <= timing["pipeline_overhead_ratio"] <= 1


def test_non_substantive_product_answer_withholds_indepth(monkeypatch):
    _stub_common(monkeypatch)

    async def fallback_answer(*_args, **_kwargs):
        return (team.FALLBACK_ANSWER, [], [])

    async def fallback_validate(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "red", "note": "The Answer is not substantive."},
        )

    async def indepth_must_not_run(*_args, **_kwargs):
        raise AssertionError("In-Depth must not run from a non-substantive Answer")

    monkeypatch.setattr(team, "_synthesize_answer", fallback_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fallback_validate)
    monkeypatch.setattr(team, "_gen_indepth", indepth_must_not_run)
    monkeypatch.setattr(team, "_synthesize_indepth", indepth_must_not_run)

    events = _collect(_product_profile())
    assert [name for name, _payload in events] == [
        "answer_done",
        "indepth_pending",
        "indepth_error",
        "done",
    ]
    final = dict(events)["done"]
    assert final["answer"] == team.FALLBACK_ANSWER
    assert final["inDepth"]["status"] == "needs_review"
    assert final["inDepth"]["answer"] == ""
    assert "not substantive" in final["inDepth"]["error"]


def test_stage_drain_returns_final_post_review_envelope(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_a, **_k):
        return ("Ans [1].", [1], [])

    async def fake_validate(_client, **_k):
        assert "validator_model" not in _k
        return (
            _k["answer_text"],
            _k["citations"],
            _k["blocks"],
            {"level": "green", "note": ""},
        )

    rewrite_calls = 0

    async def fake_rewrite(*_args, **_kwargs):
        nonlocal rewrite_calls
        rewrite_calls += 1
        if rewrite_calls == 1:
            return {
                "answer_ok": False,
                "errors": [{"chart": "Use record 2."}],
                "corrected_answer": "Ans fixed [2].",
            }
        return {"answer_ok": True, "errors": []}

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_validate)
    monkeypatch.setattr(team, "_validate_answer_rewrite", fake_rewrite)

    async def fake_indepth(*_args, **_kwargs):
        return ["In-Depth-only support [1]."], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)

    async def _run():
        return await run_profile(
            _product_profile(review_model="V"),
            [{"role": "user", "content": "q?"}],
            patient="p",
            context={"temporal": False},
            model_label="single-12b-checked",
            source_registry=_TEST_SOURCE,
        )

    env = json.loads(asyncio.run(_run()))
    assert env["answer"] == "Ans fixed [2]."
    assert env["citations"] == [2]
    assert {reference["index"] for reference in env["references"]} == {1, 2}
    assert env["answerValidation"]["status"] == "edited"
    assert env["inDepth"]["status"] == "complete"
    assert env["references"][0]["index"] == 2
    assert env["references"][0]["groundingStatus"] == "verified"


def _fake_chat_returning_verdicts(verdicts_by_call):
    """Stub for team._chat that returns the next queued verdicts list as an entailment response."""
    calls = []

    async def fake_chat(_client, model, messages, **kwargs):
        calls.append({"model": model, "messages": messages, **kwargs})
        verdicts = verdicts_by_call[len(calls) - 1]
        return {"content": json.dumps({"verdicts": verdicts})}

    return fake_chat, calls


def test_entailment_verdicts_maps_yes_no_positionally(monkeypatch):
    fake_chat, calls = _fake_chat_returning_verdicts([["YES", "NO"]])
    monkeypatch.setattr(team, "_chat", fake_chat)

    async def _run():
        return await team._entailment_verdicts(
            client=None,
            model="M",
            pairs=[("source A", "claim A"), ("source B", "claim B")],
        )

    verdicts = asyncio.run(_run())
    assert verdicts == [True, False]
    assert len(calls) == 1  # ONE batched call for both pairs, not one per pair
    assert calls[0]["response_format"]["json_schema"]["name"] == "entailment_verdicts"


def test_entailment_verdicts_fails_open_to_unchecked_on_call_error(monkeypatch):
    async def broken_chat(*_a, **_k):
        raise RuntimeError("router unreachable")

    monkeypatch.setattr(team, "_chat", broken_chat)

    async def _run():
        return await team._entailment_verdicts(
            client=None, model="M", pairs=[("s", "c")]
        )

    assert asyncio.run(_run()) == [None]


def test_entailment_verdicts_fails_open_on_malformed_response(monkeypatch):
    async def malformed_chat(*_a, **_k):
        return {"content": "not json"}

    monkeypatch.setattr(team, "_chat", malformed_chat)

    async def _run():
        return await team._entailment_verdicts(
            client=None, model="M", pairs=[("s", "c")]
        )

    assert asyncio.run(_run()) == [None]


def test_ground_references_verified_for_an_entailed_paraphrase_despite_low_word_overlap(
    monkeypatch,
):
    # The old lexical heuristic required ~45% token overlap; a clean paraphrase like this would have
    # scored well below that and come back unsupported/unchecked. Entailment must verify it anyway.
    fake_chat, calls = _fake_chat_returning_verdicts([["YES"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [
        {
            "index": 4,
            "resourceType": "encounter",
            "resourceUuid": "enc-4",
            "date": "2026-01-26",
        }
    ]
    mappings = [
        {
            "index": 4,
            "resourceType": "encounter",
            "resourceUuid": "enc-4",
            "date": "2026-01-26",
            "text": "Pt seen today for a routine checkup; no acute concerns raised.",
        }
    ]

    async def _run():
        return await team._ground_references(
            client=None,
            model="M",
            answer="The patient had a well visit on 2026-01-26 [4].",
            references=refs,
            mappings=mappings,
        )

    grounded = asyncio.run(_run())
    assert grounded[0]["groundingStatus"] == "verified"
    assert grounded[0]["grounded"] is True
    assert len(calls) == 1


def test_ground_references_checks_a_multi_citation_claim_against_combined_sources(
    monkeypatch,
):
    fake_chat, calls = _fake_chat_returning_verdicts([["YES"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    statement = "Weight decreased from 74 kg on 2006-03-16 to 71 kg on 2006-06-06 [1][2]."
    refs = [
        {
            "index": 1,
            "resourceType": "obs",
            "resourceUuid": "weight-1",
            "date": "2006-03-16",
            "usage": [{"location": "answer", "text": statement}],
        },
        {
            "index": 2,
            "resourceType": "obs",
            "resourceUuid": "weight-2",
            "date": "2006-06-06",
            "usage": [{"location": "answer", "text": statement}],
        },
    ]
    mappings = [
        {"index": 1, "date": "2006-03-16", "text": "Weight (kg): 74 kg"},
        {"index": 2, "date": "2006-06-06", "text": "Weight (kg): 71 kg"},
    ]

    async def _run():
        return await team._ground_references(None, "M", statement, refs, mappings)

    grounded = asyncio.run(_run())
    assert [item["groundingStatus"] for item in grounded] == ["verified", "verified"]
    assert [item["groundingScope"] for item in grounded] == ["source_set", "source_set"]
    assert [item["groundingGroup"] for item in grounded] == [[1, 2], [1, 2]]
    assert all(
        item["groundingChecks"] == [
            {
                "status": "verified",
                "claim": "Weight decreased from 74 kg on 2006-03-16 to 71 kg on 2006-06-06 .",
                "location": "answer",
                "path": "",
                "source_indices": [1, 2],
            }
        ]
        for item in grounded
    )
    prompt = calls[0]["messages"][0]["content"]
    assert prompt.count("PAIR ") == 1
    assert "[1] 2006-03-16 Weight (kg): 74 kg" in prompt
    assert "[2] 2006-06-06 Weight (kg): 71 kg" in prompt


def test_ground_references_caps_sources_within_one_claim(monkeypatch):
    fake_chat, _calls = _fake_chat_returning_verdicts([["YES"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    total = team._ENTAILMENT_MAX_SOURCES_PER_CLAIM + 3
    statement = "A collectively supported claim " + "".join(
        f"[{index}]" for index in range(1, total + 1)
    )
    refs = [
        {
            "index": index,
            "usage": [{"location": "answer", "text": statement}],
            "resolutionStatus": "resolved",
        }
        for index in range(1, total + 1)
    ]
    mappings = [
        {"index": index, "date": "2026-01-01", "text": f"source {index}"}
        for index in range(1, total + 1)
    ]

    async def _run():
        return await team._ground_references(None, "M", statement, refs, mappings)

    grounded = asyncio.run(_run())
    assert [item["groundingStatus"] for item in grounded].count("verified") == (
        team._ENTAILMENT_MAX_SOURCES_PER_CLAIM
    )
    assert [item["groundingStatus"] for item in grounded].count("unchecked") == 3


def test_ground_references_preserves_mixed_claim_level_verdicts(monkeypatch):
    fake_chat, _calls = _fake_chat_returning_verdicts([["YES", "NO"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [
        {
            "index": 1,
            "resolutionStatus": "resolved",
            "usage": [
                {"location": "answer", "path": "", "text": "Supported claim [1]."},
                {"location": "answer", "path": "", "text": "Unsupported claim [1]."},
            ],
        }
    ]
    mappings = [{"index": 1, "date": "2026-01-01", "text": "source one"}]

    async def _run():
        return await team._ground_references(
            None,
            "M",
            "Supported claim [1]. Unsupported claim [1].",
            refs,
            mappings,
        )

    grounded = asyncio.run(_run())
    assert grounded[0]["groundingStatus"] == "mixed"
    assert grounded[0]["grounded"] is None
    assert [check["status"] for check in grounded[0]["groundingChecks"]] == [
        "verified",
        "unsupported",
    ]


def test_ground_references_unsupported_for_high_overlap_but_negated_statement(
    monkeypatch,
):
    # High lexical overlap (shares "diabetes", "family history") but describes a RELATIVE, not the
    # patient — the failure mode the token-overlap heuristic could not catch.
    fake_chat, calls = _fake_chat_returning_verdicts([["NO"]])
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [
        {
            "index": 7,
            "resourceType": "obs",
            "resourceUuid": "obs-7",
            "date": "2026-01-01",
        }
    ]
    mappings = [
        {
            "index": 7,
            "resourceType": "obs",
            "resourceUuid": "obs-7",
            "date": "2026-01-01",
            "text": "Family history of diabetes (mother). Patient's own glucose panel is normal.",
        }
    ]

    async def _run():
        return await team._ground_references(
            client=None,
            model="M",
            answer="The patient has a diagnosis of diabetes [7].",
            references=refs,
            mappings=mappings,
        )

    grounded = asyncio.run(_run())
    assert grounded[0]["groundingStatus"] == "unsupported"
    assert grounded[0]["grounded"] is False
    assert len(calls) == 1


def test_ground_references_checks_all_pairs_in_stable_bounded_batches(monkeypatch):
    n = team._ENTAILMENT_MAX_PAIRS + 3
    fake_chat, calls = _fake_chat_returning_verdicts(
        [["YES"] * team._ENTAILMENT_MAX_PAIRS, ["YES"] * 3]
    )
    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [
        {
            "index": i,
            "resourceType": "obs",
            "resourceUuid": f"u{i}",
            "date": "2026-01-01",
        }
        for i in range(1, n + 1)
    ]
    mappings = [
        {
            "index": i,
            "resourceType": "obs",
            "resourceUuid": f"u{i}",
            "date": "2026-01-01",
            "text": f"finding number {i}",
        }
        for i in range(1, n + 1)
    ]
    answer = " ".join(f"Claim about finding {i} [{i}]." for i in range(1, n + 1))

    async def _run():
        return await team._ground_references(
            client=None, model="M", answer=answer, references=refs, mappings=mappings
        )

    grounded = asyncio.run(_run())
    assert len(calls) == 2
    assert calls[0]["messages"][0]["content"].count("PAIR ") == team._ENTAILMENT_MAX_PAIRS
    assert calls[1]["messages"][0]["content"].count("PAIR ") == 3
    verified = [r for r in grounded if r["groundingStatus"] == "verified"]
    unchecked = [r for r in grounded if r["groundingStatus"] == "unchecked"]
    assert len(verified) == n
    assert unchecked == []


def test_ground_references_isolates_a_failed_later_batch(monkeypatch):
    n = team._ENTAILMENT_MAX_PAIRS + 3
    calls = []

    async def fake_chat(_client, _model, messages, **_kwargs):
        calls.append(messages)
        if len(calls) == 2:
            raise RuntimeError("second batch failed")
        return {
            "content": json.dumps(
                {"verdicts": ["YES"] * team._ENTAILMENT_MAX_PAIRS}
            )
        }

    monkeypatch.setattr(team, "_chat", fake_chat)
    refs = [
        {
            "index": i,
            "resourceType": "obs",
            "resourceUuid": f"u{i}",
            "date": "2026-01-01",
        }
        for i in range(1, n + 1)
    ]
    mappings = [
        {
            "index": i,
            "resourceType": "obs",
            "resourceUuid": f"u{i}",
            "date": "2026-01-01",
            "text": f"finding number {i}",
        }
        for i in range(1, n + 1)
    ]
    answer = " ".join(f"Claim about finding {i} [{i}]." for i in range(1, n + 1))

    grounded = asyncio.run(
        team._ground_references(None, "M", answer, refs, mappings)
    )

    assert len(calls) == 2
    assert [r["groundingStatus"] for r in grounded[: team._ENTAILMENT_MAX_PAIRS]] == [
        "verified"
    ] * team._ENTAILMENT_MAX_PAIRS
    assert [r["groundingStatus"] for r in grounded[team._ENTAILMENT_MAX_PAIRS :]] == [
        "unchecked"
    ] * 3


def test_grounding_batch_splits_until_each_request_fits(monkeypatch):
    calls = []

    async def limited(_client, _model, pairs):
        calls.append(len(pairs))
        if len(pairs) > 2:
            raise InsufficientContextError(
                "grounding batch exceeds context", mandatory_ids=()
            )
        return [True] * len(pairs)

    monkeypatch.setattr(team, "_entailment_verdicts", limited)

    verdicts = asyncio.run(
        team._bounded_entailment_verdicts(
            None,
            "M",
            [(f"source {index}", f"claim {index}") for index in range(5)],
        )
    )

    assert verdicts == [True] * 5
    assert calls == [5, 2, 3, 1, 2]


def test_product_long_table_finishes_checked_after_all_grounding_batches(
    monkeypatch,
):
    total = team._ENTAILMENT_MAX_PAIRS + 3
    mappings = [
        {
            "index": index,
            "resourceType": "obs",
            "resourceUuid": f"u{index}",
            "date": "2026-01-01",
            "text": f"finding number {index}",
        }
        for index in range(1, total + 1)
    ]
    source = patient_source_registry(
        "".join(
            f"[{item['index']}] {item['text']}\n" for item in mappings
        ),
        mappings,
    )

    def fake_gate(**kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"mode": "enforce", "status": "not_applicable", "applied": "none"},
            None,
        )

    async def fake_answer(*_args, **_kwargs):
        return (
            "The documented findings are listed in the table.",
            [],
            [
                {
                    "kind": "table",
                    "title": "Findings",
                    "columns": [{"key": "finding", "label": "Finding"}],
                    "rows": [
                        {
                            "cells": {
                                "finding": {
                                    "text": f"finding number {index}",
                                    "refs": [index],
                                }
                            }
                        }
                        for index in range(1, total + 1)
                    ],
                }
            ],
        )

    async def keep_answer(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def fake_indepth(*_args, **_kwargs):
        return ["Finding 1 is documented [1]."]

    fake_chat, calls = _fake_chat_returning_verdicts(
        [
            ["YES"] * team._ENTAILMENT_MAX_PAIRS,
            ["YES"] * 3,
            ["YES"],
        ]
    )
    monkeypatch.setattr(team, "_apply_temporal_gate", fake_gate)
    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", keep_answer)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)
    monkeypatch.setattr(team, "_chat", fake_chat)
    monkeypatch.setattr(team, "_write_trace", lambda *_args, **_kwargs: None)

    async def collect():
        return dict(
            [
                (name, json.loads(data))
                async for name, data in stream_profile(
                    _product_profile(),
                    [{"role": "user", "content": "List the findings."}],
                    patient="p",
                    context={"temporal": False},
                    model_label="lvl",
                    source_registry=source,
                )
            ]
        )

    final = asyncio.run(collect())["done"]
    assert len(calls) == 3
    assert final["answerValidation"]["status"] == "checked"
    assert len(final["references"]) == total
    assert all(
        reference["groundingStatus"] == "verified"
        for reference in final["references"]
    )


def test_product_long_indepth_keeps_all_verified_claims(monkeypatch):
    total = team._ENTAILMENT_MAX_PAIRS + 3
    mappings = [
        {
            "index": index,
            "resourceType": "obs",
            "resourceUuid": f"u{index}",
            "date": "2026-01-01",
            "text": f"finding number {index}",
        }
        for index in range(1, total + 1)
    ]
    source = patient_source_registry(
        "".join(f"[{item['index']}] {item['text']}\n" for item in mappings),
        mappings,
    )

    def fake_gate(**kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"mode": "enforce", "status": "not_applicable", "applied": "none"},
            None,
        )

    async def fake_answer(*_args, **_kwargs):
        return "Finding 1 is documented [1].", [1], []

    async def keep_answer(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def fake_indepth(*_args, **_kwargs):
        return [
            f"Finding {index} is documented [{index}]."
            for index in range(1, total + 1)
        ]

    fake_chat, calls = _fake_chat_returning_verdicts(
        [["YES"], ["YES"] * team._ENTAILMENT_MAX_PAIRS, ["YES"] * 3]
    )
    monkeypatch.setattr(team, "_apply_temporal_gate", fake_gate)
    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", keep_answer)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)
    monkeypatch.setattr(team, "_chat", fake_chat)
    monkeypatch.setattr(team, "_write_trace", lambda *_args, **_kwargs: None)

    async def collect():
        return dict(
            [
                (name, json.loads(data))
                async for name, data in stream_profile(
                    _product_profile(),
                    [{"role": "user", "content": "Explain the findings."}],
                    patient="p",
                    context={"temporal": False},
                    model_label="lvl",
                    source_registry=source,
                )
            ]
        )

    final = asyncio.run(collect())["done"]
    assert len(calls) == 3
    assert final["inDepth"]["status"] == "complete"
    assert final["inDepth"]["answer"].count("\n-") + 1 == total
    assert len(final["references"]) == total


def test_gen_indepth_refits_review_and_retry_subcalls(monkeypatch):
    synth_calls = []
    review_charts = []
    fitted_retry_messages = []

    async def synth(
        _client,
        _model,
        base_messages,
        *_args,
        extra_msgs=None,
        **_kwargs,
    ):
        synth_calls.append((base_messages, extra_msgs))
        return ["Initial claim [1]."] if extra_msgs is None else ["Revised claim [1]."]

    async def review(_client, _model, *, chart, claims, **_kwargs):
        review_charts.append(chart)
        return (
            {"drop": [1], "issues": "retry"}
            if claims[0].startswith("Initial")
            else {"drop": [], "issues": ""}
        )

    async def fit_review(claims):
        return "review chart for " + claims[0]

    async def fit_retry(extra_msgs):
        fitted_retry_messages.extend(extra_msgs)
        return [{"role": "user", "content": "retry chart"}]

    monkeypatch.setattr(team, "_synthesize_indepth", synth)
    monkeypatch.setattr(team, "_validate_indepth_verdict", review)

    claims, confidence = asyncio.run(
        team._gen_indepth(
            None,
            "writer",
            [{"role": "user", "content": "answer chart"}],
            "instruction",
            "gathered",
            "answer",
            validator_model="reviewer",
            validator_prompt="validation-rewrite",
            chart="answer chart",
            synth_temperature=0,
            synth_repeat_penalty=None,
            synth_dry=None,
            validator_temperature=0,
            validator_repeat_penalty=None,
            validator_dry=None,
            max_tokens=64,
            max_loops=1,
            steps=[],
            review_context_fitter=fit_review,
            retry_context_fitter=fit_retry,
        )
    )

    assert claims == ["Revised claim [1]."]
    assert confidence["status"] == "edited"
    assert review_charts == [
        "review chart for Initial claim [1].",
        "review chart for Revised claim [1].",
    ]
    assert synth_calls[1][0] == [{"role": "user", "content": "retry chart"}]
    assert synth_calls[1][1] == fitted_retry_messages


def test_product_indepth_mandatory_overflow_has_structured_terminal_metadata(
    monkeypatch,
):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def overflow(*_args, **_kwargs):
        raise InsufficientContextError(
            "mandatory evidence cannot fit", mandatory_ids=("source-1",)
        )

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(engine, "_select_indepth_context", overflow)

    events = dict(_collect(_product_profile()))
    assert events["indepth_error"]["inDepth"]["errorCode"] == "insufficient_context"
    assert events["done"]["inDepth"]["mandatorySourceIds"] == ["source-1"]
    assert events["done"]["answer"] == "Supported answer [1]."


def test_product_review_context_overflow_reaches_structured_terminal_metadata(
    monkeypatch,
):
    real_gen_indepth = team._gen_indepth
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def keep_answer(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def clean_review(*_args, **_kwargs):
        return {"answer_ok": True, "errors": []}

    async def fake_indepth(*_args, **_kwargs):
        return ["Supported context [1]."]

    async def review_overflow(*_args, **_kwargs):
        raise InsufficientContextError(
            "review evidence cannot fit", mandatory_ids=("source-1",)
        )

    monkeypatch.setattr(team, "_gen_indepth", real_gen_indepth)
    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", keep_answer)
    monkeypatch.setattr(team, "_validate_answer_rewrite", clean_review)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)
    monkeypatch.setattr(engine, "_select_indepth_review_context", review_overflow)

    events = dict(_collect(_product_profile(review_model="review")))
    assert events["indepth_error"]["inDepth"]["errorCode"] == "insufficient_context"
    assert events["done"]["inDepth"]["mandatorySourceIds"] == ["source-1"]


def test_nested_references_resolve_against_current_source_ledger():
    blocks = [
        {
            "kind": "table",
            "rows": [
                {
                    "cells": {
                        "weight": {"text": "71 kg", "refs": [1]},
                        "unknown": {"text": "not in ledger", "refs": [99]},
                    }
                }
            ],
        }
    ]

    references = team._resolve_references(
        [],
        _MAPPINGS,
        answer="Summary without prose markers.",
        blocks=blocks,
        grounding_status="checking",
    )

    assert [reference["index"] for reference in references] == [1, 99]
    assert references[0]["resolutionStatus"] == "resolved"
    assert references[0]["usage"][0]["location"] == "block"
    assert references[0]["usage"][0]["text"] == "71 kg"
    assert references[1]["resolutionStatus"] == "unresolved"
    assert references[1]["groundingStatus"] == "unchecked"


def test_temporal_block_rendering_keeps_each_table_date_with_its_row_values():
    blocks = [
        {
            "kind": "table",
            "rows": [
                {
                    "cells": {
                        "date": {"text": "2006-03-03", "refs": [1]},
                        "weight": {"text": "52 kg", "refs": [1]},
                    }
                },
                {
                    "cells": {
                        "date": {"text": "2006-05-18", "refs": [2]},
                        "weight": {"text": "41 kg", "refs": [2]},
                    }
                },
            ],
        }
    ]

    text, refs = team._block_temporal_text_and_refs(blocks)

    assert text.splitlines()[:2] == [
        "2006-03-03 | 52 kg",
        "2006-05-18 | 41 kg",
    ]
    assert refs == [1, 2]


def test_final_unsupported_grounding_marks_answer_needs_review(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Unsupported claim [1].", [1], []

    async def unsupported(_client, _model, _answer, references, _mappings):
        output = []
        for reference in references:
            item = dict(reference)
            item["grounded"] = False
            item["groundingStatus"] = "unsupported"
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ground_references", unsupported)

    events = dict(_collect(_product_profile()))
    pending = events["indepth_pending"]
    final = events["done"]
    assert pending["references"][0]["groundingStatus"] == "unsupported"
    assert pending["answerValidation"]["status"] == "needs_review"
    assert final["answerValidation"]["status"] == "needs_review"
    assert final["answerValidation"]["issues"][-1]["id"] == "citation_grounding"


def test_final_unchecked_grounding_cannot_leave_answer_checked(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Claim with unavailable support [1].", [1], []

    async def unchecked(_client, _model, _answer, references, _mappings):
        output = []
        for reference in references:
            item = dict(reference)
            item["grounded"] = None
            item["groundingStatus"] = "unchecked"
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ground_references", unchecked)

    final = dict(_collect(_product_profile()))["done"]
    assert final["answerValidation"]["status"] == "unavailable"
    assert final["answerValidation"]["issues"][-1]["id"] == (
        "citation_grounding_unavailable"
    )


def test_product_overdeclared_unscoped_citations_cannot_leave_answer_checked(
    monkeypatch,
):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "The documented visit was on 2025-01-01.", [1, 2], []

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)

    final = dict(_collect(_product_profile()))["done"]
    assert final["answerValidation"]["status"] == "needs_review"
    assert any(
        issue["id"] == "citation_scope"
        for issue in final["answerValidation"]["issues"]
    )


def test_product_single_unscoped_citation_is_scoped_and_grounded(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "The documented visit was on 2025-01-01.", [1], []

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)

    events = dict(_collect(_product_profile()))
    final = events["done"]
    assert final["answer"] == "The documented visit was on 2025-01-01 [1]."
    answer_references = [
        reference
        for reference in final["references"]
        if any(
            usage.get("location") == "answer"
            for usage in reference.get("usage") or []
        )
    ]
    assert [reference["index"] for reference in answer_references] == [1]
    assert answer_references[0]["groundingStatus"] == "verified"
    assert final["answerValidation"]["status"] == "checked"


def test_temporal_patch_is_citation_canonicalized_after_gate(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Unsafe draft without markers.", [1, 2], []

    def patching_gate(**_kwargs):
        return (
            "Safe deterministic correction.",
            [1],
            [],
            {"mode": "enforce", "status": "fail", "applied": "patch"},
            "Unsafe draft without markers.",
        )

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_apply_temporal_gate", patching_gate)

    final = dict(_collect(_product_profile()))["done"]
    assert final["answer"] == "Safe deterministic correction [1]."
    assert not any(
        issue["id"] == "citation_scope"
        for issue in final["answerValidation"]["issues"]
    )
    assert final["answerValidation"]["status"] == "checked"


def test_final_mixed_grounding_cannot_leave_answer_checked(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "One supported and one unsupported claim share source [1].", [1], []

    async def mixed(_client, _model, _answer, references, _mappings):
        output = []
        for reference in references:
            item = dict(reference)
            item["grounded"] = None
            item["groundingStatus"] = "mixed"
            item["groundingChecks"] = [
                {"status": "verified", "claim": "Supported claim.", "source_indices": [1]},
                {"status": "unsupported", "claim": "Unsupported claim.", "source_indices": [1]},
            ]
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ground_references", mixed)

    final = dict(_collect(_product_profile()))["done"]
    assert final["references"][0]["groundingStatus"] == "mixed"
    assert final["answerValidation"]["status"] == "needs_review"
    assert final["answerValidation"]["issues"][-1]["id"] == "citation_grounding"


def test_answer_and_indepth_grounding_checks_merge_for_shared_reference(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1][2].", [1, 2], []

    async def fake_indepth(*_args, **_kwargs):
        return ["Supported In-Depth claim [1][2]."]

    async def checks_by_usage(_client, _model, _answer, references, _mappings):
        output = []
        for reference in references:
            item = dict(reference)
            location = (item.get("usage") or [{}])[0].get("location")
            item["grounded"] = True
            item["groundingStatus"] = "verified"
            item["groundingScope"] = "source_set"
            item["groundingGroup"] = [1, 2]
            item["groundingChecks"] = [
                {
                    "status": "verified",
                    "claim": "Supported answer."
                    if location == "answer"
                    else "Supported In-Depth claim.",
                    "location": location,
                    "path": "",
                    "source_indices": [1, 2],
                }
            ]
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)
    monkeypatch.setattr(team, "_ground_references", checks_by_usage)

    final = dict(_collect(_product_profile()))["done"]
    reference = final["references"][0]
    assert reference["groundingStatus"] == "verified"
    assert {check["location"] for check in reference["groundingChecks"]} == {
        "answer",
        "indepth",
    }
    assert {usage["location"] for usage in reference["usage"]} == {
        "answer",
        "indepth",
    }
    assert reference["groundingScope"] == "source_set"
    assert reference["groundingGroup"] == [1, 2]
    assert all(
        item["groundingScope"] == "source_set"
        and item["groundingGroup"] == [1, 2]
        for item in final["references"]
    )


def test_unavailable_indepth_reviewer_is_withheld_in_product_envelope(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def unavailable(*_args, **_kwargs):
        return [], {
            "level": "red",
            "status": "unavailable",
            "note": "In-Depth review was unavailable; no unreviewed claims were shipped.",
        }

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_gen_indepth", unavailable)

    final = dict(_collect(_product_profile(review_model="review")))["done"]
    assert final["inDepth"]["status"] == "needs_review"
    assert final["inDepth"]["answer"] == ""
    assert "review was unavailable" in final["inDepth"]["error"]
    assert final["inDepth"]["validation"]["review_status"] == "unavailable"


def test_partial_indepth_review_is_reported_as_edited(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def edited(*_args, **_kwargs):
        return ["The supported claim remains [1]."], {
            "level": "red",
            "status": "edited",
            "removed": 1,
            "issues": "The second claim was unsupported.",
            "review_attempts": 1,
            "note": "One unsupported claim was removed.",
        }

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_gen_indepth", edited)

    final = dict(_collect(_product_profile(review_model="review")))["done"]
    assert final["inDepth"]["status"] == "complete"
    assert final["inDepth"]["answer"] == "- The supported claim remains [1]."
    validation = final["inDepth"]["validation"]
    assert validation["status"] == "edited"
    assert validation["review_status"] == "edited"
    assert validation["review_removed"] == 1
    assert validation["review_issues"] == "The second claim was unsupported."
    assert validation["review_attempts"] == 1


@pytest.mark.parametrize(
    ("confidence", "expected_removed", "expected_issues"),
    (
        (
            {
                "level": "yellow",
                "status": "edited",
                "removed": 2,
                "issues": "The initial draft was unsupported.",
                "review_attempts": 2,
                "note": "The draft was replaced.",
            },
            2,
            "The initial draft was unsupported.",
        ),
        (
            {
                "level": "red",
                "status": "edited",
                "removed": 3,
                "issues": "The initial draft was unsupported.; One retry claim was unsupported.",
                "review_attempts": 2,
                "note": "Unsupported claims were removed.",
            },
            3,
            "The initial draft was unsupported.; One retry claim was unsupported.",
        ),
    ),
)
def test_retry_review_metadata_survives_product_envelope(
    monkeypatch, confidence, expected_removed, expected_issues
):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def edited(*_args, **_kwargs):
        return ["The replacement claim is supported [1]."], confidence

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_gen_indepth", edited)

    final = dict(_collect(_product_profile(review_model="review")))["done"]
    validation = final["inDepth"]["validation"]
    assert validation["status"] == "edited"
    assert validation["review_removed"] == expected_removed
    assert validation["review_issues"] == expected_issues
    assert validation["review_attempts"] == 2


def test_non_substantive_review_preserves_answer_temporal_gate(monkeypatch):
    _stub_common(monkeypatch)

    async def empty_answer(*_args, **_kwargs):
        return "", [], []

    monkeypatch.setattr(team, "_synthesize_answer", empty_answer)

    final = dict(_collect(_product_profile(review_model="review")))["done"]
    assert final["answerValidation"]["status"] == "needs_review"
    assert final["temporalGate"] == {
        "mode": "off",
        "status": "ok",
        "applied": "none",
    }


def test_indepth_citation_cannot_inherit_answer_verified_verdict(monkeypatch):
    _stub_common(monkeypatch)
    calls = []

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def fake_indepth(*_args, **_kwargs):
        return ["Unsupported In-Depth claim [1]."]

    async def ground_by_usage(_client, _model, _answer, references, _mappings):
        calls.append(references)
        output = []
        for reference in references:
            item = dict(reference)
            is_indepth = any(
                usage.get("location") == "indepth" for usage in item.get("usage") or []
            )
            item["grounded"] = not is_indepth
            item["groundingStatus"] = "unsupported" if is_indepth else "verified"
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)
    monkeypatch.setattr(team, "_ground_references", ground_by_usage)

    events = dict(_collect(_product_profile()))
    indepth_done = events["indepth_error"]
    final = events["done"]
    assert indepth_done["inDepth"] == final["inDepth"]
    assert indepth_done["answer"] == final["answer"]
    assert "status" not in indepth_done

    assert len(calls) == 2
    assert indepth_done["inDepth"]["status"] == "needs_review"
    assert indepth_done["references"][0]["groundingStatus"] == "verified"
    assert final["inDepth"]["status"] == "needs_review"
    assert final["inDepth"]["answer"] == ""
    assert final["inDepth"]["validation"]["citation_checks"][0]["status"] == "fail"
    assert "evidence checks rejected every claim" in final["inDepth"]["error"]
    assert final["references"][0]["groundingStatus"] == "verified"
    assert all(
        usage.get("location") != "indepth" for usage in final["references"][0]["usage"]
    )


def test_knowledge_reference_survives_source_set_grounding_with_provenance(monkeypatch):
    mappings = [
        {
            "index": 1,
            "sourceId": "test:patient:1",
            "source": "test-patient",
            "resourceType": "Observation",
            "resourceUuid": "obs-1",
            "date": "2026-01-01",
            "text": "The patient takes Examplemed.",
        },
        {
            "index": 2,
            "sourceId": "knowledge-base:guide-1",
            "source": "knowledge-base",
            "resourceType": "KnowledgeReference",
            "resourceUuid": "guide-1",
            "date": None,
            "text": "Example guidance: Examplemed requires annual monitoring.",
            "provenance": {
                "authority": "Example Authority",
                "version": "2026",
                "url": "https://example.test/guide",
                "license": "CC BY",
            },
        },
    ]
    claim = "The patient takes Examplemed [1], which requires annual monitoring [2]."
    references = team._resolve_references(
        [1, 2],
        mappings,
        answer=claim,
        grounding_status="unchecked",
        answer_usage_location="indepth",
    )

    async def supported(_client, _model, pairs):
        assert len(pairs) == 1
        assert "[1]" in pairs[0][0] and "[2]" in pairs[0][0]
        return [True]

    monkeypatch.setattr(team, "_entailment_verdicts", supported)
    grounded = asyncio.run(
        team._ground_references(None, "model", claim, references, mappings)
    )

    assert [item["groundingStatus"] for item in grounded] == ["verified", "verified"]
    kb_reference = grounded[1]
    assert kb_reference["resourceType"] == "KnowledgeReference"
    assert kb_reference["sourceId"] == "knowledge-base:guide-1"
    assert kb_reference["source"] == "knowledge-base"
    assert kb_reference["provenance"] == {
        "authority": "Example Authority",
        "version": "2026",
        "url": "https://example.test/guide",
        "license": "CC BY",
    }
    assert kb_reference["groundingGroup"] == [1, 2]


def test_product_review_and_final_event_preserve_grounded_knowledge_reference(monkeypatch):
    real_ground = team._ground_references
    _stub_common(monkeypatch)
    monkeypatch.setattr(team, "_ground_references", real_ground)

    async def fake_answer(*_args, **_kwargs):
        return (
            "The chart documents Examplemed [1], and the reference recommends monitoring [3].",
            [1, 3],
            [],
        )

    async def fake_review(_client, _model, **kwargs):
        assert "KnowledgeReference (source: knowledge-base)" in kwargs["chart"]
        return {"answer_ok": True, "errors": []}

    async def fake_indepth(*_args, **_kwargs):
        return (
            ["Examplemed is documented [1], with monitoring recommended by the reference [3]."],
            {"level": "green", "note": ""},
        )

    async def supported(_client, _model, pairs):
        assert pairs
        return [True] * len(pairs)

    async def fake_gather(*_args, **_kwargs):
        return [], []

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_validate_answer_rewrite", fake_review)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_entailment_verdicts", supported)
    monkeypatch.setattr(team, "_gather_evidence", fake_gather)
    monkeypatch.setattr(
        "server.context_sources.kb.search",
        lambda _query, k=3: [
            {
                "id": "guide-1",
                "title": "Example guidance",
                "text": "Examplemed requires monitoring.",
                "source": "Example Authority",
                "version": "2026",
                "url": "https://example.test/guide",
                "license": "CC BY",
            }
        ],
    )

    events = dict(
        _collect(
            _product_profile(
                review_model="review",
                orchestrator_model="orchestrator",
                expert_model="expert",
            )
        )
    )
    final = events["done"]
    kb_reference = next(
        reference
        for reference in final["references"]
        if reference["resourceType"] == "KnowledgeReference"
    )

    assert final["answerValidation"]["status"] == "checked"
    assert final["inDepth"]["status"] == "complete"
    assert kb_reference["groundingStatus"] == "verified"
    assert kb_reference["provenance"] == {
        "authority": "Example Authority",
        "url": "https://example.test/guide",
        "version": "2026",
        "license": "CC BY",
    }
    assert {usage["location"] for usage in kb_reference["usage"]} == {
        "answer",
        "indepth",
    }


def test_unchecked_indepth_citation_is_omitted_and_cannot_report_complete(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def fake_indepth(*_args, **_kwargs):
        return ["In-Depth claim whose support was not checked [1]."]

    async def ground_by_usage(_client, _model, _answer, references, _mappings):
        output = []
        for reference in references:
            item = dict(reference)
            is_indepth = any(
                usage.get("location") == "indepth"
                for usage in item.get("usage") or []
            )
            item["grounded"] = None if is_indepth else True
            item["groundingStatus"] = "unchecked" if is_indepth else "verified"
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)
    monkeypatch.setattr(team, "_ground_references", ground_by_usage)

    final = dict(_collect(_product_profile()))["done"]

    assert final["inDepth"]["status"] == "needs_review"
    assert final["inDepth"]["answer"] == ""
    check = final["inDepth"]["validation"]["citation_checks"][0]
    assert check["status"] == "fail"
    assert "could not be checked" in check["reason"]


def test_uncited_indepth_claim_is_withheld_and_cannot_report_complete(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Supported answer [1].", [1], []

    async def fake_indepth(*_args, **_kwargs):
        return ["Uncited clinical interpretation."]

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_synthesize_indepth", fake_indepth)

    final = dict(_collect(_product_profile()))["done"]

    assert final["inDepth"]["status"] == "needs_review"
    assert final["inDepth"]["answer"] == ""
    check = final["inDepth"]["validation"]["citation_checks"][0]
    assert check["status"] == "fail"
    assert "no source citation" in check["reason"]


def test_named_sse_emits_heartbeats_while_a_leg_stalls():
    # Gate 6: without heartbeats, an intermediary/browser sees a dead-looking connection during a
    # long leg and there is nothing for an abort to interrupt until the NEXT event finally arrives.
    async def slow_gen():
        await asyncio.sleep(0.05)
        yield ("answer_done", '{"answer":"hi"}')

    async def _run():
        return [
            chunk
            async for chunk in openai_compat._named_sse(slow_gen(), interval_s=0.01)
        ]

    chunks = asyncio.run(_run())
    heartbeats = [c for c in chunks if c == ": hb\n\n"]
    assert (
        len(heartbeats) >= 2
    ), f"expected repeated heartbeats while stalled, got {chunks}"
    assert chunks[-1] == 'event: answer_done\ndata: {"answer":"hi"}\n\n'


def test_default_heartbeat_interval_detects_product_preemption_promptly():
    assert 0.1 <= openai_compat._SSE_HEARTBEAT_INTERVAL_S <= 1.0


def test_named_sse_resumes_all_events_in_one_task_context():
    marker = ContextVar("stream-budget", default=None)

    async def staged_gen():
        token = marker.set("active-budget")
        try:
            yield "answer_done", "{}"
            assert marker.get() == "active-budget"
            yield "answer_validation", "{}"
            assert marker.get() == "active-budget"
            yield "done", "{}"
        finally:
            marker.reset(token)

    async def collect():
        return [chunk async for chunk in openai_compat._named_sse(staged_gen())]

    chunks = asyncio.run(collect())
    assert [chunk.splitlines()[0] for chunk in chunks] == [
        "event: answer_done",
        "event: answer_validation",
        "event: done",
    ]


def test_named_sse_emits_structured_context_error():
    from server.context_sources import ContextSourceError

    async def broken():
        if False:
            yield "unused", ""
        raise ContextSourceError(
            "tokenization_unavailable", "Exact tokenizer failed.", source="router"
        )

    async def collect():
        return [chunk async for chunk in openai_compat._named_sse(broken())]

    chunks = asyncio.run(collect())

    assert len(chunks) == 1
    assert chunks[0].startswith("event: error\n")
    assert '"code": "tokenization_unavailable"' in chunks[0]
    assert '"source": "router"' in chunks[0]


def test_named_sse_cancel_mid_heartbeat_still_frees_router_lock():
    # The heartbeat wait must not weaken the existing cancel-frees-the-lock invariant: cancelling
    # while parked on a heartbeat still has to unwind whatever the hub is doing underneath.
    async def _run():
        started = asyncio.Event()

        class Hanging:
            async def post(self, *_a, **_k):
                started.set()
                await asyncio.sleep(3600)

        async def hanging_gen():
            await team._chat(Hanging(), "m", [{"role": "user", "content": "x"}])
            yield ("done", "{}")  # unreachable; _chat never returns

        stream = openai_compat._named_sse(hanging_gen(), interval_s=0.01)
        agen = stream.__aiter__()
        first = asyncio.ensure_future(agen.__anext__())
        await started.wait()
        assert team._ROUTER_LOCK.locked()
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        await agen.aclose()
        assert (
            not team._ROUTER_LOCK.locked()
        ), "cancelling mid-heartbeat must still free the router lock"

    asyncio.run(_run())


def test_chat_cancel_releases_router_lock():
    """The load-bearing preempt invariant: a cancelled _chat releases _ROUTER_LOCK so the next
    request (the preempting question) gets the single slot immediately."""

    async def _run():
        started = asyncio.Event()

        class Hanging:
            async def post(self, *_a, **_k):
                started.set()
                await asyncio.sleep(3600)  # hang until cancelled

        task = asyncio.create_task(
            team._chat(Hanging(), "m", [{"role": "user", "content": "x"}])
        )
        await started.wait()
        assert team._ROUTER_LOCK.locked()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert (
            not team._ROUTER_LOCK.locked()
        ), "router lock not released on cancel -> preempt can't free the slot"

    asyncio.run(_run())


def test_profile_stream_client_disconnect_mid_indepth_frees_router_lock(monkeypatch):
    """Gate 6, at the layer that owns it: a client disconnect WHILE THE IN-DEPTH LEG IS GENERATING
    inside the stage engine must unwind the in-flight _chat, free the single _ROUTER_LOCK, and let the
    next request (the preempting question) acquire the slot. This is the timing invariant the e2e
    preempt spec deliberately does NOT assert (real model latency swamps the signal); a fake _chat
    makes it deterministic here. Stronger than test_chat_cancel_releases_router_lock: it drives the
    WHOLE staged generator to the in-depth phase, not just _chat in isolation."""
    _stub_common(monkeypatch)
    cancellations = []
    monkeypatch.setattr(
        team,
        "_write_cancellation_trace",
        lambda level_id, messages, *, router_lock_released: cancellations.append(
            {
                "level_id": level_id,
                "messages": messages,
                "router_lock_released": router_lock_released,
            }
        ),
    )

    async def fake_synth(*_a, **_k):
        # Bypass the answer leg's real _chat so the ONLY router-lock holder under test is in-depth.
        return ("Answer.", [], [])

    monkeypatch.setattr(team, "_synthesize_answer", fake_synth)

    indepth_running = asyncio.Event()

    class HangingChat:
        async def post(self, *_a, **_k):
            indepth_running.set()
            await asyncio.sleep(3600)  # hang until cancelled

    async def hanging_indepth(*_a, **_k):
        # The in-depth leg does REAL router work: acquire the single slot via _chat, then hang —
        # exactly the state a mid-in-depth disconnect must be able to interrupt.
        await team._chat(
            HangingChat(), "indepth-model", [{"role": "user", "content": "deep dive"}]
        )
        return (["claim"], {"level": "green", "note": ""})

    monkeypatch.setattr(team, "_synthesize_indepth", hanging_indepth)

    async def _run():
        agen = stream_profile(
            _product_profile(answer_model="m"),
            [{"role": "user", "content": "q?"}],
            patient="p",
            context={"temporal": False},
            model_label="lvl",
            source_registry=_TEST_SOURCE,
        ).__aiter__()

        # Drive the generator to the in-depth phase. validator=None → no answer_validation event;
        # sequence is answer_done -> indepth_pending -> (in-depth runs) -> indepth_done.
        seen = []
        while True:
            name, _ = await agen.__anext__()
            seen.append(name)
            if name == "indepth_pending":
                break
        assert seen == ["answer_done", "indepth_pending"], seen

        # The next step runs the (hanging) in-depth leg, which grabs the single router slot.
        step = asyncio.ensure_future(agen.__anext__())
        await indepth_running.wait()
        assert (
            team._ROUTER_LOCK.locked()
        ), "the in-depth leg must hold the router slot while generating"

        # Client disconnects → the driving task is cancelled mid-in-depth.
        step.cancel()
        try:
            await step
        except asyncio.CancelledError:
            pass
        await agen.aclose()

        assert (
            not team._ROUTER_LOCK.locked()
        ), "a disconnect mid-in-depth must free the router slot"

        # The preempting request now acquires the single slot immediately (would hang if still held).
        await asyncio.wait_for(team._ROUTER_LOCK.acquire(), timeout=1.0)
        team._ROUTER_LOCK.release()

        assert cancellations == [
            {
                "level_id": "test-profile",
                "messages": [{"role": "user", "content": "q?"}],
                "router_lock_released": True,
            }
        ]

    asyncio.run(_run())


def test_team_profile_stream_gathers_via_the_tool_loop(monkeypatch):
    # A team product profile must execute its declared gather stage before synthesis.
    _stub_common(monkeypatch)
    calls = []

    async def fake_chat(
        _client, model, _messages, *, tools=None, response_format=None, **_kwargs
    ):
        calls.append(model)
        if response_format is not None or model == "M":
            return {
                "content": json.dumps({"answer": "Ans.", "citations": [], "blocks": []})
            }
        orchestrator_turns = sum(1 for m in calls if m == "orch-model")
        if tools is not None and orchestrator_turns == 1:
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "t1",
                        "function": {
                            "name": "medical_expert",
                            "arguments": json.dumps({"query": "interpret"}),
                        },
                    }
                ],
            }
        return {"content": "ok", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)

    events = _collect(
        _product_profile(orchestrator_model="orch-model", expert_model="expert-model")
    )
    names = [n for n, _ in events]
    assert names == ["answer_done", "indepth_pending", "indepth_done", "done"]
    assert "orch-model" in calls, "team scaffolding must run the orchestrator tool loop"
    assert (
        "expert-model" in calls
    ), "the tool-called medical expert must actually be consulted"


def test_single_product_profile_never_runs_the_tool_loop(monkeypatch):
    # A single profile has no gather stage and therefore no orchestrator call.
    _stub_common(monkeypatch)
    calls = []

    async def fake_chat(_client, model, _messages, **_kwargs):
        calls.append(model)
        return {"content": "unused"}

    monkeypatch.setattr(team, "_chat", fake_chat)

    async def fake_answer(*_a, **_k):
        return ("Ans.", [], [])

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)

    _collect(_product_profile())
    assert (
        calls == []
    ), f"single profile must not touch orchestrator/expert models, got {calls}"


def test_profile_stream_executes_gather_from_the_configured_stage_plan(monkeypatch):
    _stub_common(monkeypatch)
    calls = []

    async def fake_chat(
        _client, model, _messages, *, tools=None, response_format=None, **_kwargs
    ):
        calls.append(model)
        if response_format is not None:
            return {
                "content": json.dumps({"answer": "Ans.", "citations": [], "blocks": []})
            }
        return {"content": "ok", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)

    _collect(get_profile("med-agent-team-med-validated"))
    assert (
        "gemma-e4b-q8" in calls
    ), "the orchestrator was never called even though the compiled profile declares gather"
