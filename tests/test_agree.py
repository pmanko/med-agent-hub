"""The agree stage: a second writer from another family answers on its own, the shown answer
stays the first writer's, and a sentence the second writer states differently is a finding."""

from __future__ import annotations

import server.config as config
import server.engine as engine
import server.team as team
from tests.factories import make_profile
from tests.test_staged_stream import _collect, _stub_common


def _agree_profile():
    return make_profile(
        topology="single",
        stages=["context", "answer", "gate", "resolve_refs", "final_resolve_refs", "agree",
                "indepth", "indepth_gate"],
        models={"answer": "M", "second": "S", "indepth": "M"},
        prompts={"answer": "synthesis-answer", "indepth": "synthesis-indepth"},
        output="product",
        policies={"confidence": "findings"},
        supplemental_sources=("knowledge-base",),
    )


def _writers(first, second, *, first_blocks=None, second_blocks=None, dry_seen=None):
    async def fake_answer(_client, model, *_args, **kwargs):
        if dry_seen is not None:
            dry_seen[model] = kwargs.get("dry")
        return (
            (second if model == "S" else first),
            ([1] if model != "S" else []),
            list((second_blocks if model == "S" else first_blocks) or []),
        )
    return fake_answer


async def _unchanged(_client, **kwargs):
    return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}


def test_answer_claims_are_the_sentences_and_cells_without_citations():
    blocks = [
        {
            "kind": "table",
            "title": "Medications",
            "columns": [
                {"key": "drug", "label": "Drug"},
                {"key": "dose", "label": "Dose"},
            ],
            "rows": [
                {
                    "cells": {
                        "drug": {"text": "lisinopril", "refs": [1]},
                        "dose": {"text": "10 mg", "refs": [1]},
                    }
                }
            ],
        }
    ]
    claims = team._answer_claims(
        "Weight was 71 kg on 2026-01-26 [19]. CD4 50.\n...",
        blocks,
    )
    assert claims == [
        "Weight was 71 kg on 2026-01-26 .",
        "CD4 50.",
        "Drug: lisinopril | Dose: 10 mg",
    ]
    assert team._answer_claims(None) == []
    assert team._answer_claims("   ") == []


def test_a_second_writer_that_states_a_different_value_marks_the_answer(monkeypatch):
    _stub_common(monkeypatch)
    pairs_seen = []
    dry_seen = {}

    async def verdicts(_client, model, pairs):
        pairs_seen.append((model, pairs))
        return [{"support": "contradicted", "facts": [{"fact": "71 kg", "status": "differs", "source_value": "74 kg"}],
                 "not_found": None, "chart_value": "71 kg: the record states 74 kg"}]

    monkeypatch.setattr(
        team,
        "_synthesize_answer",
        _writers("The last weight was 71 kg [1].", "The last weight was 74 kg.", dry_seen=dry_seen),
    )
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    events = dict(_collect(_agree_profile()))
    assert events["answer_done"]["answerValidation"]["status"] == "checking"
    done = events["done"]

    assert done["answer"] == "The last weight was 71 kg [1]."
    assert dry_seen["S"] == 0.0
    assert pairs_seen[0][0] == "S"
    assert pairs_seen[0][1] == [("The last weight was 74 kg.", "The last weight was 71 kg .")]
    assert done["agreement"]["status"] == "differs"
    assert done["agreement"]["claims"][0]["other"] == "71 kg: the record states 74 kg"
    assert done["answerValidation"]["status"] == "needs_review"
    assert done["confidence"]["answer"]["level"] is None
    assert [f["check"] for f in done["confidence"]["answer"]["findings"]] == ["agreement"]
    assert "the models differ" not in done["answer"]


def test_a_second_writer_that_agrees_leaves_the_answer_checked(monkeypatch):
    import asyncio
    import json
    from tests.factories import run_profile
    from tests.test_staged_stream import _TEST_SOURCE

    _stub_common(monkeypatch)

    async def verdicts(_client, _model, pairs):
        return [{"support": "supported", "facts": [], "not_found": None, "chart_value": None} for _ in pairs]

    monkeypatch.setattr(team, "_synthesize_answer", _writers("The last weight was 71 kg [1].", "Weight 71 kg on 2026-01-26."))
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    done = dict(_collect(_agree_profile()))["done"]
    blocked = json.loads(
        asyncio.run(
            run_profile(
                _agree_profile(),
                [{"role": "user", "content": "What was the weight?"}],
                patient="p",
                source_registry=_TEST_SOURCE,
            )
        )
    )

    assert done["agreement"]["status"] == "agreed"
    assert blocked["agreement"] == done["agreement"]
    assert done["answerValidation"]["status"] == "checked"
    assert done["confidence"]["answer"]["findings"] == []


def test_short_claim_and_structured_table_disagreements_are_checked(monkeypatch):
    _stub_common(monkeypatch)
    pairs_seen = []

    async def verdicts(_client, model, pairs):
        pairs_seen.append((model, pairs))
        out = []
        for source, statement in pairs:
            if "CD4 50" in statement:
                out.append(
                    {
                        "support": "contradicted",
                        "facts": [{"fact": "CD4 50", "status": "differs", "source_value": "CD4 500"}],
                        "not_found": None,
                        "chart_value": "CD4 50: the record states CD4 500",
                    }
                )
            elif "10 mg" in statement and "20 mg" in source:
                out.append(
                    {
                        "support": "contradicted",
                        "facts": [{"fact": "10 mg", "status": "differs", "source_value": "20 mg"}],
                        "not_found": None,
                        "chart_value": "10 mg: the record states 20 mg",
                    }
                )
            else:
                out.append({"support": "supported", "facts": [], "not_found": None, "chart_value": None})
        return out

    def table(dose):
        return [
            {
                "kind": "table",
                "title": "Medications",
                "columns": [
                    {"key": "drug", "label": "Drug"},
                    {"key": "dose", "label": "Dose"},
                ],
                "rows": [
                    {
                        "cells": {
                            "drug": {"text": "lisinopril", "refs": [1]},
                            "dose": {"text": dose, "refs": [1]},
                        }
                    }
                ],
            }
        ]

    monkeypatch.setattr(
        team,
        "_synthesize_answer",
        _writers(
            "CD4 50.",
            "CD4 500.",
            first_blocks=table("10 mg"),
            second_blocks=table("20 mg"),
        ),
    )
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    done = dict(_collect(_agree_profile()))["done"]
    assert done["agreement"]["status"] == "differs"
    assert done["agreement"]["blocks"] == table("20 mg")
    assert done["answerValidation"]["status"] == "needs_review"
    assert len([f for f in done["confidence"]["answer"]["findings"] if f["check"] == "agreement"]) == 2


def test_an_unavailable_agreement_check_marks_answer_validation_unavailable(monkeypatch):
    _stub_common(monkeypatch)
    monkeypatch.setattr(team, "_synthesize_answer", _writers("Weight 71 kg [1].", ""))
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)

    done = dict(_collect(_agree_profile()))["done"]
    assert done["agreement"]["status"] == "unchecked"
    assert done["answerValidation"]["status"] == "unavailable"
    assert any(
        issue.get("id") == "agreement_unavailable"
        for issue in done["answerValidation"]["issues"]
    )
    assert any(
        finding.get("check") == "agreement"
        for finding in done["confidence"]["answer"]["findings"]
    )


def test_agree_stage_requires_second_model_and_valid_stage_order():
    from dataclasses import replace
    import pytest
    from server.levels_loader import compile_execution_spec, compile_profile, profile_metadata

    profile = compile_execution_spec(
        "gemma-4-12b", {"mode": "agree", "second_model": "qwen3.6-35b"}
    )
    metadata = profile_metadata(profile, available=True)
    assert "second" in metadata["role_models"]
    assert "second" in metadata["role_prompt_digests"]

    with pytest.raises(ValueError, match="requires model role second"):
        compile_profile(replace(profile, models={"answer": "M", "indepth": "M"}))

    with pytest.raises(ValueError, match="agree must run after final_resolve_refs"):
        compile_profile(
            replace(
                profile,
                stages=(
                    "context",
                    "agree",
                    "answer",
                    "gate",
                    "resolve_refs",
                    "final_resolve_refs",
                    "indepth",
                    "indepth_gate",
                ),
            )
        )


def test_a_fact_the_second_writer_does_not_state_is_a_note(monkeypatch):
    _stub_common(monkeypatch)

    async def verdicts(_client, _model, pairs):
        return [{"support": "not_addressed", "facts": [], "not_found": "2026-01-26", "chart_value": None}]

    monkeypatch.setattr(team, "_synthesize_answer", _writers("Weight 71 kg on 2026-01-26 [1].", "Weight is 71 kg."))
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    done = dict(_collect(_agree_profile()))["done"]

    assert done["agreement"]["status"] == "partly"
    assert done["answerValidation"]["status"] == "checked"
    notes = [i for i in done["answerValidation"]["issues"] if i["id"] == "agreement_unverified"]
    assert notes and "2026-01-26" in notes[0]["reason"]


def test_a_profile_without_a_citation_check_grounds_nothing():
    assert engine._grounding_policy(_agree_profile()) == "off"


def test_the_writer_runs_without_the_repetition_penalty_by_default():
    assert config.SYNTH_DRY_MULTIPLIER == 0.0


def _bare_profile():
    return make_profile(
        topology="single",
        stages=["context", "answer", "gate", "resolve_refs", "final_resolve_refs", "indepth", "indepth_gate"],
        models={"answer": "M", "indepth": "M"},
        prompts={"answer": "synthesis-answer", "indepth": "synthesis-indepth"},
        output="product",
        policies={"confidence": "findings"},
        supplemental_sources=("knowledge-base",),
    )


def test_a_bare_profile_reports_a_gate_patch_as_edited_and_no_review(monkeypatch):
    import asyncio
    import json
    from tests.factories import _TestTokenCounter, patient_source_registry, stream_profile

    real_gate = team._apply_temporal_gate
    _stub_common(monkeypatch)
    monkeypatch.setattr(team, "_apply_temporal_gate", real_gate)

    async def fake_answer(_client, _model, *_args, **_kwargs):
        return "The visit was on 2026-01_26 [1].", [1], []

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)

    source = patient_source_registry(
        "[1] (2026-01-26) Weight: 71 kg",
        [{"index": 1, "resourceType": "obs", "resourceUuid": "u1", "date": "2026-01-26", "text": "(2026-01-26) Weight: 71 kg"}],
    )

    async def run():
        out = {}
        async for name, raw in stream_profile(
            _bare_profile(),
            [{"role": "user", "content": "What was the visit date?"}],
            patient="p",
            source_registry=source,
            token_counter=_TestTokenCounter(),
        ):
            out[name] = json.loads(raw)
        return out

    done = asyncio.run(run())["done"]

    assert done["answer"] == "The visit was on 2026-01-26 [1]."
    assert done["answerValidation"]["status"] == "edited"
    assert done["answerValidation"]["originalAnswer"] == "The visit was on 2026-01_26 [1]."
    assert "review_status" not in done["inDepth"]["validation"]
    assert "groundingChecks" not in (done["references"][0] if done["references"] else {})


def test_second_writer_fits_context_with_its_own_tokenizer(monkeypatch):
    import asyncio
    from server.engine import ContextView, IncludedRecord
    from tests.factories import _TestTokenCounter, stream_profile
    from tests.test_staged_stream import _TEST_SOURCE

    _stub_common(monkeypatch)
    seen_messages = {}

    async def fake_answer(_client, model, messages, *_args, **_kwargs):
        seen_messages[model] = [dict(m) for m in messages]
        return ("Weight is 71 kg [1].", [1], [])

    async def verdicts(_client, _model, pairs):
        return [{"support": "supported", "facts": [], "not_found": None, "chart_value": None} for _ in pairs]

    class SecondNeedsSmallerChart(_TestTokenCounter):
        async def count_chat(self, model, payload):
            text = " ".join(str(m.get("content", "")) for m in payload.get("messages", []))
            if model == "S" and "Record two" in text:
                return 999999
            return 100

    async def custom_selector(ledger, *, question, model, budget, counter, input_measure=None):
        records = ledger.records if model != "S" else ledger.records[:1]
        return ContextView(
            records=records,
            record_indices=tuple(range(1, len(records) + 1)),
            mode="selected" if model == "S" else "full",
            included=tuple(IncludedRecord(r.stable_id, "selected") for r in records),
            excluded=(),
            input_tokens=100,
            input_limit=budget.input_limit,
            original_text=ledger.render(),
            preamble=ledger.preamble,
        )

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    async def run():
        out = {}
        async for name, data in stream_profile(
            _agree_profile(),
            [{"role": "user", "content": "Record one\nRecord two"}],
            patient="p",
            source_registry=_TEST_SOURCE,
            token_counter=SecondNeedsSmallerChart(),
            context_selector=custom_selector,
        ):
            import json
            out[name] = json.loads(data)
        return out

    events = asyncio.run(run())
    assert events["done"]["agreement"]["status"] == "agreed"
    assert "S" in seen_messages


def test_verifier_review_retains_checked_entries_in_verdict_and_step(monkeypatch):
    import asyncio
    import json
    import httpx

    async def fake_chat(_client, _model, _messages, **_kwargs):
        return {
            "content": json.dumps(
                {
                    "answer_ok": True,
                    "errors": [],
                    "checked": [{"question": "What was the weight?", "chart": "71 kg [1]"}],
                }
            )
        }

    monkeypatch.setattr(team, "_chat", fake_chat)
    steps = []
    asyncio.run(
        team._review_existing_answer(
            httpx.AsyncClient(),
            messages=[{"role": "user", "content": "What was the weight?"}],
            gathered="",
            chart="[1] 2026-01-26 | Weight: 71 kg",
            temporal_facts=None,
            temporal_gate_mode="off",
            reviewer_model="R",
            reviewer_prompt="validation-verify",
            validator_temperature=0.0,
            validator_repeat_penalty=None,
            validator_dry=0.0,
            max_tokens=512,
            steps=steps,
            payload_override={
                "schema_version": "answer_to_review.v1",
                "original_question": "What was the weight?",
                "answer": "Weight was 71 kg [1].",
                "citations": [1],
                "blocks": [],
            },
        )
    )
    review_steps = [s for s in steps if s.get("role") == "answer_review"]
    assert review_steps and review_steps[0].get("checked") == [
        {"question": "What was the weight?", "chart": "71 kg [1]"}
    ]

    async def fake_indepth_chat(_client, _model, _messages, **_kwargs):
        return {
            "content": json.dumps(
                {
                    "flag": [],
                    "issues": "",
                    "checked": [{"question": "Was weight 71 kg?", "chart": "71 kg [1]"}],
                }
            )
        }

    monkeypatch.setattr(team, "_chat", fake_indepth_chat)
    indepth_verdict = asyncio.run(
        team._validate_indepth_verdict(
            httpx.AsyncClient(),
            "R",
            chart="[1] 2026-01-26 | Weight: 71 kg",
            gathered="",
            answer_text="Weight was 71 kg [1].",
            claims=["Weight was 71 kg [1]."],
            max_tokens=512,
            temperature=0.0,
            repeat_penalty=None,
            dry=0.0,
            validation_prompt="validation-verify",
        )
    )
    assert indepth_verdict.get("checked") == [
        {"question": "Was weight 71 kg?", "chart": "71 kg [1]"}
    ]

