"""The review policy block: what happens with the reviewer's verdict and how citations are grounded.

Profiles choose; the engine implements. These tests load every configured profile, reject the
reserved and invalid choices, and run the folded and off grounding choices through the engine with
the model calls stubbed.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import pytest

import server.team as team
from server.levels_loader import get_profile, profile_ids
from tests.factories import make_profile, patient_source_registry, stream_profile

_MAPPINGS = [
    {
        "index": 1,
        "resourceType": "obs",
        "resourceUuid": "u1",
        "date": "2025-01-01",
        "text": "Weight: 71 kg",
    },
    {
        "index": 2,
        "resourceType": "obs",
        "resourceUuid": "u2",
        "date": "2025-02-02",
        "text": "CD4 count: 341 cells/uL",
    },
]
_SOURCE = patient_source_registry("[1] weight\n[2] cd4\n", _MAPPINGS)
_FAST_STAGES = [
    "context",
    "answer",
    "gate",
    "resolve_refs",
    "review",
    "gate",
    "final_resolve_refs",
    "ground_verdicts",
]


def _profile(grounding: str, *, action: str = "label", models=None):
    return make_profile(
        topology="single",
        stages=_FAST_STAGES,
        models=models or {"answer": "W", "review": "R"},
        prompts={"answer": "synthesis-answer", "review": "validation-label"},
        output="product",
        policies={"review": {"action": action, "grounding": grounding}},
    )


def test_every_configured_reviewing_profile_labels_and_names_its_grounding():
    for profile_id in profile_ids():
        profile = get_profile(profile_id)
        if not {"review", "ground_verdicts"} & set(profile.stages):
            continue
        review = profile.policies["review"]
        assert review["action"] == "label", profile_id
        assert review["grounding"] in {"separate", "folded", "off"}, profile_id


@pytest.mark.parametrize("action", ["apply", "redo"])
def test_reserved_review_actions_are_refused(action):
    with pytest.raises(ValueError, match="reserved and not built"):
        _profile("separate", action=action, models={"answer": "W", "review": "R", "grounding": "G"})


def test_unknown_grounding_policy_is_refused():
    with pytest.raises(ValueError, match="invalid review grounding"):
        _profile("sometimes", models={"answer": "W", "review": "R", "grounding": "G"})


def test_separate_grounding_requires_a_grounding_model():
    with pytest.raises(ValueError, match="requires model role grounding"):
        _profile("separate")


def test_folded_grounding_requires_a_review_stage():
    with pytest.raises(ValueError, match="folded requires the review stage"):
        make_profile(
            topology="single",
            stages=["context", "answer", "gate", "resolve_refs", "final_resolve_refs", "ground_verdicts"],
            models={"answer": "W"},
            prompts={"answer": "synthesis-answer"},
            output="product",
            policies={"review": {"action": "label", "grounding": "folded"}},
        )


def test_a_reviewing_profile_without_a_review_policy_is_refused():
    with pytest.raises(ValueError, match="must declare policies.review"):
        make_profile(
            topology="single",
            stages=_FAST_STAGES,
            models={"answer": "W", "review": "R", "grounding": "G"},
            prompts={"answer": "synthesis-answer", "review": "validation-label"},
            output="product",
            policies={"review": None},
        )


def test_engine_code_names_no_profile():
    """Forbidden coupling: the engine implements policies and never branches on a profile id."""
    server = Path(__file__).resolve().parent.parent / "server"
    code = "\n".join(
        (server / name).read_text() for name in ("engine.py", "team.py", "temporal.py")
    )
    named = [
        profile_id
        for profile_id in profile_ids()
        if re.search(rf"[\"']{re.escape(profile_id)}[\"']", code)
    ]
    assert named == []


def _stub(monkeypatch, answer, review_verdict):
    calls = []

    async def fake_answer(*_args, **_kwargs):
        return answer, team._extract_citations(answer), []

    async def fake_review(_client, model, **kwargs):
        calls.append(("review", model, kwargs.get("citation_support")))
        return dict(review_verdict)

    async def no_entailment(*_args, **_kwargs):
        raise AssertionError("this grounding policy makes no entailment call")

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_validate_answer_label", fake_review)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", no_entailment)
    monkeypatch.setattr(team, "_write_trace", lambda *_a, **_k: None)
    return calls


def _final(profile):
    async def run():
        events = []
        async for name, data in stream_profile(
            profile,
            [{"role": "user", "content": "What was recorded?"}],
            patient="p",
            context={"temporal": False},
            model_label="lvl",
            source_registry=_SOURCE,
        ):
            events.append((name, json.loads(data)))
        return events

    events = asyncio.run(run())
    return [name for name, _payload in events], dict(events)["done"]


def test_folded_grounding_writes_citation_statuses_from_the_review(monkeypatch):
    calls = _stub(
        monkeypatch,
        "Weight was 71 kg [1]. CD4 was 500 cells/uL [2].",
        {"answer_ok": True, "errors": [], "citations": {1: True, 2: False}},
    )

    _names, final = _final(_profile("folded"))

    assert calls == [("review", "R", True)]
    statuses = {ref["index"]: ref["groundingStatus"] for ref in final["references"]}
    assert statuses == {1: "verified", 2: "unsupported"}
    assert final["answerValidation"]["status"] == "needs_review"
    assert final["confidence"]["answer"]["level"] == "red"


def test_folded_grounding_leaves_a_citation_the_review_did_not_judge_unchecked(monkeypatch):
    _stub(
        monkeypatch,
        "Weight was 71 kg [1]. CD4 was 341 cells/uL [2].",
        {"answer_ok": True, "errors": [], "citations": {1: True}},
    )

    _names, final = _final(_profile("folded"))

    statuses = {ref["index"]: ref["groundingStatus"] for ref in final["references"]}
    assert statuses == {1: "verified", 2: "unchecked"}
    assert final["answerValidation"]["status"] == "unavailable"


def test_off_grounding_marks_citations_unchecked_without_a_label(monkeypatch):
    calls = _stub(
        monkeypatch,
        "Weight was 71 kg [1].",
        {"answer_ok": True, "errors": []},
    )

    names, final = _final(_profile("off"))

    assert calls == [("review", "R", False)]
    assert names[0] == "answer_done"
    assert [ref["groundingStatus"] for ref in final["references"]] == ["unchecked"]
    assert final["answerValidation"]["status"] == "checked"
    assert final["confidence"]["answer"]["level"] == "green"


def test_folded_reviewer_is_asked_for_citation_support_and_keeps_only_cited_indices(
    monkeypatch,
):
    seen = {}

    async def fake_chat(_client, _model, messages, *, response_format=None, **_kwargs):
        seen["schema"] = response_format["json_schema"]["schema"]
        seen["prompt"] = messages[-1]["content"]
        return {
            "content": json.dumps(
                {
                    "answer_ok": True,
                    "errors": [],
                    "citations": [
                        {"index": 1, "supported": True},
                        {"index": 9, "supported": False},
                        {"index": 2, "supported": "yes"},
                    ],
                }
            )
        }

    monkeypatch.setattr(team, "_chat", fake_chat)

    verdict = asyncio.run(
        team._validate_answer_label(
            None,
            "R",
            chart="[1] weight\n[2] cd4",
            gathered="",
            answer_text="Weight was 71 kg [1]. CD4 was 341 cells/uL [2].",
            max_tokens=None,
            temperature=0,
            repeat_penalty=None,
            dry=None,
            citation_support=True,
        )
    )

    assert "citations" in seen["schema"]["required"]
    assert "CITATION SUPPORT" in seen["prompt"]
    assert verdict == {"answer_ok": True, "errors": [], "citations": {1: True}}


def test_label_reviewer_is_not_asked_for_a_fix_or_a_corrected_answer(monkeypatch):
    seen = {}

    async def fake_chat(_client, _model, messages, *, response_format=None, **_kwargs):
        seen["schema"] = response_format["json_schema"]["schema"]
        seen["prompt"] = messages[-1]["content"]
        return {"content": json.dumps({"answer_ok": True, "errors": []})}

    monkeypatch.setattr(team, "_chat", fake_chat)

    asyncio.run(
        team._validate_answer_label(
            None,
            "R",
            chart="[1] weight",
            gathered="",
            answer_text="Weight was 71 kg [1].",
            max_tokens=None,
            temperature=0,
            repeat_penalty=None,
            dry=None,
        )
    )

    properties = seen["schema"]["properties"]
    assert set(properties) == {"answer_ok", "errors"}
    assert set(properties["errors"]["items"]["properties"]) == {"wrong", "chart"}
    assert "CITATION SUPPORT" not in seen["prompt"]


_FULL_STAGES = _FAST_STAGES + ["indepth", "indepth_gate"]


def _full_profile(grounding: str):
    models = {"answer": "W", "review": "R", "indepth": "W"}
    if grounding == "separate":
        models["grounding"] = "G"
    return make_profile(
        topology="single",
        stages=_FULL_STAGES,
        models=models,
        prompts={
            "answer": "synthesis-answer",
            "review": "validation-label",
            "indepth": "synthesis-indepth",
        },
        output="product",
        policies={"review": {"action": "label", "grounding": grounding}},
    )


def _stub_indepth(monkeypatch, claims, indepth_verdict):
    calls = _stub(
        monkeypatch,
        "Weight was 71 kg [1].",
        {"answer_ok": True, "errors": [], "citations": {1: True}},
    )

    async def fake_claims(*_args, **_kwargs):
        return list(claims)

    async def fake_indepth_review(_client, model, **kwargs):
        calls.append(("indepth_review", model, kwargs.get("citation_support", False)))
        return dict(indepth_verdict)

    async def entailment(_client, model, pairs, **kwargs):
        calls.append(("entailment", model, kwargs.get("attribution_only", False)))
        return [True for _pair in pairs]

    monkeypatch.setattr(team, "_synthesize_indepth", fake_claims)
    monkeypatch.setattr(team, "_validate_indepth_verdict", fake_indepth_review)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", entailment)
    return calls


def _indepth_statuses(final):
    return {
        ref["index"]: ref["groundingStatus"]
        for ref in final["references"]
        if any(usage.get("location") == "indepth" for usage in ref.get("usage") or [])
    }


def test_separate_grounding_checks_in_depth_citations_for_what_they_attribute(monkeypatch):
    calls = _stub_indepth(
        monkeypatch,
        ["CD4 was 341 cells/uL [2], which calls for monitoring for infections."],
        {"flag": [], "issues": ""},
    )

    _names, final = _final(_full_profile("separate"))

    assert ("entailment", "G", False) in calls
    assert ("entailment", "G", True) in calls
    assert _indepth_statuses(final) == {2: "verified"}
    assert final["inDepth"]["validation"]["status"] == "checked"


def test_folded_grounding_takes_in_depth_citation_verdicts_from_the_review(monkeypatch):
    calls = _stub_indepth(
        monkeypatch,
        ["Weight was 71 kg [1], which is stable.", "CD4 was 500 cells/uL [2]."],
        {"flag": [], "issues": "", "unsupported": [2]},
    )

    _names, final = _final(_full_profile("folded"))

    assert [call for call in calls if call[0] == "entailment"] == []
    assert ("indepth_review", "R", True) in calls
    assert _indepth_statuses(final) == {1: "verified", 2: "unsupported"}
    validation = final["inDepth"]["validation"]
    assert validation["marked"] == [2]
    assert "does not support" in final["inDepth"]["answer"].splitlines()[1]


def test_off_grounding_leaves_in_depth_citations_unchecked_and_unmarked(monkeypatch):
    calls = _stub_indepth(
        monkeypatch,
        ["CD4 was 341 cells/uL [2], which calls for monitoring for infections."],
        {"flag": [], "issues": ""},
    )

    _names, final = _final(_full_profile("off"))

    assert [call for call in calls if call[0] == "entailment"] == []
    assert _indepth_statuses(final) == {2: "unchecked"}
    assert final["inDepth"]["validation"]["status"] == "checked"
    assert "Check:" not in final["inDepth"]["answer"]


def test_attribution_check_asks_only_what_the_claim_attributes_to_its_record(monkeypatch):
    prompts = []

    async def fake_chat(_client, _model, messages, **_kwargs):
        prompts.append(messages[-1]["content"])
        return {"content": json.dumps({"verdicts": ["YES"]})}

    monkeypatch.setattr(team, "_chat", fake_chat)
    pair = [("[20] Temperature 39.7 C", "Temperature 39.7 C indicates a febrile state.")]

    asyncio.run(team._entailment_verdicts(None, "G", pair, attribution_only=True))
    asyncio.run(team._entailment_verdicts(None, "G", pair))

    assert prompts[0].startswith(team._ATTRIBUTION_SYSTEM_PROMPT)
    assert prompts[1].startswith(team._ENTAILMENT_SYSTEM_PROMPT)
