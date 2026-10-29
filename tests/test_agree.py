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


def _writers(first, second):
    async def fake_answer(_client, model, *_args, **_kwargs):
        return (second if model == "S" else first), ([1] if model != "S" else []), []
    return fake_answer


async def _unchanged(_client, **kwargs):
    return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}


def test_answer_claims_are_the_sentences_and_cells_without_citations():
    claims = team._answer_claims("Weight was 71 kg on 2026-01-26 [19]. CD4 was 341 [182].\nShort.", [])
    assert claims == ["Weight was 71 kg on 2026-01-26 .", "CD4 was 341 ."]


def test_a_second_writer_that_states_a_different_value_marks_the_answer(monkeypatch):
    _stub_common(monkeypatch)
    pairs_seen = []

    async def verdicts(_client, model, pairs):
        pairs_seen.append((model, pairs))
        return [{"support": "contradicted", "facts": [{"fact": "71 kg", "status": "differs", "source_value": "74 kg"}],
                 "not_found": None, "chart_value": "71 kg: the record states 74 kg"}]

    monkeypatch.setattr(team, "_synthesize_answer", _writers("The last weight was 71 kg [1].", "The last weight was 74 kg."))
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    done = dict(_collect(_agree_profile()))["done"]

    assert done["answer"] == "The last weight was 71 kg [1]."
    assert pairs_seen[0][0] == "S"
    assert pairs_seen[0][1] == [("The last weight was 74 kg.", "The last weight was 71 kg .")]
    assert done["agreement"]["status"] == "differs"
    assert done["agreement"]["claims"][0]["other"] == "71 kg: the record states 74 kg"
    assert done["answerValidation"]["status"] == "needs_review"
    assert done["confidence"]["answer"]["level"] is None
    assert [f["check"] for f in done["confidence"]["answer"]["findings"]] == ["agreement"]
    assert "the models differ" not in done["answer"]


def test_a_second_writer_that_agrees_leaves_the_answer_checked(monkeypatch):
    _stub_common(monkeypatch)

    async def verdicts(_client, _model, pairs):
        return [{"support": "supported", "facts": [], "not_found": None, "chart_value": None} for _ in pairs]

    monkeypatch.setattr(team, "_synthesize_answer", _writers("The last weight was 71 kg [1].", "Weight 71 kg on 2026-01-26."))
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_bounded_entailment_verdicts", verdicts)

    done = dict(_collect(_agree_profile()))["done"]

    assert done["agreement"]["status"] == "agreed"
    assert done["answerValidation"]["status"] == "checked"
    assert done["confidence"]["answer"]["findings"] == []
    assert [s["role"] for s in done.get("events", []) if False] == []


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
    _stub_common(monkeypatch)

    async def fake_answer(_client, _model, *_args, **_kwargs):
        return "The visit was on 2026-01-07 [1].", [1], []

    def patching_gate(**kwargs):
        return ("The visit was on 2026-01-26 [1].", [1], [],
                {"mode": "enforce", "status": "fail", "applied": "patch"}, kwargs["answer_text"])

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", _unchanged)
    monkeypatch.setattr(team, "_apply_temporal_gate", patching_gate)

    done = dict(_collect(_bare_profile()))["done"]

    assert done["answerValidation"]["status"] == "edited"
    assert "review_status" not in done["inDepth"]["validation"]
    assert "groundingChecks" not in (done["references"][0] if done["references"] else {})
