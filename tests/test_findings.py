"""Findings, not grades: what a checked answer ships under the findings policy, and the rules
that keep a check from marking what the chart does not contradict."""

from __future__ import annotations

import dataclasses

import server.team as team
from server.levels_loader import compile_profile
from server.temporal import repair_garbled_dates
from tests.test_staged_stream import _collect, _product_profile, _stub_common

FACTS = {
    "reference_date": "2026-06-20",
    "date_ledger": [{"iso": "2026-01-26", "date_id": "d1"}, {"iso": "2025-12-15", "date_id": "d2"}],
    "date_output_contract": {"allowed_iso_dates": ["2026-01-26", "2025-12-15"]},
}


def test_an_underscore_in_a_chart_date_is_repaired_before_the_check():
    text, repairs = repair_garbled_dates(
        "Weight 71 kg on 2026-01_26 [19]; oximetry on 2025_12-15; visit 2026-02_26.", FACTS
    )

    assert text == "Weight 71 kg on 2026-01-26 [19]; oximetry on 2025-12-15; visit 2026-02_26."
    assert repairs == [
        {"from": "2026-01_26", "to": "2026-01-26"},
        {"from": "2025_12-15", "to": "2025-12-15"},
    ]
    assert repair_garbled_dates("no dates", FACTS) == ("no dates", [])


def test_the_answer_gate_warns_non_destructively_without_overwriting_prose():
    steps: list = []
    answer, _citations, _blocks, gate, original = team._apply_temporal_gate(
        question="What was the last weight?",
        answer_text="The last weight was 71 kg on 2026-01_26 [19].",
        citations=[19],
        blocks=[],
        temporal_facts=FACTS,
        temporal_gate_mode="enforce",
        steps=steps,
    )

    assert answer == "The last weight was 71 kg on 2026-01_26 [19]."
    assert gate["applied"] == "none"
    assert gate["status"] == "warn"
    assert original is None
    assert any(c["id"] == "date_format" and c["status"] == "warn" for c in gate["checks"])


def _with_review_policy(profile, **review):
    policies = dict(profile.policies)
    policies["review"] = {**policies["review"], **review}
    return compile_profile(dataclasses.replace(profile, policies=policies))


def _findings_profile(**overrides):
    return _with_review_policy(_product_profile(**overrides), confidence="findings")


def test_findings_policy_ships_no_grade_and_lists_what_the_checks_found(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Useful answer [1].", [1], []

    async def fake_indepth(*_args, **_kwargs):
        return ["Supported context [1].", "Invented context [99]."], {"level": "green", "note": ""}

    async def unchanged(_client, **kwargs):
        return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)

    done = dict(_collect(_findings_profile(review_model="V")))["done"]

    assert done["confidence"]["answer"]["level"] is None
    assert done["confidence"]["in_depth"]["level"] is None
    assert done["confidence"]["in_depth"]["findings"] == [
        {"check": "record", "where": 2, "status": "fail", "says": "it cites a record not in this chart"}
    ]
    assert done["inDepth"]["validation"]["review_status"] == "clean"
    assert done["inDepth"]["validation"]["marked"] == [2]


def test_levels_policy_keeps_the_grade(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Useful answer [1].", [1], []

    async def unchanged(_client, **kwargs):
        return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)

    done = dict(_collect(_product_profile(review_model="V")))["done"]

    assert done["confidence"]["answer"]["level"] == "green"
    assert done["confidence"]["answer"]["findings"] == []


def test_a_fact_the_record_does_not_hold_leaves_the_answer_checked_with_a_note(monkeypatch):
    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Weight fell from 52 kg to 41 kg [1].", [1], []

    async def unchanged(_client, **kwargs):
        return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

    async def partial(_client, _model, _answer, references, _mappings, **_kwargs):
        output = []
        for reference in references:
            item = dict(reference)
            item["support"] = "not_addressed"
            item["grounded"] = None
            item["groundingStatus"] = "unchecked"
            item["groundingChecks"] = [
                {"status": "unchecked", "support": "not_addressed", "claim": "Weight fell from 52 kg to 41 kg .",
                 "location": "answer", "path": "", "source_indices": [1], "not_found": "52 kg"}
            ]
            output.append(item)
        return output

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)
    monkeypatch.setattr(team, "_ground_references", partial)

    done = dict(_collect(_findings_profile(review_model="V")))["done"]

    assert done["answerValidation"]["status"] == "checked"
    notes = [issue for issue in done["answerValidation"]["issues"] if issue["id"] == "citation_not_addressed"]
    assert notes and notes[0]["status"] == "info" and "52 kg" in notes[0]["reason"]
    assert done["confidence"]["answer"]["findings"] == [
        {"check": "citation", "where": "answer", "status": "info", "says": notes[0]["reason"]}
    ]


def test_a_reviewer_doubt_that_quotes_no_record_is_a_note_not_a_mark(monkeypatch):
    _stub_common(monkeypatch)
    seen = []

    async def fake_answer(*_args, **_kwargs):
        return "The patient is on nevirapine [1].", [1], []

    async def unchanged(_client, **kwargs):
        return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

    async def doubting_review(_client, _model, **kwargs):
        seen.append(kwargs)
        return {
            "answer_ok": False,
            "errors": [{"wrong": "nevirapine", "chart": "No record supports a nevirapine order", "fix": ""}],
        }

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)
    monkeypatch.setattr(team, "_validate_answer_label", doubting_review)

    done = dict(_collect(_findings_profile(review_model="V")))["done"]

    assert done["answerValidation"]["status"] == "checked"
    doubts = [issue for issue in done["answerValidation"]["issues"] if issue["id"] == "review_doubt"]
    assert doubts and doubts[0]["wrong"] == "nevirapine"
    assert seen[0]["dry"] == 0.0


def test_review_policy_accepts_only_known_confidence_values():
    try:
        _with_review_policy(_product_profile(), confidence="grades")
    except ValueError as error:
        assert "confidence" in str(error)
    else:
        raise AssertionError("an unknown confidence policy loaded")


def test_product_profiles_ship_findings():
    from server.levels_loader import compile_execution_spec

    for mode in ("quick", "standard", "verified", "agree"):
        compiled = compile_execution_spec(
            "gemma-4-12b", {"mode": mode, "checks": {"confidence": "findings"}}
        )
        assert compiled.policies["confidence"] == "findings", mode


def test_indepth_pass_entry_and_multirecord_literal_verification(monkeypatch):
    import asyncio
    import pytest
    from tests.test_review_policy import _final, _full_profile, _stub_indepth

    _stub_indepth(monkeypatch, ["CD4 was 341 cells/uL [2]."], {"flag": [], "issues": ""})
    _, final = _final(_full_profile("separate"))
    assert final["inDepth"]["validation"]["citation_checks"][0]["status"] == "pass"

    async def forbidden(*_args, **_kwargs):
        pytest.fail("literal facts unexpectedly dispatched to a model")

    monkeypatch.setattr(team, "_bounded_entailment_verdicts", forbidden)
    mappings = [{"index": i, "date": "2026-01-26", "text": "Weight: 71 kg"} for i in (1, 2)]
    blocks = [
        {
            "kind": "table",
            "columns": [{"key": "w", "label": "Weight"}],
            "rows": [{"cells": {"w": {"text": "71 kg", "refs": [1, 2]}}}],
        }
    ]
    refs = team._resolve_references([1, 2], mappings, blocks=blocks, grounding_status="checking")
    result = asyncio.run(team._ground_references(None, "G", "", refs, mappings))
    assert all(r["groundingChecks"][0]["method"] == "deterministic_exact" for r in result)


def test_findings_policy_retains_unavailable_review_and_mixed_review_doubt(monkeypatch):
    import server.engine as engine

    unavailable_findings = engine._answer_findings(
        engine._State(
            messages=[],
            answer_validation={
                "status": "unavailable",
                "issues": [{"id": "answer_review_unavailable", "reason": "offline"}],
            },
        )
    )
    assert unavailable_findings == [
        {"check": "review", "where": "answer", "status": "unavailable", "says": "offline"}
    ]

    _stub_common(monkeypatch)

    async def fake_answer(*_args, **_kwargs):
        return "Useful answer [1].", [1], []

    async def unchanged(_client, **kwargs):
        return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

    async def mixed_review(*_args, **_kwargs):
        return {
            "answer_ok": False,
            "errors": [
                {"wrong": "first", "chart": "See [1]"},
                {"wrong": "second", "chart": "No chart record states this"},
            ],
        }

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)
    monkeypatch.setattr(team, "_validate_answer_label", mixed_review)

    done = dict(_collect(_findings_profile(review_model="V")))["done"]
    issues = done["answerValidation"]["issues"]
    assert len(issues) == 2
    assert issues[0]["wrong"] == "first"
    assert issues[1]["id"] == "review_doubt" and issues[1]["wrong"] == "second"


def test_quick_profile_preserves_multifact_citations_without_false_edit_or_grade(monkeypatch):
    import asyncio
    import json
    from server.levels_loader import compile_execution_spec
    from tests.factories import _TestTokenCounter, patient_source_registry, stream_profile

    real_gate = team._apply_temporal_gate
    _stub_common(monkeypatch)
    monkeypatch.setattr(team, "_apply_temporal_gate", real_gate)

    mappings = [
        {"resourceType": "obs", "resourceUuid": "u1", "date": "2026-01-07", "text": "(2026-01-07) Assessment — New complaints: Yes"},
        {"resourceType": "obs", "resourceUuid": "u2", "date": "2026-01-07", "text": "(2026-01-07) Finding — Diastolic blood pressure: 50 mmHg"},
        {"resourceType": "obs", "resourceUuid": "u3", "date": "2026-01-07", "text": "(2026-01-07) Finding — Pulse: 157 beats/min"},
        {"resourceType": "obs", "resourceUuid": "u4", "date": "2026-01-07", "text": "(2026-01-07) Finding — Systolic blood pressure: 90 mmHg"},
        {"resourceType": "obs", "resourceUuid": "u5", "date": "2026-01-07", "text": "(2026-01-07) Assessment — Current WHO HIV stage: WHO stage 3"},
        {"resourceType": "condition", "resourceUuid": "u6", "date": "2025-12-31", "text": "(2025-12-31) Condition — Fever: active"},
        {"resourceType": "condition", "resourceUuid": "u7", "date": "2025-11-10", "text": "(2025-11-10) Condition — Vomiting: active"},
        {"resourceType": "condition", "resourceUuid": "u8", "date": "2025-10-22", "text": "(2025-10-22) Condition — Oral candidiasis: active"},
    ]
    chart = "\n".join(f"[{idx}] {m['text']}" for idx, m in enumerate(mappings, start=1))
    source = patient_source_registry(chart, mappings)
    profile = compile_execution_spec(
        "gemma-4-12b", {"mode": "quick", "checks": {"confidence": "findings", "drug_safety": False}}
    )
    expected_indices = [1, 2, 3, 4, 5, 6, 7, 8]

    for draft_answer, expect_canonicalized in (
        (
            "Patient presented on 2026-01-07 with new complaints, pulse 157 bpm, and blood pressure 90/50 mmHg [1], [2], [3], [4], [5]. Active conditions include fever, vomiting, and oral candidiasis [6], [7], [8].",
            False,
        ),
        (
            "Patient presented on 2026-01-07 with new complaints, pulse 157 bpm, and blood pressure 90/50 mmHg [1, 2, 3, 4, 5]. Active conditions include fever, vomiting, and oral candidiasis [6, 7, 8].",
            True,
        ),
    ):
        async def fake_answer(*_args, _ans=draft_answer, **_kwargs):
            return _ans, list(expected_indices), []

        async def unchanged(_client, **kwargs):
            return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

        monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
        monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)

        async def run():
            out = {}
            async for name, raw in stream_profile(
                profile,
                [{"role": "user", "content": "whats up with this patient?"}],
                patient="p39",
                source_registry=source,
                token_counter=_TestTokenCounter(),
            ):
                out[name] = json.loads(raw)
            return out

        done = asyncio.run(run())["done"]
        if expect_canonicalized:
            assert "[1][2][3][4][5]" in done["answer"]
            assert "[6][7][8]" in done["answer"]
        else:
            assert done["answer"] == draft_answer
        assert sorted(ref["index"] for ref in done["references"]) == expected_indices
        assert done["answerValidation"]["status"] == "checked"
        assert "originalAnswer" not in done["answerValidation"]
        assert done["confidence"]["answer"]["level"] is None
