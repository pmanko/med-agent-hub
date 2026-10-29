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


def test_the_answer_gate_repairs_a_garbled_date_instead_of_flagging_it():
    steps: list = []
    answer, _citations, _blocks, gate, _original = team._apply_temporal_gate(
        question="What was the last weight?",
        answer_text="The last weight was 71 kg on 2026-01_26 [19].",
        citations=[19],
        blocks=[],
        temporal_facts=FACTS,
        temporal_gate_mode="enforce",
        steps=steps,
    )

    assert answer == "The last weight was 71 kg on 2026-01-26 [19]."
    assert gate["applied"] == "repair"
    assert gate["date_repairs"] == [{"from": "2026-01_26", "to": "2026-01-26"}]
    assert not [c for c in gate["checks"] if c["id"] == "date_format"]


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
        {"check": "record", "where": 2, "says": "it cites a record not in this chart"}
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
    from server.levels_loader import get_profile

    for profile_id in ("single-12b-checked", "single-12b-answer", "single-31b-xcheck",
                       "single-12b-bare", "single-12b-verify", "single-12b-agree"):
        assert get_profile(profile_id).policies["confidence"] == "findings", profile_id
