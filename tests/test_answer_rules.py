"""Deterministic answer rules (umbrella spec ANSWER-RULES-2026-10), run on the fixture charts with
the answers and In-Depth points runs D3 and D4 (28 October 2026) produced and the judges faulted,
and on near misses each rule must leave alone."""

from __future__ import annotations

from pathlib import Path

import pytest

import server.team as team
from server import answer_rules
from server.temporal import build_temporal_facts, gate_indepth_claims

CHARTS = Path(__file__).resolve().parent / "fixtures" / "charts"
REFERENCE = "2026-06-20"


@pytest.fixture(scope="module")
def aloice():
    return build_temporal_facts((CHARTS / "aloice-mukangu.snapshot.txt").read_text(), REFERENCE)


@pytest.fixture(scope="module")
def zabella():
    return build_temporal_facts((CHARTS / "zabella-halambe.snapshot.txt").read_text(), REFERENCE)


def _ids(findings, status=None):
    return [f["id"] for f in findings if status is None or f["status"] == status]


# R1 window ------------------------------------------------------------------------------------------


def test_window_drops_old_table_rows_and_says_how_many(aloice):
    def row(name, date, index):
        return {"cells": {"medication_name": {"text": name, "refs": [index]}, "order_date": {"text": date, "refs": [index]}}}

    blocks = [{"kind": "table", "rows": [
        row("Efavirenz", "2026-01-07", 4),
        row("Efavirenz", "2025-12-31", 34),
        row("Efavirenz", "2025-12-16", 71),
        row("Efavirenz", "2025-11-10", 112),
    ]}]
    text, citations, new_blocks, findings = answer_rules.apply_answer_rules(
        "Show this patient's medication orders in the past 6 months as a table.",
        "The patient's medication orders in the past 6 months are listed in the table below.",
        [], blocks, aloice,
    )
    assert [r["cells"]["order_date"]["text"] for r in new_blocks[0]["rows"]] == ["2026-01-07", "2025-12-31"]
    assert "before 2025-12-20, outside the past 6 months, are left out (2 records)" in text
    assert _ids(findings) == ["rule_window"] and findings[0]["status"] == "info"
    assert len(blocks[0]["rows"]) == 4, "the caller's blocks are not mutated"


def test_window_prunes_old_citations_and_drops_sentences_citing_only_old_records(aloice):
    answer = (
        "Over the past 6 months, drug orders were placed for trimethoprim and sulfamethoxazole "
        "[2][37][72][114][147] and efavirenz [4][34][71]. Additionally, chest X-rays were ordered "
        "and a CD4 panel [307]."
    )
    text, citations, _blocks, findings = answer_rules.apply_answer_rules(
        "What has been ordered for this patient over the past 6 months?", answer, [], [], aloice
    )
    assert "trimethoprim and sulfamethoxazole [2][37]" in text
    assert "efavirenz [4][34]" in text
    assert "CD4 panel" not in text
    assert "rule_window" in _ids(findings)
    assert 307 not in citations and {2, 37, 4, 34} <= set(citations)


def test_window_leaves_an_answer_without_a_window_question_alone(aloice):
    answer = "The CD4 count was 72 cells/uL on 2025-10-22 [311]."
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "What was the CD4 count?", answer, [311], [], aloice
    )
    assert "rule_window" not in _ids(findings)
    assert text.startswith(answer)


# R2 relative time -----------------------------------------------------------------------------------


def test_relative_time_marks_a_span_the_cited_dates_contradict(zabella):
    """Run D4: "a decrease from 75 to 71 kg over the last few months" for December and January
    records under a June reference date (judged wrong_fact)."""
    marks = answer_rules.claim_marks(
        "Her weight decreased from 75 kg [90] to 71 kg [19] over the last few months.", zabella
    )
    assert _ids(marks) == ["rule_relative_time"]
    assert "2025-12-15 to 2026-01-26, not the last few months before 2026-06-20" in marks[0]["reason"]


def test_relative_time_leaves_spans_it_cannot_decide(zabella):
    # anchored to a stated date
    assert not answer_rules.claim_marks(
        "She reported most adherence for the past week on 2026-01-26 [11].", zabella
    )
    # quoting the record's own question ("Antiretroviral adherence in past week: Most"); both
    # judges called this point correct in run D4
    assert not answer_rules.claim_marks("Records show 'Most' adherence in the past week [11].", zabella)
    # no span named
    assert not answer_rules.claim_marks("Adherence was poor in previous months [46].", zabella)
    assert not answer_rules.claim_marks(
        "Over the past year her weight went from 74 kg [350] to 71 kg [19].", zabella
    )


# R5 drug citation -----------------------------------------------------------------------------------


def test_drug_citation_is_corrected_to_the_order_that_names_the_drug(zabella):
    answer = "Stavudine [1] and nevirapine [2] were prescribed."
    text, citations, _b, findings = answer_rules.apply_answer_rules(
        "Was she ever on stavudine?", answer, [1, 2], [], zabella
    )
    assert text.startswith("Stavudine [3] and nevirapine [1] were prescribed.")
    assert _ids(findings, "info").count("rule_drug_citation") == 2


def test_drug_citation_drops_another_drugs_record_from_a_group(aloice):
    text, _c, _b, _f = answer_rules.apply_answer_rules(
        "What drugs were ordered on 2025-12-31?",
        "Trimethoprim and sulfamethoxazole [2][34][37] were ordered.",
        [2, 34, 37], [], aloice,
    )
    assert text.startswith("Trimethoprim and sulfamethoxazole [2][37] were ordered.")


def test_drug_citation_leaves_correct_citations_and_text_without_a_drug_name(zabella):
    answer = "The regimen includes nevirapine [1], lamivudine [2], and stavudine [3]. Adherence was Most [11]."
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "What medications is this patient on?", answer, [1, 2, 3, 11], [], zabella
    )
    assert text == answer
    assert findings == []


# R6 drug class --------------------------------------------------------------------------------------


def test_drug_class_marks_a_tb_combination_called_antiretroviral(aloice):
    marks = answer_rules.claim_marks(
        "The patient's antiretroviral regimen includes efavirenz [4], lamivudine / zidovudine [3] "
        "and rifampicin isoniazid pyrazinamide and ethambutol [5].",
        aloice,
    )
    assert _ids(marks) == ["rule_drug_class"]
    assert "Rifampicin isoniazid pyrazinamide and ethambutol is not antiretroviral" in marks[0]["reason"]


def test_drug_class_leaves_a_sentence_that_names_the_other_class(aloice):
    assert not answer_rules.claim_marks(
        "She takes efavirenz [4] as antiretroviral therapy and rifampicin isoniazid pyrazinamide "
        "and ethambutol [5] for tuberculosis.",
        aloice,
    )


# R7 demographics citation ---------------------------------------------------------------------------


def test_demographics_record_cited_for_a_clinical_fact_is_marked(zabella):
    marks = answer_rules.claim_marks("The patient is living with HIV [365].", zabella)
    assert _ids(marks) == ["rule_demographics_citation"]
    assert not answer_rules.claim_marks("The patient is a female born in 1998 [365].", zabella)


# R8 advanced HIV disease ----------------------------------------------------------------------------


def test_denying_advanced_hiv_disease_with_who_stage_3_is_marked(zabella):
    marks = answer_rules.claim_marks(
        "With a CD4 count of 341 cells/uL [182], the patient does not meet the criteria for advanced HIV disease.",
        zabella,
    )
    assert _ids(marks) == ["rule_advanced_hiv"]
    assert "[20] records WHO stage 3 on 2026-01-26" in marks[0]["reason"]


def test_a_cd4_count_above_200_alone_is_not_a_denial(zabella):
    assert not answer_rules.claim_marks(
        "The most recent CD4 count of 341 cells/uL [182] is above the threshold for advanced HIV disease.",
        zabella,
    )


# R9 diagnosis presence ------------------------------------------------------------------------------


def test_saying_no_diagnosis_is_recorded_with_an_hiv_programme_is_marked(zabella):
    marks = answer_rules.claim_marks(
        "The patient's record does not explicitly state a primary diagnosis, but she takes nevirapine [1].",
        zabella,
    )
    assert "rule_diagnosis_presence" in _ids(marks)
    finding = next(m for m in marks if m["id"] == "rule_diagnosis_presence")
    assert "Human immunodeficiency virus (HIV) disease" in finding["reason"]


# R3 stale latest value and R4 series ----------------------------------------------------------------


def test_a_trend_answer_gets_its_readings_and_the_gap_to_the_reference_date(aloice):
    text, citations, _b, findings = answer_rules.apply_answer_rules(
        "How has this patient's weight changed over the past year?",
        "The patient's weight dropped from 52 kg on 2025-10-22 [287] to 41 kg by 2026-01-07 [21].",
        [287, 21], [], aloice,
    )
    assert (
        "Recorded weight readings: 52 kg (2025-10-22) [287], 52 kg (2025-10-23) [217], "
        "49.5 kg (2025-10-26) [184], 47 kg (2025-11-10) [132]"
    ) in text
    assert "41 kg (2026-01-07) [21]." in text
    assert "The latest weight in the chart is from 2026-01-07 [21], about 5.5 months before 2026-06-20." in text
    assert {"rule_series", "rule_stale"} <= set(_ids(findings, "info"))
    assert {184, 132, 89, 56} <= set(citations)


def test_steady_about_a_series_that_changes_direction_is_marked(aloice):
    _t, _c, _b, findings = answer_rules.apply_answer_rules(
        "How has this patient's weight changed over the past year?",
        "Weight fell steadily from 52 kg [287] through 49.5 kg [184], 47 kg [132], 48 kg [89], "
        "42 kg [56] to 41 kg [21]; no later weight is recorded.",
        [287, 184, 132, 89, 56, 21], [], aloice,
    )
    assert "rule_series_shape" in _ids(findings, "fail")
    assert "rule_series" not in _ids(findings), "every reading is already in the answer"
    assert "rule_stale" not in _ids(findings), "the answer already says no later weight exists"


def test_series_rules_leave_answers_the_question_does_not_ask_about_a_measurement(aloice):
    _t, _c, _b, findings = answer_rules.apply_answer_rules(
        "When was the last visit?",
        "The last visit was an Adult Visit on 2026-01-07 [6]; weight was 41 kg [21].",
        [6, 21], [], aloice,
    )
    assert not {"rule_series", "rule_stale"} & set(_ids(findings))


# Where the rules run --------------------------------------------------------------------------------


def test_the_answer_gate_applies_the_rule_edits_and_reports_them(zabella):
    steps: list = []
    answer, citations, _blocks, gate, original = team._apply_temporal_gate(
        question="Which of those are antiretrovirals?",
        answer_text="Stavudine [1] and nevirapine [2] are antiretrovirals.",
        citations=[1, 2],
        blocks=[],
        temporal_facts=zabella,
        temporal_gate_mode="enforce",
        steps=steps,
    )
    assert answer.startswith("Stavudine [3] and nevirapine [1]")
    assert original == "Stavudine [1] and nevirapine [2] are antiretrovirals."
    assert gate["applied"] == "edit" and gate["rule_edits"] == ["rule_drug_citation", "rule_drug_citation"]
    assert gate["status"] == "pass", "an edit from the chart is a note, not a mark"


def test_the_answer_gate_in_warn_mode_reports_marks_without_editing(zabella):
    answer, _c, _b, gate, original = team._apply_temporal_gate(
        question="Does she have advanced HIV disease?",
        answer_text="She does not have advanced HIV disease; her CD4 count is 341 cells/uL [182].",
        citations=[182],
        blocks=[],
        temporal_facts=zabella,
        temporal_gate_mode="warn",
        steps=[],
    )
    assert original is None and answer.startswith("She does not have advanced HIV disease")
    assert "rule_advanced_hiv" in [c["id"] for c in gate["checks"]]


def test_an_in_depth_point_a_rule_contradicts_is_flagged(aloice):
    result = gate_indepth_claims(
        [
            "The patient was started on new antiretroviral therapy including efavirenz [4] and "
            "rifampicin isoniazid pyrazinamide and ethambutol [5].",
            "Efavirenz [4] is an antiretroviral.",
        ],
        aloice,
        mode="enforce",
    )
    assert result["flagged"] == [1]
    failing = [c["id"] for c in result["checks"][0]["gate"]["checks"] if c["status"] == "fail"]
    assert failing == ["rule_drug_class"]
