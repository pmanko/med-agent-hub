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


# R7 demographics citation ---------------------------------------------------------------------------


def test_demographics_record_cited_for_a_clinical_fact_is_marked(zabella):
    marks = answer_rules.claim_marks("The patient is living with HIV [365].", zabella)
    assert _ids(marks) == ["rule_demographics_citation"]
    assert not answer_rules.claim_marks("The patient is a female born in 1998 [365].", zabella)


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
        question="What is she being treated for?",
        answer_text="The record does not explicitly state a primary diagnosis; she takes nevirapine [1].",
        citations=[1],
        blocks=[],
        temporal_facts=zabella,
        temporal_gate_mode="warn",
        steps=[],
    )
    assert original is None and answer.startswith("The record does not explicitly state")
    assert "rule_diagnosis_presence" in [c["id"] for c in gate["checks"]]


def test_an_in_depth_point_a_rule_contradicts_is_flagged(zabella):
    result = gate_indepth_claims(
        ["HIV status is documented in the chart [365].", "Stavudine was ordered [3]."],
        zabella,
        mode="enforce",
    )
    assert result["flagged"] == [1]
    failing = [c["id"] for c in result["checks"][0]["gate"]["checks"] if c["status"] == "fail"]
    assert failing == ["rule_demographics_citation"]


def test_the_chart_measuring_rhythm_decides_when_a_value_is_old():
    def series(*dates):
        return {"points": [{"date": d, "value": 1} for d in dates]}

    reference = answer_rules._date("2026-06-20")
    # monthly readings, then five months of silence
    assert answer_rules._overdue(series("2026-01-04", "2026-01-19", "2026-02-02"), reference)
    # yearly readings, five months since the last
    assert not answer_rules._overdue(series("2024-01-10", "2025-01-12", "2026-01-15"), reference)
    # one reading has no rhythm
    assert not answer_rules._overdue(series("2025-01-01"), reference)


_DIABETES_CHART = """Patient records (most recent first):
[1] (2026-03-02) Drug order: Metformin. Action: REVISE. Urgency: ROUTINE
[2] (2026-03-02) Drug order: Amlodipine. Action: NEW. Urgency: ROUTINE
[3] (2026-03-02) Systolic blood pressure: 150 mmHg
[4] (2026-03-02) HbA1c: 8.1 %
[5] (2026-01-05) Systolic blood pressure: 138 mmHg
[6] (2026-01-05) HbA1c: 7.4 %
[7] (2025-11-03) Systolic blood pressure: 145 mmHg
[8] (2025-11-03) Drug order: Metformin. Action: NEW. Urgency: ROUTINE
[9] (2025-09-01) Program: Diabetes care program. Status: Active. Current state: Enrolled
[10] (2025-09-01) Patient: Test Person. Male. Born 1960-02-01. Address: Town. Identifiers: X1
"""


def test_the_rules_read_another_disease_from_its_chart():
    """Behavior on a chart unlike the ones the rules were written from (diabetes and
    hypertension, no HIV): every rule takes its drugs, conditions and measurements from it."""
    facts = build_temporal_facts(_DIABETES_CHART, "2026-09-01")
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "How has her systolic blood pressure changed over the past 6 months?",
        "Amlodipine [1] was started, and systolic blood pressure rose from 138 mmHg [5] to 150 mmHg [3]. "
        "It was 145 mmHg in November [7].",
        [1, 5, 3, 7], [], facts,
    )
    assert "Amlodipine [2]" in text, "the citation moves to the amlodipine order"
    assert "145 mmHg in November" not in text, "a sentence citing only records before the window goes"
    assert "The latest systolic blood pressure in the chart is from 2026-03-02 [3]" in text
    assert {"rule_drug_citation", "rule_window", "rule_stale"} <= set(_ids(findings, "info"))
    marks = answer_rules.claim_marks("The record does not state a diagnosis for her metformin [1].", facts)
    assert _ids(marks) == ["rule_diagnosis_presence"] and "Diabetes care program" in marks[0]["reason"]
    assert _ids(answer_rules.claim_marks("She has diabetes [10].", facts)) == ["rule_demographics_citation"]


_VACCINE_CHART = """Patient records (most recent first):
[1] (2026-01-25) Drug order: Hemophilus influenza B vaccine. Action: NEW. Urgency: ROUTINE
[2] (2026-01-25) Drug order: Polio vaccine (oral). Action: NEW. Urgency: ROUTINE
[3] (2026-01-25) Drug order: Diphtheria tetanus and pertussis vaccination. Action: NEW. Urgency: ROUTINE
[4] (2026-01-25) Drug order: Isoniazid. Action: NEW. Urgency: ROUTINE
[5] (2026-01-25) Weight (kg): 11 kg
[6] (2025-12-22) Weight (kg): 11 kg
"""


def test_an_abbreviation_names_the_order_its_initials_spell():
    """Held-out pass (28 October): "Hib" and "DTP" in a cited list were read as naming no order."""
    facts = build_temporal_facts(_VACCINE_CHART, "2026-06-20")
    assert not answer_rules.claim_marks("Vaccines were ordered (Hib, Polio, DTP) [1][2][3].", facts)
    marks = answer_rules.claim_marks("Vaccines were ordered (Hib, Polio, DTP) [1][2][3][4].", facts)
    assert _ids(marks) == ["rule_drug_citation"] and "[4] is the order for Isoniazid" in marks[0]["reason"]


def test_an_answer_with_nothing_in_the_window_keeps_its_records_behind_a_chart_lead():
    """Held-out pass: leaving out every record older than "the last 3 months" emptied the answer.
    With nothing of those kinds inside the window, the answer keeps them and says they are older."""
    facts = build_temporal_facts(_VACCINE_CHART, "2026-06-20")
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "Has anything changed in the last 3 months?",
        "Weight stayed at 11 kg from 2025-12-22 [6] to 2026-01-25 [5].",
        [5, 6], [], facts,
    )
    assert text.startswith("The chart has no records of these kinds dated within the last 3 months (from 2026-03-20)")
    assert text.endswith("Weight stayed at 11 kg from 2025-12-22 [6] to 2026-01-25 [5].")
    assert _ids(findings, "info") == ["rule_window"]
    # an answer that already says so is left alone
    stated = "There are no records within the last three months; the last weight was 11 kg on 2026-01-25 [5]."
    assert answer_rules.apply_answer_rules("Has anything changed in the last 3 months?", stated, [5], [], facts)[0] == stated


def test_an_answer_that_misses_the_windows_records_is_marked_not_edited():
    chart = _VACCINE_CHART.replace("Patient records (most recent first):\n", "Patient records (most recent first):\n[7] (2026-05-01) Weight (kg): 12 kg\n")
    facts = build_temporal_facts(chart, "2026-06-20")
    answer = "Weight stayed at 11 kg from 2025-12-22 [6] to 2026-01-25 [5]."
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "Has anything changed in the last 3 months?", answer, [5, 6], [], facts
    )
    assert text.startswith(answer)
    window = next(f for f in findings if f["id"] == "rule_window")
    assert window["status"] == "fail" and "latest 2026-05-01 [7]" in window["reason"]


def test_an_old_item_inside_a_sentence_about_the_window_is_named_not_claimed_as_left_out():
    """Judged run R10: "records before 2025-12-20 are left out" while an item citing only older
    orders stayed in the sentence. The rule names such an item instead."""
    chart = _VACCINE_CHART + "[9] (2025-11-01) Drug order: Dapsone. Action: NEW. Urgency: ROUTINE\n"
    facts = build_temporal_facts(chart, "2026-06-20")
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "What medications were ordered in the past 6 months?",
        "Orders in the past 6 months: isoniazid [4] and dapsone [9].",
        [4, 9], [], facts,
    )
    window = [f for f in findings if f["id"] == "rule_window"]
    assert [f["status"] for f in window] == ["fail"]
    assert "[9] (2025-11-01)" in window[0]["reason"] and "left out" not in text


def test_an_old_item_the_answer_frames_as_older_is_not_named():
    chart = _VACCINE_CHART + "[9] (2025-11-01) Drug order: Dapsone. Action: NEW. Urgency: ROUTINE\n"
    facts = build_temporal_facts(chart, "2026-06-20")
    _t, _c, _b, findings = answer_rules.apply_answer_rules(
        "What medications were ordered in the past 6 months?",
        "Isoniazid was ordered [4]; dapsone was a previous order [9].",
        [4, 9], [], facts,
    )
    assert not [f for f in findings if f["id"] == "rule_window" and f["status"] == "fail"]


def test_rule_scoping_table_cells_and_boundary_cases():
    chart = """Patient records (most recent first):
[1] (2026-06-01) Drug order: Metformin. Action: DISCONTINUE. Urgency: ROUTINE
[2] (2026-05-01) Drug order: Amlodipine. Action: NEW. Urgency: ROUTINE
[3] (2026-04-01) Drug order: Metformin. Action: NEW. Urgency: ROUTINE
[4] (2026-06-01) Weight (kg): 72 kg
[5] (2026-05-01) Weight (kg): 68 kg
[6] (2026-04-01) Weight (kg): 70 kg
[7] (2026-06-01) Systolic blood pressure: 120 mmHg
[8] (2026-05-01) Systolic blood pressure: 120 mmHg
[9] (2026-04-01) Systolic blood pressure: 120 mmHg
[10] (2026-05-01) Program: Diabetes care program. Status: Active. Current state: Enrolled
[11] (2026-06-01) Diagnosis: No tuberculosis diagnosis documented
"""
    facts = build_temporal_facts(chart, "2026-06-20")

    # Date/action mismatch on drug replacement retains draft and reports rule_drug_citation fail
    text, _c, _b, findings = answer_rules.apply_answer_rules(
        "What was prescribed?", "Metformin was started on 2026-05-01 [2].", [2], [], facts
    )
    assert text == "Metformin was started on 2026-05-01 [2]."
    assert _ids(findings, "fail") == ["rule_drug_citation"]

    # Warn mode assesses original text without simulated repair
    w_text, _wc, _wb, w_findings = answer_rules.apply_answer_rules(
        "What was prescribed?", "Amlodipine was prescribed [1].", [1], [], facts, edit=False
    )
    assert w_text == "Amlodipine was prescribed [1]."
    assert _ids(w_findings, "fail") == ["rule_drug_citation"]

    # Table cell wrong drug citation is edited in enforce mode and flagged in warn mode
    blocks = [{"kind": "table", "rows": [{"cells": {"drug": {"text": "Amlodipine", "refs": [1]}}}]}]
    _bt, _bc, new_blocks, b_findings = answer_rules.apply_answer_rules(
        "What was prescribed?", "The orders are shown below.", [], blocks, facts
    )
    assert new_blocks[0]["rows"][0]["cells"]["drug"]["refs"] == [2]
    assert _ids(b_findings, "info") == ["rule_drug_citation"]

    # Specific diagnosis denial is not contradicted by an unrelated program or negative record
    assert answer_rules.claim_marks("No diagnosis of tuberculosis is documented [11].", facts) == []
    assert answer_rules.claim_marks("No diagnosis of HIV is documented.", facts) == []
    assert answer_rules.claim_marks("No diagnosis of TB is documented.", facts) == []

    # Condition and linked Drug order records count as diagnosis evidence
    cond_facts = build_temporal_facts(
        "Patient records (most recent first):\n"
        "[1] (2026-06-01) Condition: Hypertension. Status: ACTIVE\n",
        "2026-06-20",
    )
    cond_marks = answer_rules.claim_marks("The record does not state any diagnosis.", cond_facts)
    assert _ids(cond_marks) == ["rule_diagnosis_presence"] and "Hypertension" in cond_marks[0]["reason"]

    linked_order_facts = build_temporal_facts(
        "Patient records (most recent first):\n"
        "[1] (2026-06-01) Drug order: Metformin. Indication: Type 2 diabetes mellitus. Action: NEW\n",
        "2026-06-20",
    )
    order_marks = answer_rules.claim_marks("The record does not state any diagnosis.", linked_order_facts)
    assert _ids(order_marks) == ["rule_diagnosis_presence"] and "Type 2 diabetes mellitus" in order_marks[0]["reason"]

    # Mixed-age table row drops old cell refs and keeps in-window cell refs
    mixed_blocks = [{"kind": "table", "rows": [{"cells": {"drug": {"text": "Metformin", "refs": [1, 3]}}}]}]
    m_text, m_cites, m_blocks, m_findings = answer_rules.apply_answer_rules(
        "Show medication orders in the past 1 month as a table.",
        "Orders are shown below.",
        [1, 3],
        mixed_blocks,
        facts,
    )
    assert m_blocks[0]["rows"][0]["cells"]["drug"]["refs"] == [1]
    assert m_cites == [1]
    assert _ids(m_findings, "info") == ["rule_window"]

    # Monthly series last measured < 90 days ago is not marked rule_stale
    recent_overdue_facts = build_temporal_facts(
        "Patient records (most recent first):\n"
        "[1] (2026-05-06) Weight (kg): 70 kg\n"
        "[2] (2026-04-20) Weight (kg): 70 kg\n"
        "[3] (2026-04-05) Weight (kg): 70 kg\n",
        "2026-06-20",
    )
    _rt, _rc, _rb, r_findings = answer_rules.apply_answer_rules(
        "What is the patient's weight?", "Weight was 70 kg [1].", [1], [], recent_overdue_facts
    )
    assert "rule_stale" not in _ids(r_findings)

    # Steady wording on another measurement, drug adherence, or negated steady wording is not flagged
    for q, ans in (
        (
            "How have weight and systolic blood pressure changed?",
            "Weight fell from 70 kg [6] to 68 kg [5] then rose to 72 kg [4]. Systolic blood pressure stayed steady at 120 mmHg [7][8][9].",
        ),
        (
            "How has weight changed?",
            "Weight fell from 70 kg [6] to 68 kg [5] then rose to 72 kg [4]. The patient consistently took amlodipine [2].",
        ),
        (
            "How has weight changed?",
            "Weight did not fall steadily: it fell from 70 kg [6] to 68 kg [5] then rose to 72 kg [4].",
        ),
        (
            "How has weight changed over the past 2 months?",
            "The weight was 70 kg on 2026-04-01 [6], before the requested window. It rose from 68 kg [5] to 72 kg [4].",
        ),
    ):
        _st, _sc, _sb, s_findings = answer_rules.apply_answer_rules(q, ans, [], [], facts)
        assert not [f for f in s_findings if f["status"] == "fail"], ans

    # Negative window statement for a specific drug is not contradicted by another drug's order
    drug_window_facts = build_temporal_facts(
        "Patient records (most recent first):\n"
        "[1] (2026-06-01) Drug order: Amlodipine. Action: NEW. Urgency: ROUTINE\n"
        "[2] (2026-01-01) Drug order: Metformin. Action: NEW. Urgency: ROUTINE\n",
        "2026-06-20",
    )
    neg_ans = "No orders in the last 3 months for metformin; it was last ordered on 2026-01-01 [2]."
    dw_text, _dwc, _dwb, dw_findings = answer_rules.apply_answer_rules(
        "Was metformin ordered in the last 3 months?", neg_ans, [2], [], drug_window_facts
    )
    assert dw_text == neg_ans and dw_findings == []

    # Constant series with all readings already cited/dated is not duplicated; >8 readings deduplicate in chronological order
    const_facts = build_temporal_facts(
        "Patient records (most recent first):\n"
        "[1] (2026-06-01) Weight (kg): 70 kg\n"
        "[2] (2026-05-01) Weight (kg): 70 kg\n"
        "[3] (2026-04-01) Weight (kg): 70 kg\n",
        "2026-06-20",
    )
    const_ans = "Recorded weight readings: 70 kg (2026-04-01) [3], 70 kg (2026-05-01) [2], 70 kg (2026-06-01) [1]."
    c_text, _cc, _cb, c_findings = answer_rules.apply_answer_rules(
        "How has weight changed?", const_ans, [3, 2, 1], [], const_facts
    )
    assert c_text == const_ans and c_findings == []

    long_chart = "Patient records (most recent first):\n" + "\n".join(
        f"[{i}] (2026-06-{20 - i:02d}) Weight (kg): {60 + (i % 3)} kg" for i in range(1, 10)
    )
    long_facts = build_temporal_facts(long_chart, "2026-06-20")
    l_text, _lc, _lb, l_findings = answer_rules.apply_answer_rules(
        "How has weight changed?", "Weight changed from 60 kg [9] to 61 kg [1].", [9, 1], [], long_facts
    )
    assert "9 weight readings; first, lowest, highest and last:" in l_text
    assert _ids(l_findings, "info") == ["rule_series"]

    # Oversized/non-positive window spans and empty/missing facts degrade cleanly; future-dated citations in window fail
    for bad_q in ("last 0 days", "past 10000 years", "past 999999999999999999999999 days"):
        assert answer_rules.asked_window(bad_q, facts) is None
    assert answer_rules.apply_answer_rules("What was prescribed?", "Amlodipine [1].", [1], [], None)[3] == []
    future_facts = build_temporal_facts(
        "Patient records (most recent first):\n[4] (2026-07-01) Weight (kg): 70 kg\n", "2026-06-20"
    )
    _ft, _fc, _fb, f_findings = answer_rules.apply_answer_rules(
        "What happened in the last month?", "Weight was 70 kg [4].", [4], [], future_facts
    )
    assert _ids(f_findings, "fail") == ["rule_window"]


def test_stage_engine_streams_rule_findings_inline_indepth_reasons_and_edit_history(monkeypatch):
    import asyncio
    import json
    from tests.factories import make_profile, patient_source_registry, stream_profile

    chart = """Patient records (most recent first):
[1] (2026-05-01) Program: Diabetes care program. Status: Active. Current state: Enrolled
[2] (2026-05-01) Drug order: Metformin. Action: NEW. Urgency: ROUTINE
[3] (2026-05-01) Drug order: Amlodipine. Action: NEW. Urgency: ROUTINE
[4] (2026-05-01) Patient: Test Person. Male. Born 1960-02-01
"""
    mappings = [
        {"index": 1, "resourceType": "Program", "resourceUuid": "p1", "date": "2026-05-01", "text": "Program: Diabetes care program. Status: Active. Current state: Enrolled"},
        {"index": 2, "resourceType": "Drug order", "resourceUuid": "o1", "date": "2026-05-01", "text": "Drug order: Metformin. Action: NEW. Urgency: ROUTINE"},
        {"index": 3, "resourceType": "Drug order", "resourceUuid": "o2", "date": "2026-05-01", "text": "Drug order: Amlodipine. Action: NEW. Urgency: ROUTINE"},
        {"index": 4, "resourceType": "Patient", "resourceUuid": "pt1", "date": "2026-05-01", "text": "Patient: Test Person. Male. Born 1960-02-01"},
    ]
    registry = patient_source_registry(chart, mappings)
    monkeypatch.setattr(team, "_write_trace", lambda *a, **k: None)

    # 1. Rule failure preserved through model review and In-Depth rule mark rendered with check="rule" and inline reason
    async def synth_fail(*_a, **_k):
        return "The record does not state a diagnosis, but metformin was ordered [2].", [2], []

    async def review_ok(*_a, **_k):
        return {"answer_ok": True, "errors": []}

    async def indepth_fail(*_a, **_k):
        return ["She has diabetes [4]."], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_synthesize_answer", synth_fail)
    monkeypatch.setattr(team, "_validate_answer_label", review_ok)
    monkeypatch.setattr(team, "_gen_indepth", indepth_fail)

    reviewed_profile = make_profile(
        topology="single",
        output="product",
        stages=["context", "answer", "gate", "resolve_refs", "review", "gate", "final_resolve_refs", "ground_verdicts", "indepth", "indepth_gate"],
        models={"answer": "W", "review": "R", "indepth": "W"},
        policies={"anchor": "2026-06-20", "confidence": "findings", "review": {"action": "label", "grounding": "off"}},
        prompts={"answer": "synthesis-answer", "review": "validation-label", "indepth": "synthesis-indepth"},
    )

    async def collect(profile):
        events = []
        async for ev, raw in stream_profile(
            profile, [{"role": "user", "content": "What is she being treated for?"}], patient="p", source_registry=registry
        ):
            events.append((ev, json.loads(raw)))
        return dict(events)

    done = asyncio.run(collect(reviewed_profile))["done"]
    assert done["answerValidation"]["status"] == "needs_review"
    assert done["confidence"]["answer"]["findings"] == [
        {"check": "rule", "where": "answer", "status": "fail", "says": "[1] records Diabetes care program"}
    ]
    assert done["confidence"]["in_depth"]["findings"] == [
        {
            "check": "rule",
            "where": 1,
            "status": "fail",
            "says": "[4] is the patient demographics record and states no clinical fact",
        }
    ]
    assert "[4] is the patient demographics record and states no clinical fact" in done["inDepth"]["answer"]

    # 2. Rule edit tracks originalAnswer and sets status="edited"
    async def synth_edit(*_a, **_k):
        return "Amlodipine [2] was prescribed.", [2], []

    async def unchanged(_client, **kwargs):
        return kwargs["answer_text"], kwargs["citations"], kwargs["blocks"], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_synthesize_answer", synth_edit)
    monkeypatch.setattr(team, "_ensure_substantive_answer", unchanged)
    bare_profile = make_profile(
        topology="single",
        output="product",
        stages=["context", "answer", "gate", "resolve_refs", "final_resolve_refs"],
        models={"answer": "W"},
        policies={"anchor": "2026-06-20", "confidence": "findings"},
        prompts={"answer": "synthesis-answer"},
    )
    edited_done = asyncio.run(collect(bare_profile))["done"]
    assert edited_done["answer"] == "Amlodipine [3] was prescribed."
    assert edited_done["answerValidation"]["status"] == "edited"
    assert edited_done["answerValidation"]["originalAnswer"] == "Amlodipine [2] was prescribed."
    assert edited_done["confidence"]["answer"]["findings"][0]["check"] == "rule"


def test_compact_comma_and_slash_citation_groups_prune_older_refs_and_keep_in_window_items():
    chart = """Patient records (most recent first):
[2] (2026-06-15) Drug order: Sulfamethoxazole 400 MG / trimethoprim 80 MG Oral Tablet. Action: NEW. Urgency: ROUTINE
[5] (2026-06-15) Drug order: Abacavir 600 MG / Lamivudine 300 MG / Dolutegravir 50 MG. Action: NEW. Urgency: ROUTINE
[34] (2026-03-19) Drug order: Sulfamethoxazole 400 MG / trimethoprim 80 MG Oral Tablet. Action: NEW. Urgency: ROUTINE
[37] (2026-02-19) Drug order: Sulfamethoxazole 400 MG / trimethoprim 80 MG Oral Tablet. Action: NEW. Urgency: ROUTINE
[72] (2025-11-26) Drug order: Sulfamethoxazole 400 MG / trimethoprim 80 MG Oral Tablet. Action: NEW. Urgency: ROUTINE
[114] (2025-07-10) Drug order: Sulfamethoxazole 400 MG / trimethoprim 80 MG Oral Tablet. Action: NEW. Urgency: ROUTINE
[147] (2025-01-12) Drug order: Sulfamethoxazole 400 MG / trimethoprim 80 MG Oral Tablet. Action: NEW. Urgency: ROUTINE
"""
    facts = build_temporal_facts(chart, "2026-06-20")
    draft = (
        "The following medications were ordered in the past 6 months:\n"
        "- **Sulfamethoxazole / Trimethoprim**: Ordered on multiple dates [2, 34, 37, 72, 114, 147].\n"
        "- **Abacavir / Lamivudine / Dolutegravir**: Ordered on 2026-06-15 [5]."
    )
    text, cites, _b, findings = answer_rules.apply_answer_rules(
        "What medications were ordered in the past 6 months?",
        draft,
        [2, 5, 34, 37, 72, 114, 147],
        [],
        facts,
    )
    assert "Sulfamethoxazole / Trimethoprim" in text
    assert "[2][34][37]" in text
    assert "[72]" not in text and "[114]" not in text and "[147]" not in text
    assert sorted(cites) == [2, 5, 34, 37]
    assert _ids(findings, "info") == ["rule_window"]
    assert not _ids(findings, "fail")

    conjunctive_draft = (
        "Medications ordered in the past 6 months: "
        "Sulfamethoxazole / Trimethoprim [2], [34], [37], and [72]; "
        "Abacavir / Lamivudine / Dolutegravir [5]."
    )
    c_text, c_cites, _cb, c_findings = answer_rules.apply_answer_rules(
        "What medications were ordered in the past 6 months?",
        conjunctive_draft,
        [2, 5, 34, 37, 72],
        [],
        facts,
    )
    assert "Sulfamethoxazole / Trimethoprim [2][34][37];" in c_text
    assert "and [72]" not in c_text
    assert sorted(c_cites) == [2, 5, 34, 37]
    assert _ids(c_findings, "info") == ["rule_window"]


