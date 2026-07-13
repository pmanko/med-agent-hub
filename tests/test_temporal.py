"""Deterministic temporal grounding (P0 anchor + P1 series), computed server-side from the chart
text so the synth REPORTS dated facts instead of deriving them. Pure-function unit tests — no LLM.

Grounded in the real Aloice chart format (chartsearchai PatientChartSerializer):
  [N] (YYYY-MM-DD) <Class> — <concept>: <value> <unit>
Run: pytest tests/test_temporal.py
"""

import json

import pytest

from server import temporal

# A realistic slice of the serialized chart (most-recent-first, like the real snapshot).
_CHART = """Patient records (most recent first):
Patient: 41-year-old Male

[15] (2006-05-18) Finding — Weight (kg), WT): 41.0 kg
[47] (2006-05-11) Finding — Weight (kg), WT): 42.0 kg
[79] (2006-04-26) Finding — Weight (kg), WT): 48.0 kg
[97] (2006-04-24) Test — Haemoglobin: 3.9 g/dL
[186] (2006-03-06) Test — Haemoglobin: 9.1 g/dL
[202] (2006-03-03) Finding — Weight (kg), WT): 52.0 kg
[218] (2006-03-02) Test — CD4 count: 72.0 cells/uL
[26] (2006-05-18) Drug order: Lamivudine / zidovudine. Action: NEW. Urgency: ROUTINE
[1] (2006-05-18) Assessment — Scheduled visit: No
[5] (2006-05-11) Assessment — Return visit date: 2006-05-18
[300] (2006-05-18) Encounter: Adult Visit at Clinic A
"""


# ---- resolve_anchor ---------------------------------------------------------


def test_resolve_anchor_latest_record_picks_max_date():
    assert temporal.resolve_anchor("latest_record", _CHART) == "2006-05-18"
    assert (
        temporal.resolve_anchor(None, _CHART) == "2006-05-18"
    )  # default = latest_record


def test_resolve_anchor_wall_clock_uses_configured_deployment_timezone(monkeypatch):
    instant = temporal._dt.datetime(
        2026, 7, 11, 3, 0, tzinfo=temporal._dt.timezone.utc
    )

    class FixedDateTime:
        @classmethod
        def now(cls, timezone=None):
            return instant.astimezone(timezone)

    monkeypatch.setattr(temporal._dt, "datetime", FixedDateTime)

    assert (
        temporal.resolve_anchor(
            "wall_clock", _CHART, timezone_name="Pacific/Honolulu"
        )
        == "2026-07-10"
    )


def test_resolve_anchor_explicit_date_passthrough():
    assert temporal.resolve_anchor("2006-06-01", _CHART) == "2006-06-01"


def test_resolve_anchor_latest_record_no_dates_is_none():
    assert temporal.resolve_anchor("latest_record", "no dates here") is None


# ---- parse_dated_observations ----------------------------------------------


def test_parse_extracts_numeric_obs_only():
    obs = temporal.parse_dated_observations(_CHART)
    concepts = {o["concept"] for o in obs}
    # numeric series present; non-numeric (drug order, Yes/No assessment) excluded
    assert (
        "Weight" in concepts and "Haemoglobin" in concepts and "CD4 count" in concepts
    )
    assert not any("Scheduled visit" in c or "Lamivudine" in c for c in concepts)
    assert (
        "Return visit date" not in concepts
    )  # date-valued obs is not a numeric series
    weights = sorted(
        (o for o in obs if o["concept"] == "Weight"), key=lambda o: o["date"]
    )
    assert [w["value"] for w in weights] == [
        52.0,
        48.0,
        42.0,
        41.0,
    ]  # sorted ascending, deduped per date
    assert weights[0]["unit"] == "kg"


# ---- active temporal facts and renderer ------------------------------------


def test_facts_carry_anchor_and_correct_recency_and_trend():
    facts = temporal.build_temporal_facts(_CHART, "2006-05-18")
    by_concept = {item["concept"]: item for item in facts["numeric_series"]}

    assert facts["reference_date"] == "2006-05-18"
    assert by_concept["Haemoglobin"]["most_recent"]["value"] == 3.9
    assert [point["value"] for point in by_concept["Haemoglobin"]["points"]] == [
        9.1,
        3.9,
    ]
    assert by_concept["Weight"]["direction"] == "down"
    assert by_concept["Weight"]["points"][0]["value"] == 52.0
    assert by_concept["Weight"]["most_recent"]["value"] == 41.0
    assert "2006-05-18" in temporal.render_temporal_facts(facts)


def test_facts_single_point_make_no_trend_claim():
    facts = temporal.build_temporal_facts(_CHART, "2006-05-18")
    cd4 = next(item for item in facts["numeric_series"] if item["concept"] == "CD4 count")

    assert cd4["most_recent"]["value"] == 72.0
    assert cd4["n_points"] == 1
    assert cd4["trend_supported"] is False
    assert cd4["direction"] == "none"


# ---- run-length compression (real Ellcky chart: only 8/276 lines keep the date) -------------

# A real fragment of Ellcky's heavily-compressed chart: the run leader [1] carries (2026-02-02);
# the numeric follow-ons [19]/[20]/[21]/[24] DROP it (same-date run-length compression). temporal.py
# must carry the run date forward, or it silently loses every compressed measurement — the Ellcky
# temporal failures. (Aloice, by contrast, is date-per-line, so its parser already works.)
_COMPRESSED = """Patient records (most recent first):
[1] (2026-02-02) Condition: Acute coryza. Status: ACTIVE. Onset: 2026-02-02
[19] Body surface area: 45.5
[20] Height (cm): 67 cm
[21] Pulse: 147 beats/min
[24] Weight (kg): 6.2 kg
"""


def test_parse_carries_date_forward_across_run_length_compression():
    obs = temporal.parse_dated_observations(_COMPRESSED)
    by = {(o["concept"], o["date"]): o["value"] for o in obs}
    # the dateless follow-ons inherit the run-leader's 2026-02-02 — DROPPED on today's regex (RED)
    assert by.get(("Weight", "2026-02-02")) == 6.2
    assert by.get(("Height", "2026-02-02")) == 67.0
    assert by.get(("Pulse", "2026-02-02")) == 147.0
    assert by.get(("Body surface area", "2026-02-02")) == 45.5


def test_facts_state_most_recent_record_date_for_last_visit():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    assert facts["reference_date"] == "2006-06-01"
    assert facts["last_clinical_encounter"]["date"] == "2006-05-18"


# ---- typed event timeline: an administrative record (e.g. a Program enrollment) must NOT be reported
# as "the last visit" even when its date is the chart's max. The real am-last-visit failure: Aloice's
# TB Program enrollment 2026-05-20 post-dates the last clinical visit 2026-01-07, and max(all dates)
# picked the enrollment. The timeline TYPES events so a Program is labeled administrative, not a visit. --

_PROGRAM_CONFOUND = """Patient records (most recent first):
[1] (2026-05-20) Program: Tuberculosis treatment program. Status: Active. Current state: GROUP TB
[2] (2026-01-07) Assessment — Scheduled visit: No
[3] (2026-01-07) Finding — Weight (kg): 41.0 kg
[5] (2026-01-07) Encounter: Adult Visit at Clinic A
[4] (2025-12-31) Finding — Weight (kg): 42.0 kg
"""

_SPARSE_ACTIVITY_ONLY = """Patient records (most recent first):
[1] (2026-05-20) Program: Tuberculosis treatment program. Status: Active
[2] (2026-01-07) Assessment — Scheduled visit: No
[3] Finding — Weight (kg): 41.0 kg
"""

_WEIGHT_WITH_HOSPITALIZATION = """Patient records (most recent first):
[1] (2026-01-07) Finding — Weight (kg): 41.0 kg
[2] (2025-12-31) Finding — Weight (kg): 42.0 kg
[3] (2025-10-22) Finding — Number of hospitalizations in past year: 27 # hospitalizations
"""

_EXPLICIT_ENCOUNTER_WITH_SAME_DAY_RECORDS = """Patient records (most recent first):
[1] (2026-06-06) Drug order: Nevirapine. Action: NEW
[2] Encounter: Adult Visit at Unknown Location. Provider: Horatio L Hornblower
[3] Finding — Weight (kg): 71.0 kg
[4] Assessment — Scheduled visit: No
[5] (2026-05-20) Encounter: Follow-up Visit at Clinic A. Provider: Jane Doe
"""

_EXPLICIT_ENCOUNTER_BEFORE_LATER_OBSERVATION = """Patient records (most recent first):
[1] (2026-06-10) Finding — Weight (kg): 72.0 kg
[2] (2026-06-06) Encounter: Adult Visit at Unknown Location. Provider: Horatio L Hornblower
"""


def test_event_timeline_excludes_program_enrollment_from_last_visit():
    facts = temporal.build_temporal_facts(_PROGRAM_CONFOUND, anchor="2026-06-20")

    assert facts["last_clinical_encounter"]["date"] == "2026-01-07"
    assert facts["admin_dates"][0]["date"] == "2026-05-20"


def test_resolve_anchor_latest_record_ignores_program_enrollment():
    # the default "now" = the last CLINICAL record date, not a post-dated administrative enrollment
    assert temporal.resolve_anchor("latest_record", _PROGRAM_CONFOUND) == "2026-01-07"


def test_last_clinical_encounter_prefers_explicit_encounter_records_only():
    facts = temporal.build_temporal_facts(
        _EXPLICIT_ENCOUNTER_WITH_SAME_DAY_RECORDS, "2026-06-20"
    )

    assert facts["last_clinical_encounter"]["date"] == "2026-06-06"
    assert facts["last_clinical_encounter"]["indices"] == [2]
    assert facts["last_clinical_encounter"]["classes"] == ["Encounter"]
    assert facts["last_clinical_encounter"]["summaries"] == [
        "Encounter: Adult Visit at Unknown Location. Provider: Horatio L Hornblower"
    ]


def test_last_clinical_encounter_ignores_later_non_encounter_record():
    facts = temporal.build_temporal_facts(
        _EXPLICIT_ENCOUNTER_BEFORE_LATER_OBSERVATION, "2026-06-20"
    )

    assert facts["last_clinical_encounter"]["date"] == "2026-06-06"
    assert facts["last_clinical_encounter"]["indices"] == [2]


def test_activity_only_chart_does_not_fabricate_a_clinical_encounter():
    facts = temporal.build_temporal_facts(_SPARSE_ACTIVITY_ONLY, "2026-06-20")

    assert facts["last_clinical_encounter"] is None
    assert facts["latest_clinical_activity"]["date"] == "2026-01-07"
    assert facts["latest_clinical_activity"]["indices"] == [2, 3]
    rendered = temporal.render_temporal_facts(facts)
    assert '"last_clinical_encounter":null' in rendered
    assert '"latest_clinical_activity"' in rendered


def test_last_visit_claim_fails_when_only_clinical_activity_is_documented():
    facts = temporal.build_temporal_facts(_SPARSE_ACTIVITY_ONLY, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?",
        "The last visit was 2026-01-07 [2].",
        [2],
        facts,
        "enforce",
    )

    assert result["status"] == "fail"
    assert result["patch_answer"].startswith(
        "No explicit visit/encounter record is documented"
    )
    assert "latest dated clinical activity is 2026-01-07 [2]" in result["patch_answer"]
    assert result["patch_citations"] == [2]


def test_last_visit_abstention_may_report_separate_latest_activity():
    facts = temporal.build_temporal_facts(_SPARSE_ACTIVITY_ONLY, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?",
        "No explicit visit is documented. The latest clinical activity was 2026-01-07 [2].",
        [2],
        facts,
        "enforce",
    )

    assert not any(check["id"] == "last_visit" for check in result["checks"])


@pytest.mark.parametrize(
    "answer",
    (
        "No explicit visit is documented, but the last visit was 2026-01-07 [2].",
        "There is no encounter record. The last visit occurred on 2026-01-07 [2].",
        "Although no explicit visit is documented, the last visit was 2026-01-07 [2].",
        "While no encounter is documented, the last visit occurred on 2026-01-07 [2].",
        "No explicit visit is documented, yet the last visit was 2026-01-07 [2].",
        "No explicit visit is documented — the last visit was 2026-01-07 [2].",
        "On 2026-01-07 [2], the last visit occurred.",
        "2026-01-07 [2] was the date of the last encounter.",
        "The date 2026-01-07 [2] corresponds to the last visit.",
        "Clinical activity was reviewed, but the last visit was 2026-01-07 [2].",
        "The latest clinical activity is unclear; however, the last encounter was 2026-01-07 [2].",
        "No visit is documented. Clinical activity exists, yet the last visit occurred on 2026-01-07 [2].",
    ),
)
def test_last_visit_disclaimer_cannot_mask_a_contradictory_assertion(answer):
    facts = temporal.build_temporal_facts(_SPARSE_ACTIVITY_ONLY, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?", answer, [2], facts, "enforce"
    )

    assert result["status"] == "fail"
    assert any(check["id"] == "last_visit" for check in result["checks"])


@pytest.mark.parametrize(
    "answer",
    (
        "2026-01-07 [2] was not the date of the last encounter.",
        "On 2026-01-07 [2], no last visit is documented.",
        "The last visit is not documented as of 2026-01-07 [2].",
    ),
)
def test_negated_date_first_visit_claim_is_not_treated_as_an_assertion(answer):
    facts = temporal.build_temporal_facts(_SPARSE_ACTIVITY_ONLY, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?", answer, [2], facts, "enforce"
    )

    assert not any(check["id"] == "last_visit" for check in result["checks"])


@pytest.mark.parametrize(
    "answer",
    (
        "The last visit was 2025-12-31 [4] at an unknown location.",
        "The last encounter on 2025-12-31 [4] had an unknown provider.",
        "The last visit was 2025-12-31 [4] and the diagnosis is not available.",
    ),
)
def test_unknown_visit_attributes_do_not_mask_a_wrong_visit_date(answer):
    facts = temporal.build_temporal_facts(_PROGRAM_CONFOUND, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?", answer, [4], facts, "enforce"
    )

    assert result["status"] == "fail"
    assert any(check["id"] == "last_visit" for check in result["checks"])


def test_unknown_location_does_not_block_the_correct_visit_date():
    facts = temporal.build_temporal_facts(_PROGRAM_CONFOUND, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?",
        "The last visit was 2026-01-07 [5] at Unknown Location.",
        [5],
        facts,
        "enforce",
    )

    assert not any(check["id"] == "last_visit" for check in result["checks"])


def test_series_selection_prefers_specific_blood_pressure_concepts():
    facts = {
        "numeric_series": [
            {"concept": "White blood cells"},
            {"concept": "Diastolic blood pressure"},
            {"concept": "Systolic blood pressure"},
        ]
    }

    selected = temporal._selected_series(
        "What was the latest visit?",
        "The patient's blood pressure readings show a downward trend.",
        facts,
    )

    assert {item["concept"] for item in selected} == {
        "Diastolic blood pressure",
        "Systolic blood pressure",
    }


def test_series_selection_uses_exact_tokens_not_substrings():
    facts = {
        "numeric_series": [
            {"concept": "Weight"},
            {"concept": "Pulse"},
            {"concept": "CD4 count"},
        ]
    }

    assert temporal._selected_series(
        "Review care", "The overweight impulse was labeled XCD4Y.", facts
    ) == []


def test_series_selection_keeps_independently_named_measurements():
    facts = {
        "numeric_series": [
            {"concept": "White blood cells"},
            {"concept": "Diastolic blood pressure"},
            {"concept": "Pulse"},
        ]
    }

    selected = temporal._selected_series(
        "Review trends",
        "Diastolic blood pressure decreased while pulse decreased.",
        facts,
    )

    assert {item["concept"] for item in selected} == {
        "Diastolic blood pressure",
        "Pulse",
    }


def test_trend_direction_is_checked_per_named_measurement():
    facts = {
        "date_ledger": [],
        "date_output_contract": {},
        "appointment_candidates": {},
        "numeric_series": [
            {
                "concept": "Diastolic blood pressure",
                "points": [{"index": 1}, {"index": 2}],
                "trend_supported": True,
                "direction": "down",
            },
            {
                "concept": "Pulse",
                "points": [{"index": 3}, {"index": 4}],
                "trend_supported": True,
                "direction": "up",
            },
        ],
    }

    result = temporal.run_temporal_gate(
        "Review both trends",
        "Diastolic blood pressure increased, while pulse decreased.",
        [1, 2, 3, 4],
        facts,
        "enforce",
    )

    contradictions = [
        check for check in result["checks"] if check["id"] == "trend_direction"
    ]
    assert len(contradictions) == 2
    assert any("Diastolic blood pressure" in check["reason"] for check in contradictions)
    assert any("Pulse" in check["reason"] for check in contradictions)


def test_shared_trend_predicate_applies_to_coordinated_measurements():
    facts = {
        "date_ledger": [],
        "date_output_contract": {},
        "appointment_candidates": {},
        "numeric_series": [
            {
                "concept": "Diastolic blood pressure",
                "points": [{"index": 1}, {"index": 2}],
                "trend_supported": True,
                "direction": "down",
            },
            {
                "concept": "Pulse",
                "points": [{"index": 3}, {"index": 4}],
                "trend_supported": True,
                "direction": "up",
            },
        ],
    }

    result = temporal.run_temporal_gate(
        "Review both trends",
        "Diastolic blood pressure and pulse increased.",
        [1, 2, 3, 4],
        facts,
        "enforce",
    )

    contradictions = [
        check for check in result["checks"] if check["id"] == "trend_direction"
    ]
    assert len(contradictions) == 1
    assert "Diastolic blood pressure" in contradictions[0]["reason"]


def test_shared_trend_predicate_supports_oxford_comma_lists():
    facts = {
        "date_ledger": [],
        "date_output_contract": {},
        "appointment_candidates": {},
        "numeric_series": [
            {
                "concept": "Diastolic blood pressure",
                "points": [{"index": 1}, {"index": 2}],
                "trend_supported": True,
                "direction": "down",
            },
            {
                "concept": "Systolic blood pressure",
                "points": [{"index": 3}, {"index": 4}],
                "trend_supported": True,
                "direction": "down",
            },
            {
                "concept": "Pulse",
                "points": [{"index": 5}, {"index": 6}],
                "trend_supported": True,
                "direction": "up",
            },
        ],
    }

    result = temporal.run_temporal_gate(
        "Review all trends",
        "Diastolic blood pressure, systolic blood pressure, and pulse increased.",
        [1, 2, 3, 4, 5, 6],
        facts,
        "enforce",
    )

    contradictions = [
        check for check in result["checks"] if check["id"] == "trend_direction"
    ]
    assert len(contradictions) == 2
    assert {"Diastolic blood pressure", "Systolic blood pressure"} == {
        concept
        for concept in ("Diastolic blood pressure", "Systolic blood pressure")
        if any(concept in check["reason"] for check in contradictions)
    }


def test_series_selection_keeps_overlapping_exact_cd4_concepts():
    facts = {
        "numeric_series": [
            {"concept": "CD4 count"},
            {"concept": "CD4%"},
        ]
    }

    selected = temporal._selected_series(
        "Review trends",
        "The CD4 count decreased while CD4% increased.",
        facts,
    )

    assert {item["concept"] for item in selected} == {"CD4 count", "CD4%"}

    percent_only = temporal._selected_series(
        "Review trends", "The CD4% increased.", facts
    )
    assert [item["concept"] for item in percent_only] == ["CD4%"]

    count_only = temporal._selected_series(
        "Review trends", "The CD4 count decreased.", facts
    )
    assert [item["concept"] for item in count_only] == ["CD4 count"]


@pytest.mark.parametrize(
    "answer",
    (
        "The last encounter did not include labs and occurred on 2025-12-31 [4].",
        "The last visit was not routine; it occurred on 2025-12-31 [4].",
        "The last visit did not document a diagnosis and was on 2025-12-31 [4].",
        "The last visit was not at Clinic A but on 2025-12-31 [4] at Clinic B.",
    ),
)
def test_unrelated_negation_does_not_mask_a_wrong_visit_date(answer):
    facts = temporal.build_temporal_facts(_PROGRAM_CONFOUND, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?", answer, [4], facts, "enforce"
    )

    assert result["status"] == "fail"
    assert any(check["id"] == "last_visit" for check in result["checks"])


@pytest.mark.parametrize(
    "answer",
    (
        "The last visit was not on 2025-12-31 [4].",
        "The last visit did not occur on 2025-12-31 [4].",
    ),
)
def test_direct_visit_date_negation_is_not_an_assertion(answer):
    facts = temporal.build_temporal_facts(_PROGRAM_CONFOUND, "2026-06-20")

    result = temporal.run_temporal_gate(
        "When was the last visit?", answer, [4], facts, "enforce"
    )

    assert not any(check["id"] == "last_visit" for check in result["checks"])


# ---- temporal facts sidecar ------------------------------------------------


def test_temporal_facts_captures_return_visit_dates_and_classifies_against_anchor():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    assert "schema_version" not in facts
    assert facts["reference_date"] == "2006-06-01"
    assert facts["reference_date_id"] == "D2006_06_01"
    assert facts["last_clinical_encounter"]["date"] == "2006-05-18"
    assert facts["last_clinical_encounter"]["date_id"] == "D2006_05_18"

    ledger = {d["iso"]: d for d in facts["date_ledger"]}
    assert ledger["2006-05-18"]["date_id"] == "D2006_05_18"
    assert ledger["2006-05-18"]["year"] == 2006
    assert ledger["2006-05-18"]["month"] == 5
    assert ledger["2006-05-18"]["month_name"] == "May"
    assert ledger["2006-05-18"]["day"] == 18
    assert "2005-12-01" in ledger  # deterministic past-6-months window boundary

    rv = facts["return_visit_dates"]
    assert rv and rv[0]["concept"] == "Return visit date"
    assert rv[0]["value_date"] == "2006-05-18"
    assert rv[0]["value_date_id"] == "D2006_05_18"
    assert rv[0]["relation_to_reference"] == "past"

    appt = facts["appointment_candidates"]
    assert appt["past"][0]["date"] == "2006-05-18"
    assert appt["past"][0]["date_id"] == "D2006_05_18"
    assert appt["future"] == []

    wt = next(s for s in facts["numeric_series"] if s["concept"] == "Weight")
    assert wt["points"][-1]["effective_date_id"] == "D2006_05_18"
    assert wt["window"]["start_date_id"] == "D2006_03_03"


def _rendered_json_payload(rendered: str) -> dict:
    start = rendered.index("```json\n") + len("```json\n")
    end = rendered.index("\n```", start)
    return json.loads(rendered[start:end])


def test_render_temporal_facts_includes_full_json_sidecar_marker():
    facts = temporal.build_temporal_facts(_CHART, "2006-05-18")
    rendered = temporal.render_temporal_facts(facts)
    assert "Structured temporal facts" in rendered
    assert "temporal_facts.v" not in rendered
    assert '"date_ledger"' in rendered
    assert '"return_visit_dates"' in rendered
    assert '"numeric_series"' in rendered


def test_render_temporal_facts_default_profile_is_the_uncompacted_sidecar_verbatim():
    # Gate 8: temporal prompt content is a config knob, not an unconditional compaction — the
    # DEFAULT profile ("full") must ship the exact facts dict, byte-identical to a raw dump, so
    # every research/batch arm keeps seeing what it saw before H2. Concretely: each
    # clinical_dates entry keeps its full "classes" list (compact drops it entirely).
    facts = temporal.build_temporal_facts(_CHART, "2006-05-18")
    rendered = temporal.render_temporal_facts(facts)
    payload = _rendered_json_payload(rendered)
    assert payload == facts
    assert "classes" in facts["clinical_dates"][0]
    assert "classes" in payload["clinical_dates"][0]


def test_render_temporal_facts_compact_profile_is_explicit_opt_in():
    facts = temporal.build_temporal_facts(_CHART, "2006-05-18")
    rendered = temporal.render_temporal_facts(facts, profile="compact")
    payload = _rendered_json_payload(rendered)
    assert payload == temporal.compact_temporal_facts_for_prompt(facts)
    assert "classes" not in payload["clinical_dates"][0]
    assert payload != facts


# ---- temporal gate ----------------------------------------------------------


def test_gate_off_is_noop_metadata_only():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        "The next appointment is 2006-05-18 [5].",
        [5],
        facts,
        "off",
    )
    assert gate["mode"] == "off"
    assert gate["status"] == "not_applicable"
    assert gate["checks"] == []


def test_gate_fails_past_return_visit_called_upcoming_and_offers_patch():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        "The next upcoming appointment is 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )
    assert gate["status"] == "fail"
    assert any(
        c["id"] == "upcoming_date" and c["status"] == "fail" for c in gate["checks"]
    )
    assert (
        "No upcoming appointment is documented after 2006-06-01" in gate["patch_answer"]
    )
    assert gate["patch_citations"] == [5]


def test_historical_return_visit_date_is_not_treated_as_last_clinical_visit():
    chart = """Patient records (most recent first):
[4] (2026-01-26) Encounter: Adult Visit at Clinic A
[22] Return visit date: 2026-02-23
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        (
            "No upcoming appointment is documented after 2026-06-20. The latest return-visit "
            "date found is 2026-02-23, which is before the reference date [22]."
        ),
        [22],
        facts,
        "enforce",
    )

    assert not any(check["id"] == "last_visit" for check in gate["checks"])
    assert gate["patch_answer"] is None


def test_no_upcoming_pass_carries_all_candidate_source_indices():
    chart = """Patient records (most recent first):
[4] (2026-01-26) Encounter: Adult Visit at Clinic A
[22] Return visit date: 2026-02-23
[55] Return visit date: 2026-01-14
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        (
            "The record does not show any upcoming appointments; all listed return visit dates "
            "are in the past [22][55]."
        ),
        [22, 55],
        facts,
        "enforce",
    )

    check = next(
        check
        for check in gate["checks"]
        if check["id"] == "upcoming_date" and check["status"] == "pass"
    )
    assert check["source_indices"] == [22, 55]
    assert check["claim"] == (
        "The record does not show any upcoming appointments; all listed return visit dates "
        "are in the past [22][55]."
    )


def test_no_upcoming_pass_accepts_strict_scheduled_return_visit_summary():
    chart = """Patient records (most recent first):
[4] (2026-01-26) Encounter: Adult Visit at Clinic A
[22] Return visit date: 2026-02-23
[55] Return visit date: 2026-01-14
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")
    answer = (
        "The record does not show any upcoming appointments; all scheduled return visits "
        "are in the past."
    )

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [22, 55],
        facts,
        "enforce",
    )

    check = next(
        check
        for check in gate["checks"]
        if check["id"] == "upcoming_date" and check["status"] == "pass"
    )
    assert check["source_indices"] == [22, 55]
    assert check["claim"] == answer


def test_no_upcoming_pass_claim_excludes_an_unrelated_coordinated_claim():
    chart = """Patient records (most recent first):
[22] Return visit date: 2026-02-23
[55] Return visit date: 2026-01-14
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")
    answer = (
        "No upcoming appointments are documented [22][55], and the patient has diabetes "
        "[22][55]."
    )

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [22, 55],
        facts,
        "enforce",
    )

    check = next(
        check
        for check in gate["checks"]
        if check["id"] == "upcoming_date" and check["status"] == "pass"
    )
    assert "diabetes" not in check["claim"]
    assert check["claim"] in answer


def test_scheduled_no_upcoming_pass_excludes_an_unrelated_semicolon_claim():
    chart = """Patient records (most recent first):
[22] Return visit date: 2026-02-23
[55] Return visit date: 2026-01-14
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")
    answer = (
        "The record does not show any upcoming appointments; all scheduled return visits "
        "are in the past; the patient has diabetes."
    )

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [22, 55],
        facts,
        "enforce",
    )

    check = next(
        check
        for check in gate["checks"]
        if check["id"] == "upcoming_date" and check["status"] == "pass"
    )
    assert "diabetes" not in check["claim"]


def test_no_upcoming_pass_claim_excludes_unrelated_claim_with_visit_word():
    chart = """Patient records (most recent first):
[22] Return visit date: 2026-02-23
[55] Return visit date: 2026-01-14
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")
    answer = (
        "No upcoming appointments are documented [22][55], and diabetes was diagnosed at the "
        "last visit [22][55]."
    )

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [22, 55],
        facts,
        "enforce",
    )

    check = next(
        check
        for check in gate["checks"]
        if check["id"] == "upcoming_date" and check["status"] == "pass"
    )
    assert "diabetes" not in check["claim"]


def test_no_upcoming_pass_claim_excludes_unrelated_subordinate_claim():
    chart = """Patient records (most recent first):
[22] Return visit date: 2026-02-23
[55] Return visit date: 2026-01-14
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")
    answer = (
        "No upcoming appointments are documented because diabetes was diagnosed at the last "
        "visit [22][55]."
    )

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [22, 55],
        facts,
        "enforce",
    )

    check = next(
        check
        for check in gate["checks"]
        if check["id"] == "upcoming_date" and check["status"] == "pass"
    )
    assert "diabetes" not in check["claim"]


def test_return_visit_short_answer_does_not_use_last_encounter_fallback():
    chart = """Patient records (most recent first):
[4] (2026-01-26) Encounter: Adult Visit at Clinic A
[22] Return visit date: 2026-02-23
"""
    facts = temporal.build_temporal_facts(chart, "2026-06-20")

    gate = temporal.run_temporal_gate(
        "When was the last visit?",
        "Latest return-visit date: 2026-02-23 [22].",
        [22],
        facts,
        "enforce",
    )

    assert not any(check["id"] == "last_visit" for check in gate["checks"])
    assert gate["patch_answer"] is None


def test_previous_anticipated_follow_up_date_is_not_framed_as_future():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        "A previous anticipated follow-up date was 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )

    assert not any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )
    assert gate["patch_answer"] is None


def test_historical_date_near_next_visit_instruction_is_not_framed_as_future():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "What should be reviewed?",
        "At the next visit, compare the result from 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )

    assert not any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )
    assert gate["patch_answer"] is None


def test_past_date_scheduled_as_appointment_is_framed_as_future():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        "The appointment is scheduled for 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )

    assert any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )


def test_scheduled_return_visit_on_past_date_is_framed_as_future():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        "The patient has a scheduled return visit on 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )

    assert any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )


def test_historical_scheduled_return_visit_is_not_framed_as_future():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "What appointment history is documented?",
        "The patient had a scheduled return visit on 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )

    assert not any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )


@pytest.mark.parametrize(
    "answer",
    (
        "The next appointment date is 2006-05-18 [5].",
        "The upcoming return-visit date is 2006-05-18 [5].",
    ),
)
def test_explicit_future_event_date_phrase_is_framed_as_future(answer):
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [5],
        facts,
        "enforce",
    )

    assert any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )


def test_negated_clause_does_not_mask_later_future_claim_in_same_sentence():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        (
            "No future appointment is documented, but the next appointment is "
            "2006-05-18 [5]."
        ),
        [5],
        facts,
        "enforce",
    )

    assert any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )


@pytest.mark.parametrize(
    "answer",
    (
        "No upcoming appointment on 2006-05-18 [5] is documented.",
        "There is no next visit for 2006-05-18 [5].",
    ),
)
def test_directly_negated_future_phrase_does_not_fail(answer):
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        answer,
        [5],
        facts,
        "enforce",
    )

    assert not any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )


def test_previously_scheduled_past_appointment_is_not_framed_as_future():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")

    gate = temporal.run_temporal_gate(
        "What appointment history is documented?",
        "The appointment was previously scheduled for 2006-05-18 [5].",
        [5],
        facts,
        "enforce",
    )

    assert not any(
        check["id"] == "upcoming_date" and check["status"] == "fail"
        for check in gate["checks"]
    )
    assert gate["patch_answer"] is None


def test_gate_warn_mode_reports_but_does_not_patch_by_itself():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "Does this patient have any upcoming appointments?",
        "The next upcoming appointment is 2006-05-18 [5].",
        [5],
        facts,
        "warn",
    )
    assert gate["mode"] == "warn"
    assert gate["status"] == "fail"
    assert gate[
        "patch_answer"
    ]  # patch is advisory; callers apply it only in enforce mode


def test_gate_fails_wrong_last_visit_date():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "When was this patient's last visit?",
        "The last visit was 2006-05-11 [47].",
        [47],
        facts,
        "enforce",
    )
    assert gate["status"] == "fail"
    assert any(c["id"] == "last_visit" for c in gate["checks"])
    assert "2006-05-18" in gate["patch_answer"]


def test_gate_fails_single_point_trend_claim():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "What is the CD4 trend?",
        "The CD4 count is decreasing over time, most recently 72 cells/uL [218].",
        [218],
        facts,
        "enforce",
    )
    assert gate["status"] == "fail"
    assert any(c["id"] == "single_point_trend" for c in gate["checks"])
    assert "A trend cannot be determined from one point" in gate["patch_answer"]


def test_gate_fails_contradictory_weight_direction_and_wrong_date_value_binding():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The weight increased to 52 kg on 2006-05-18 [15].",
        [15],
        facts,
        "enforce",
    )
    assert gate["status"] == "fail"
    ids = {c["id"] for c in gate["checks"] if c["status"] == "fail"}
    assert {"trend_direction", "date_value_binding"} <= ids


def test_gate_fails_swapped_values_when_two_dates_share_one_sentence():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The weight decreased from 41 kg on 2006-03-03 to 52 kg on 2006-05-18 [15].",
        [15],
        facts,
        "enforce",
    )

    assert gate["status"] == "fail"
    assert any(c["id"] == "date_value_binding" for c in gate["checks"])


def test_gate_fails_invented_value_bound_to_a_ledger_date():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The weight was 53 kg on 2006-03-03 [1].",
        [1],
        facts,
        "enforce",
    )

    assert gate["status"] == "fail"
    assert any(c["id"] == "date_value_binding" for c in gate["checks"])


def test_gate_fails_series_value_on_date_without_that_measurement():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The weight was 53 kg on 2006-04-24 [97].",
        [97],
        facts,
        "enforce",
    )

    assert gate["status"] == "fail"
    assert any(c["id"] == "date_value_binding" for c in gate["checks"])


@pytest.mark.parametrize(
    "answer",
    [
        "The weight was 53 kg, with 52 kg recorded on 2006-03-03 [202].",
        "The weight ranged from 53 to 52 kg on 2006-03-03 [202].",
        "The weight ranged from 53-52 kg on 2006-03-03 [202].",
        "The weight ranged from 53–52 kg on 2006-03-03 [202].",
    ],
)
def test_gate_checks_every_weight_value_in_a_dated_sentence(answer):
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?", answer, [202], facts, "enforce"
    )

    assert gate["status"] == "fail"
    assert any(c["id"] == "date_value_binding" for c in gate["checks"])


@pytest.mark.parametrize(
    "answer",
    [
        "The weight was 52 kg (BMI 18) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg/m2) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg / m2) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg m-2) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg m−2) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg m^-2) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg·m−2) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg·m⁻²) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg⁄m²) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg∕m²) on 2006-03-03 [202].",
        "The weight was 52 kg (BMI 18 kg m²) on 2006-03-03 [202].",
        "Weight 52 kg and height 150 cm were recorded on 2006-03-03 [202].",
    ],
)
def test_gate_ignores_values_from_other_measurements(answer):
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "What was the patient's weight?", answer, [202], facts, "enforce"
    )

    assert not any(c["id"] == "date_value_binding" for c in gate["checks"])


def test_gate_preserves_ordinal_date_value_pairs_with_respectively():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The weights on 2006-03-03 and 2006-05-18 were 52 kg and 41 kg, respectively [1][15].",
        [1, 15],
        facts,
        "enforce",
    )

    assert not any(c["id"] == "date_value_binding" for c in gate["checks"])


def _paired_blood_pressure_facts():
    return {
        "reference_date": "2006-06-20",
        "clinical_dates": [],
        "admin_dates": [],
        "date_ledger": [{"iso": "2006-06-06", "date_id": "bp"}],
        "date_output_contract": {"allowed_iso_dates": ["2006-06-06"]},
        "appointment_candidates": {},
        "numeric_series": [
            {
                "concept": "Diastolic blood pressure",
                "points": [
                    {"date": "2006-06-06", "value": 60, "unit": "mmHg", "index": 15}
                ],
                "trend_supported": False,
                "direction": "none",
            },
            {
                "concept": "Systolic blood pressure",
                "points": [
                    {"date": "2006-06-06", "value": 110, "unit": "mmHg", "index": 17}
                ],
                "trend_supported": False,
                "direction": "none",
            },
        ],
    }


def test_gate_binds_same_unit_values_to_their_named_series():
    gate = temporal.run_temporal_gate(
        "What was the most recent blood pressure?",
        "The systolic blood pressure was 110 mmHg and the diastolic blood pressure was 60 mmHg on 2006-06-06 [17][15].",
        [17, 15],
        _paired_blood_pressure_facts(),
        "enforce",
    )

    assert not any(c["id"] == "date_value_binding" for c in gate["checks"])


def test_gate_rejects_same_unit_values_swapped_between_named_series():
    gate = temporal.run_temporal_gate(
        "What was the most recent blood pressure?",
        "The systolic blood pressure was 60 mmHg and the diastolic blood pressure was 110 mmHg on 2006-06-06 [17][15].",
        [17, 15],
        _paired_blood_pressure_facts(),
        "enforce",
    )

    assert gate["status"] == "fail"
    assert any(c["id"] == "date_value_binding" for c in gate["checks"])


@pytest.mark.parametrize(
    "answer",
    (
        "Systolic blood pressure and diastolic blood pressure were 110 mmHg and 60 mmHg, respectively, on 2006-06-06 [17][15].",
        "The systolic and diastolic blood pressures were 110 and 60 mmHg, respectively, on 2006-06-06 [17][15].",
    ),
)
def test_gate_binds_coordinated_same_unit_values_by_respective_order(answer):
    gate = temporal.run_temporal_gate(
        "What was the most recent blood pressure?",
        answer,
        [17, 15],
        _paired_blood_pressure_facts(),
        "enforce",
    )

    assert not any(c["id"] == "date_value_binding" for c in gate["checks"])


def test_gate_rejects_coordinated_same_unit_values_swapped_by_respective_order():
    gate = temporal.run_temporal_gate(
        "What was the most recent blood pressure?",
        "The systolic and diastolic blood pressures were 60 and 110 mmHg, respectively, on 2006-06-06 [17][15].",
        [17, 15],
        _paired_blood_pressure_facts(),
        "enforce",
    )

    assert gate["status"] == "fail"
    assert any(c["id"] == "date_value_binding" for c in gate["checks"])


@pytest.mark.parametrize("values", ("110 and 60", "60 and 110"))
def test_gate_leaves_generic_coordinated_multi_series_values_unbound(values):
    gate = temporal.run_temporal_gate(
        "What was the most recent blood pressure?",
        f"The blood pressure values were {values} mmHg, respectively, on 2006-06-06 [17][15].",
        [17, 15],
        _paired_blood_pressure_facts(),
        "enforce",
    )

    assert not any(c["id"] == "date_value_binding" for c in gate["checks"])


def test_gate_fails_malformed_date_output_and_can_patch_selected_series():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The weight decreased to 41 kg on 2006-05- 18 [15].",
        [15],
        facts,
        "enforce",
    )
    assert gate["status"] == "fail"
    assert any(c["id"] == "date_format" for c in gate["checks"])
    assert gate["patch_answer"].startswith("The documented Weight series decreased")


def test_gate_fails_pathological_malformed_date_strings():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    answer = "\n".join(
        [
            "The order date was written as 2025-10-//13.",
            "The follow-up date was written as 2026-0-[59].",
            "The month-only date was 2026-02.",
            "The unicode-hyphen date was 2006\u201105\u201118.",
        ]
    )
    gate = temporal.run_temporal_gate(
        "List any malformed dates.",
        answer,
        [],
        facts,
        "warn",
    )
    assert gate["status"] == "fail"
    claims = {c["claim"] for c in gate["checks"] if c["id"] == "date_format"}
    assert "2025-10-//13" in claims
    assert "2026-0-[59]" in claims
    assert "2026-02" in claims
    assert "2006\u201105\u201118" in claims


@pytest.mark.parametrize("answer", ["Seen on 2025-10-//13.", "Seen on 2026-01-09."])
def test_gate_rejects_malformed_and_nonledger_dates_when_ledger_is_empty(answer):
    facts = {
        "date_output_contract": {},
        "date_ledger": [],
        "numeric_series": [],
        "appointment_candidates": {},
    }

    result = temporal.run_temporal_gate("When?", answer, [], facts, "enforce")

    assert result["status"] == "fail"
    assert any(check["id"] == "date_format" for check in result["checks"])


def test_gate_fails_valid_iso_date_that_is_not_in_ledger():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "Were there orders in the past 6 months?",
        "There was a drug order on 3025-12-31 [26].",
        [26],
        facts,
        "warn",
    )
    assert gate["status"] == "fail"
    check = next(c for c in gate["checks"] if c["id"] == "date_format")
    assert "not present in date_ledger" in check["reason"]


def test_gate_fails_exposed_internal_date_id():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed?",
        "The first weight was on date D2006_03_03 and the most recent was 2006-05-18 [15].",
        [15],
        facts,
        "enforce",
    )
    assert gate["status"] == "fail"
    check = next(c for c in gate["checks"] if c["id"] == "date_format")
    assert "internal matching" in check["reason"]


def test_gate_does_not_select_hospitalizations_from_generic_past_year_words():
    facts = temporal.build_temporal_facts(_WEIGHT_WITH_HOSPITALIZATION, "2026-06-20")
    gate = temporal.run_temporal_gate(
        "How has this patient's weight changed over the past year?",
        "The documented weight decreased from 42 kg on 2025-12-31 [2] to 41 kg on 2026-01-07 [1].",
        [1, 2],
        facts,
        "warn",
    )
    failed_ids = {c["id"] for c in gate["checks"] if c["status"] == "fail"}
    assert "single_point_trend" not in failed_ids
    assert "date_value_binding" not in failed_ids
    assert all("hospitalizations" not in c["reason"] for c in gate["checks"])
