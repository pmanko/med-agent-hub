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


# ---- build_temporal_block ---------------------------------------------------


def test_block_carries_anchor_and_correct_recency_and_trend():
    block = temporal.build_temporal_block(_CHART, "2006-05-18")
    # the explicit reference-date anchor line
    assert "2006-05-18" in block
    assert "recent" in block.lower()  # tells the model how to read "now"/"most recent"
    # haemoglobin most-recent is 3.9 (2006-04-24), NOT the older 9.1 (the ordering bug P1 fixes)
    hgb_line = next(l for l in block.splitlines() if "Haemoglobin" in l)
    assert "3.9" in hgb_line
    assert hgb_line.index("3.9") < (
        hgb_line.index("9.1") if "9.1" in hgb_line else len(hgb_line)
    )
    # weight is a real downward trend 52 -> 41
    wt_line = next(l for l in block.splitlines() if "Weight" in l)
    assert "52" in wt_line and "41.0" in wt_line


def test_block_single_point_makes_no_trend_claim():
    block = temporal.build_temporal_block(_CHART, "2006-05-18")
    cd4_line = next(l for l in block.splitlines() if "CD4 count" in l)
    # one CD4 point -> report the value, never a trend/direction (the <2-points guard)
    assert "72" in cd4_line
    assert (
        "↑" not in cd4_line and "↓" not in cd4_line and "trend" not in cd4_line.lower()
    )


def test_block_empty_without_anchor_or_series():
    assert temporal.build_temporal_block("no dates", None) == ""


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


def test_block_states_most_recent_record_date_for_last_visit():
    # "When was the last visit?" needs the most-recent RECORD date stated explicitly, so the model
    # REPORTS it instead of fabricating (the am-last-visit failure). Distinct from the anchor/now.
    block = temporal.build_temporal_block(
        _CHART, "2006-06-01"
    )  # anchor (now) != latest record
    line = next(
        (l for l in block.splitlines() if l.startswith("Most recent clinical visit")),
        None,
    )
    assert (
        line is not None and "2006-05-18" in line
    )  # the chart's max CLINICAL date = the last visit


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
    block = temporal.build_temporal_block(_PROGRAM_CONFOUND, anchor="2026-06-20")
    visit_line = next(
        (l for l in block.splitlines() if l.startswith("Most recent clinical visit")),
        None,
    )
    assert visit_line is not None, block
    assert (
        "2026-01-07" in visit_line and "2026-05-20" not in visit_line
    )  # the visit, not the enrollment
    admin_line = next(
        (l for l in block.splitlines() if "Administrative record" in l), None
    )
    assert (
        admin_line is not None
        and "2026-05-20" in admin_line
        and "Tuberculosis" in admin_line
    )


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
    block = temporal.build_temporal_block(_SPARSE_ACTIVITY_ONLY, "2026-06-20")
    assert "No explicit visit/encounter record is present" in block
    assert "Do NOT report this activity date as a visit date" in block


# ---- temporal_facts.v1.2 sidecar -------------------------------------------


def test_temporal_facts_captures_return_visit_dates_and_classifies_against_anchor():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    assert facts["schema_version"] == "temporal_facts.v1.2"
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
    assert "temporal_facts.v1.2" in rendered
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
        "schema_version": "temporal_facts.v1.1",
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
