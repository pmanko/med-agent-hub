"""Deterministic temporal grounding (P0 anchor + P1 series), computed server-side from the chart
text so the synth REPORTS dated facts instead of deriving them. Pure-function unit tests — no LLM.

Grounded in the real Aloice chart format (chartsearchai PatientChartSerializer):
  [N] (YYYY-MM-DD) <Class> — <concept>: <value> <unit>
Run: pytest tests/test_temporal.py
"""

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
"""


# ---- resolve_anchor ---------------------------------------------------------

def test_resolve_anchor_latest_record_picks_max_date():
    assert temporal.resolve_anchor("latest_record", _CHART) == "2006-05-18"
    assert temporal.resolve_anchor(None, _CHART) == "2006-05-18"  # default = latest_record


def test_resolve_anchor_explicit_date_passthrough():
    assert temporal.resolve_anchor("2006-06-01", _CHART) == "2006-06-01"


def test_resolve_anchor_latest_record_no_dates_is_none():
    assert temporal.resolve_anchor("latest_record", "no dates here") is None


# ---- parse_dated_observations ----------------------------------------------

def test_parse_extracts_numeric_obs_only():
    obs = temporal.parse_dated_observations(_CHART)
    concepts = {o["concept"] for o in obs}
    # numeric series present; non-numeric (drug order, Yes/No assessment) excluded
    assert "Weight" in concepts and "Haemoglobin" in concepts and "CD4 count" in concepts
    assert not any("Scheduled visit" in c or "Lamivudine" in c for c in concepts)
    assert "Return visit date" not in concepts  # date-valued obs is not a numeric series
    weights = sorted((o for o in obs if o["concept"] == "Weight"), key=lambda o: o["date"])
    assert [w["value"] for w in weights] == [52.0, 48.0, 42.0, 41.0]  # sorted ascending, deduped per date
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
    assert hgb_line.index("3.9") < (hgb_line.index("9.1") if "9.1" in hgb_line else len(hgb_line))
    # weight is a real downward trend 52 -> 41
    wt_line = next(l for l in block.splitlines() if "Weight" in l)
    assert "52" in wt_line and "41.0" in wt_line


def test_block_single_point_makes_no_trend_claim():
    block = temporal.build_temporal_block(_CHART, "2006-05-18")
    cd4_line = next(l for l in block.splitlines() if "CD4 count" in l)
    # one CD4 point -> report the value, never a trend/direction (the <2-points guard)
    assert "72" in cd4_line
    assert "↑" not in cd4_line and "↓" not in cd4_line and "trend" not in cd4_line.lower()


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
    block = temporal.build_temporal_block(_CHART, "2006-06-01")  # anchor (now) != latest record
    line = next((l for l in block.splitlines() if l.startswith("Most recent clinical visit")), None)
    assert line is not None and "2006-05-18" in line  # the chart's max CLINICAL date = the last visit


# ---- typed event timeline: an administrative record (e.g. a Program enrollment) must NOT be reported
# as "the last visit" even when its date is the chart's max. The real am-last-visit failure: Aloice's
# TB Program enrollment 2026-05-20 post-dates the last clinical visit 2026-01-07, and max(all dates)
# picked the enrollment. The timeline TYPES events so a Program is labeled administrative, not a visit. --

_PROGRAM_CONFOUND = """Patient records (most recent first):
[1] (2026-05-20) Program: Tuberculosis treatment program. Status: Active. Current state: GROUP TB
[2] (2026-01-07) Assessment — Scheduled visit: No
[3] (2026-01-07) Finding — Weight (kg): 41.0 kg
[4] (2025-12-31) Finding — Weight (kg): 42.0 kg
"""

_WEIGHT_WITH_HOSPITALIZATION = """Patient records (most recent first):
[1] (2026-01-07) Finding — Weight (kg): 41.0 kg
[2] (2025-12-31) Finding — Weight (kg): 42.0 kg
[3] (2025-10-22) Finding — Number of hospitalizations in past year: 27 # hospitalizations
"""


def test_event_timeline_excludes_program_enrollment_from_last_visit():
    block = temporal.build_temporal_block(_PROGRAM_CONFOUND, anchor="2026-06-20")
    visit_line = next((l for l in block.splitlines() if l.startswith("Most recent clinical visit")), None)
    assert visit_line is not None, block
    assert "2026-01-07" in visit_line and "2026-05-20" not in visit_line  # the visit, not the enrollment
    admin_line = next((l for l in block.splitlines() if "Administrative record" in l), None)
    assert admin_line is not None and "2026-05-20" in admin_line and "Tuberculosis" in admin_line


def test_resolve_anchor_latest_record_ignores_program_enrollment():
    # the default "now" = the last CLINICAL record date, not a post-dated administrative enrollment
    assert temporal.resolve_anchor("latest_record", _PROGRAM_CONFOUND) == "2026-01-07"


# ---- temporal_facts.v1.1 sidecar -------------------------------------------

def test_temporal_facts_captures_return_visit_dates_and_classifies_against_anchor():
    facts = temporal.build_temporal_facts(_CHART, "2006-06-01")
    assert facts["schema_version"] == "temporal_facts.v1.1"
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


def test_render_temporal_facts_includes_full_json_sidecar_marker():
    facts = temporal.build_temporal_facts(_CHART, "2006-05-18")
    rendered = temporal.render_temporal_facts(facts)
    assert "temporal_facts.v1.1" in rendered
    assert '"date_ledger"' in rendered
    assert '"return_visit_dates"' in rendered
    assert '"numeric_series"' in rendered


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
    assert any(c["id"] == "upcoming_date" and c["status"] == "fail" for c in gate["checks"])
    assert "No upcoming appointment is documented after 2006-06-01" in gate["patch_answer"]
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
    assert gate["patch_answer"]  # patch is advisory; callers apply it only in enforce mode


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
