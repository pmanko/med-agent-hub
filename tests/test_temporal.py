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
