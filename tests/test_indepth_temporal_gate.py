from __future__ import annotations

from server.temporal import gate_indepth_claims

FACTS = {
    "reference_date": "2026-06-20",
    "clinical_dates": [],
    "admin_dates": [],
    "numeric_series": [],
    "date_ledger": [{"iso": "2026-06-20", "date_id": "d1"}],
    "date_output_contract": {"allowed_iso_dates": ["2026-06-20"]},
    "appointment_candidates": {"past": [], "today": [], "future": [], "all": []},
}

MULTI_COUNT_FACTS = {
    **FACTS,
    "date_ledger": [{"iso": "2006-04-04", "date_id": "d1"}],
    "date_output_contract": {"allowed_iso_dates": ["2006-04-04"]},
    "numeric_series": [
        {
            "concept": "Absolute lymphocyte count",
            "points": [
                {"date": "2006-04-04", "value": 299000, "unit": "10^3/uL", "index": 177}
            ],
            "trend_supported": False,
            "direction": "none",
        },
        {
            "concept": "CD4 count",
            "points": [
                {"date": "2006-04-04", "value": 341, "unit": "cells/uL", "index": 178}
            ],
            "trend_supported": False,
            "direction": "none",
        },
    ],
}


def test_indepth_temporal_gate_checks_each_claim_and_removes_unsafe_output():
    result = gate_indepth_claims(
        "Summarize follow-up timing",
        [
            "Follow-up should consider the clinical context.",
            "The visit occurred on 2025-10-//13.",
        ],
        FACTS,
        mode="enforce",
    )

    assert result["status"] == "edited"
    assert result["claims"] == ["Follow-up should consider the clinical context."]
    assert result["removed"] == [2]
    assert [item["claim_index"] for item in result["checks"]] == [1, 2]


def test_fully_rejected_indepth_cannot_report_complete():
    result = gate_indepth_claims(
        "When was follow-up?",
        ["The visit occurred on 2025-10-//13."],
        FACTS,
        mode="enforce",
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == []
    assert result["removed"] == [1]


def test_non_temporal_claim_still_records_not_applicable_verdict():
    result = gate_indepth_claims(
        "Summarize care",
        ["Continue to verify treatment against the chart."],
        FACTS,
        mode="enforce",
    )

    assert result["status"] == "checked"
    assert result["claims"] == ["Continue to verify treatment against the chart."]
    assert result["checks"][0]["gate"]["status"] == "not_applicable"


def test_empty_indepth_cannot_report_checked_or_complete():
    result = gate_indepth_claims("What should I know?", [], FACTS, mode="enforce")

    assert result["status"] == "needs_review"
    assert result["claims"] == []


def test_unsafe_patch_cannot_replace_claim_with_uncited_different_series():
    result = gate_indepth_claims(
        "Has this patient's weight changed recently?",
        [
            "The CD4 count was 341 cells/uL on 2006-04-04; the overall trend of immune suppression should be monitored."
        ],
        MULTI_COUNT_FACTS,
        mode="enforce",
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == []
    assert result["removed"] == [1]
    check = result["checks"][0]["gate"]
    assert check["patch_citations"] == [178]
    assert {index for item in check["checks"] for index in item["source_indices"]} == {178}


def test_safe_patch_must_stay_within_original_claim_citations():
    result = gate_indepth_claims(
        "What is the CD4 trend?",
        ["The CD4 count is declining over time [178]."],
        MULTI_COUNT_FACTS,
        mode="enforce",
    )

    assert result["status"] == "edited"
    assert result["removed"] == []
    assert result["claims"][0].startswith("The chart shows one dated CD4 count measurement")
    assert "[178]" in result["claims"][0]
