from __future__ import annotations

from server.temporal import gate_indepth_claims

FACTS = {
    "schema_version": "temporal_facts.v1",
    "reference_date": "2026-06-20",
    "clinical_dates": [],
    "admin_dates": [],
    "numeric_series": [],
    "date_ledger": [{"iso": "2026-06-20", "date_id": "d1"}],
    "date_output_contract": {"allowed_iso_dates": ["2026-06-20"]},
    "appointment_candidates": {"past": [], "today": [], "future": [], "all": []},
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
