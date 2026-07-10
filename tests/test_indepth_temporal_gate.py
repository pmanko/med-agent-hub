from __future__ import annotations

from server.temporal import gate_indepth_claims


FACTS = {
    "schema_version": "temporal_facts.v1",
    "reference_date": "2026-06-20",
    "clinical_dates": [],
    "admin_dates": [],
    "numeric_series": [],
    "appointment_candidates": {"past": [], "today": [], "future": [], "all": []},
}


def test_each_indepth_claim_is_gated_and_unsafe_claim_is_removed():
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
