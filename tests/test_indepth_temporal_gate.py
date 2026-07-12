from __future__ import annotations

import pytest

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


def test_long_indepth_date_is_not_inferred_as_a_last_visit_short_answer():
    facts = {
        **MULTI_COUNT_FACTS,
        "last_clinical_encounter": {
            "date": "2006-06-06",
            "indices": [4],
        },
        "date_ledger": [
            {"iso": "2006-06-06", "date_id": "visit"},
            {"iso": "2006-04-04", "date_id": "cd4"},
        ],
        "date_output_contract": {
            "allowed_iso_dates": ["2006-06-06", "2006-04-04"]
        },
    }
    claim = (
        "The patient's CD4 count was documented on 2006-04-04 as 341 cells/uL [178], "
        "which warrants continued clinical monitoring."
    )

    result = gate_indepth_claims(
        "What was the most recent documented clinical visit?",
        [claim],
        facts,
        mode="enforce",
    )

    assert result["status"] == "checked"
    assert result["claims"] == [claim]


def test_indepth_blood_pressure_claim_does_not_bind_white_blood_cells():
    facts = {
        **FACTS,
        "last_clinical_encounter": {"date": "2006-06-06", "indices": [4]},
        "numeric_series": [
            {
                "concept": "White blood cells",
                "points": [
                    {"date": "2006-04-04", "value": 5300, "unit": "10^3/uL", "index": 185}
                ],
                "trend_supported": False,
                "direction": "none",
            },
            {
                "concept": "Diastolic blood pressure",
                "points": [
                    {"date": "2006-04-25", "value": 70, "unit": "mmHg", "index": 88},
                    {"date": "2006-06-06", "value": 60, "unit": "mmHg", "index": 15},
                ],
                "trend_supported": True,
                "direction": "down",
            },
        ],
        "date_ledger": [
            {"iso": "2006-06-06", "date_id": "visit"},
            {"iso": "2006-04-25", "date_id": "bp-old"},
            {"iso": "2006-04-04", "date_id": "wbc"},
        ],
        "date_output_contract": {
            "allowed_iso_dates": ["2006-06-06", "2006-04-25", "2006-04-04"]
        },
    }
    claim = (
        "The patient's blood pressure readings show a downward trend, with the most recent "
        "diastolic reading being 60 mmHg [15]."
    )

    result = gate_indepth_claims(
        "What was the most recent documented clinical visit?",
        [claim],
        facts,
        mode="enforce",
    )

    assert result["status"] == "checked"
    assert result["claims"] == [claim]


def test_dangling_citation_is_repaired_from_one_exact_numeric_point():
    facts = {
        **FACTS,
        "numeric_series": [
            {
                "concept": "Weight",
                "points": [
                    {"date": "2006-06-06", "value": 71, "unit": "kg", "index": 19}
                ],
                "trend_supported": False,
                "direction": "none",
            }
        ],
        "date_ledger": [{"iso": "2006-06-06", "date_id": "weight"}],
        "date_output_contract": {"allowed_iso_dates": ["2006-06-06"]},
    }

    result = gate_indepth_claims(
        "What was the patient's weight?",
        ["The patient's weight was 71 kg on 2006-06-06 ["],
        facts,
        mode="enforce",
    )

    assert result["status"] == "edited"
    assert result["claims"] == ["The patient's weight was 71 kg on 2006-06-06 [19]"]
    assert result["checks"][0]["citation_repair"]["source_index"] == 19


def test_dangling_citation_is_not_repaired_when_point_binding_is_ambiguous():
    facts = {
        **FACTS,
        "numeric_series": [
            {
                "concept": "Weight",
                "points": [
                    {"date": "2006-06-06", "value": 71, "unit": "kg", "index": 19},
                    {"date": "2006-06-06", "value": 71, "unit": "kg", "index": 20},
                ],
                "trend_supported": True,
                "direction": "flat",
            }
        ],
        "date_ledger": [{"iso": "2006-06-06", "date_id": "weight"}],
        "date_output_contract": {"allowed_iso_dates": ["2006-06-06"]},
    }
    claim = "The patient's weight was 71 kg on 2006-06-06 ["

    result = gate_indepth_claims(
        "What was the patient's weight?", [claim], facts, mode="enforce"
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == []
    assert result["removed"] == [1]
    assert "citation_repair" not in result["checks"][0]


def test_uncited_claim_without_malformed_marker_is_not_auto_cited():
    facts = {
        **FACTS,
        "numeric_series": [
            {
                "concept": "Weight",
                "points": [
                    {"date": "2006-06-06", "value": 71, "unit": "kg", "index": 19}
                ],
                "trend_supported": False,
                "direction": "none",
            }
        ],
        "date_ledger": [{"iso": "2006-06-06", "date_id": "weight"}],
        "date_output_contract": {"allowed_iso_dates": ["2006-06-06"]},
    }
    claim = "The patient's weight was 71 kg on 2006-06-06."

    result = gate_indepth_claims(
        "What was the patient's weight?", [claim], facts, mode="enforce"
    )

    assert result["claims"] == [claim]
    assert "citation_repair" not in result["checks"][0]


@pytest.mark.parametrize("written_unit", ("lb", "kg/m2", "kg/m²", "kg per m2"))
def test_dangling_citation_with_wrong_unit_is_rejected_not_repaired(written_unit):
    facts = {
        **FACTS,
        "numeric_series": [
            {
                "concept": "Weight",
                "points": [
                    {"date": "2006-06-06", "value": 71, "unit": "kg", "index": 19}
                ],
                "trend_supported": False,
                "direction": "none",
            }
        ],
        "date_ledger": [{"iso": "2006-06-06", "date_id": "weight"}],
        "date_output_contract": {"allowed_iso_dates": ["2006-06-06"]},
    }

    result = gate_indepth_claims(
        "What was the patient's weight?",
        [f"The patient's weight was 71 {written_unit} on 2006-06-06 ["],
        facts,
        mode="enforce",
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == []
    assert "citation_repair" not in result["checks"][0]


def test_existing_citation_does_not_authorize_cleaning_a_second_uncited_claim():
    claim = "Weight was 71 kg [19]. Monitoring is recommended ["

    result = gate_indepth_claims(
        "What was the patient's weight?", [claim], FACTS, mode="enforce"
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == []
    assert "citation_repair" not in result["checks"][0]


def test_dangling_citation_repair_does_not_mutate_off_or_warn_modes():
    facts = {
        **FACTS,
        "numeric_series": [
            {
                "concept": "Weight",
                "points": [
                    {"date": "2006-06-06", "value": 71, "unit": "kg", "index": 19}
                ],
                "trend_supported": False,
                "direction": "none",
            }
        ],
    }
    claim = "The patient's weight was 71 kg on 2006-06-06 ["

    off = gate_indepth_claims("What was the weight?", [claim], facts, mode="off")
    warn = gate_indepth_claims("What was the weight?", [claim], facts, mode="warn")

    assert off["claims"] == [claim]
    assert warn["claims"] == [claim]
    assert "citation_repair" not in off["checks"][0]
    assert "citation_repair" not in warn["checks"][0]
    assert warn["checks"][0]["gate"]["status"] == "fail"


def test_grouped_numeric_citations_are_canonicalized_in_enforce_mode():
    claim = "The recorded vital signs are supported by sources [18, 17, 15]."

    result = gate_indepth_claims("What was recorded?", [claim], FACTS, mode="enforce")

    assert result["status"] == "edited"
    assert result["claims"] == [
        "The recorded vital signs are supported by sources [18][17][15]."
    ]
    assert result["checks"][0]["citation_canonicalization"] == {
        "status": "canonicalized",
        "source_indices": [18, 17, 15],
    }


def test_grouped_numeric_citations_are_not_mutated_in_warn_mode():
    claim = "The recorded vital signs are supported by sources [18, 17, 15]."

    result = gate_indepth_claims("What was recorded?", [claim], FACTS, mode="warn")

    assert result["claims"] == [claim]
    assert "citation_canonicalization" not in result["checks"][0]


def test_grouped_numeric_citations_deduplicate_without_reordering():
    claim = "The recorded vital signs are supported by sources [18, 18, 17]."

    result = gate_indepth_claims("What was recorded?", [claim], FACTS, mode="enforce")

    assert result["claims"] == [
        "The recorded vital signs are supported by sources [18][17]."
    ]
    assert result["checks"][0]["citation_canonicalization"]["source_indices"] == [
        18,
        17,
    ]


@pytest.mark.parametrize(
    "citation",
    (
        "[[18,17]]",
        "x[18,17]y",
        "[18, foo]",
    ),
)
def test_malformed_grouped_citations_fail_closed_in_enforce_mode(citation):
    claim = f"The recorded vital signs are supported by sources {citation}."

    result = gate_indepth_claims("What was recorded?", [claim], FACTS, mode="enforce")

    assert result["status"] == "needs_review"
    assert result["claims"] == []
    assert result["removed"] == [1]
    citation_checks = [
        check
        for check in result["checks"][0]["gate"]["checks"]
        if check["id"] == "citation_format"
    ]
    assert len(citation_checks) == 1
    assert citation_checks[0]["severity"] == "block"
