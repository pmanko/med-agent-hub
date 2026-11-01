from __future__ import annotations

import json
from pathlib import Path

import pytest

from server.temporal import build_temporal_facts, gate_indepth_claims

FIXTURES = Path(__file__).parent / "fixtures"

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


def test_indepth_temporal_gate_checks_each_claim_and_flags_unsafe_output():
    result = gate_indepth_claims(
        [
            "Follow-up should consider the clinical context.",
            "The visit occurred on 2025-10-//13.",
        ],
        FACTS,
        mode="enforce",
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == [
        "Follow-up should consider the clinical context.",
        "The visit occurred on 2025-10-//13.",
    ]
    assert result["flagged"] == [2]
    assert [item["claim_index"] for item in result["checks"]] == [1, 2]


def test_a_claim_the_date_check_cannot_correct_is_kept_and_flagged():
    result = gate_indepth_claims(
        ["The visit occurred on 2025-10-//13."],
        FACTS,
        mode="enforce",
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == ["The visit occurred on 2025-10-//13."]
    assert result["flagged"] == [1]


def test_non_temporal_claim_still_records_not_applicable_verdict():
    result = gate_indepth_claims(
        ["Continue to verify treatment against the chart."],
        FACTS,
        mode="enforce",
    )

    assert result["status"] == "checked"
    assert result["claims"] == ["Continue to verify treatment against the chart."]
    assert result["checks"][0]["gate"]["status"] == "not_applicable"


def test_empty_indepth_cannot_report_checked_or_complete():
    result = gate_indepth_claims([], FACTS, mode="enforce")

    assert result["status"] == "needs_review"
    assert result["claims"] == []


def test_uncited_value_mismatch_is_flagged_without_introducing_patch_citations():
    claim = "The CD4 count was 300 cells/uL on 2006-04-04."
    result = gate_indepth_claims([claim], MULTI_COUNT_FACTS, mode="enforce")

    assert result["status"] == "needs_review"
    assert result["claims"] == [claim]
    assert result["flagged"] == [1]
    check = result["checks"][0]["gate"]
    assert check["patch_citations"] == [178]


def test_single_point_cd4_trend_claim_targets_only_cited_cd4_series():
    claim = "The CD4 count was 341 cells/uL on 2006-04-04 [178] and shows a declining trend."
    result = gate_indepth_claims([claim], MULTI_COUNT_FACTS, mode="warn")

    assert result["status"] == "needs_review"
    assert result["claims"] == [claim]
    assert result["flagged"] == [1]
    check = result["checks"][0]["gate"]
    assert check["patch_citations"] == [178]
    assert {index for item in check["checks"] for index in item["source_indices"]} == {178}


def test_safe_patch_must_stay_within_original_claim_citations():
    result = gate_indepth_claims(
        ["The CD4 count is declining over time [178]."],
        MULTI_COUNT_FACTS,
        mode="enforce",
    )

    assert result["status"] == "edited"
    assert result["flagged"] == []
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
        [claim], facts, mode="enforce"
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == [claim]
    assert result["flagged"] == [1]
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
        [claim], facts, mode="enforce"
    )

    assert result["claims"] == [claim]
    assert "citation_repair" not in result["checks"][0]


@pytest.mark.parametrize("written_unit", ("lb", "kg/m2", "kg/m²", "kg per m2"))
def test_dangling_citation_with_wrong_unit_is_flagged_not_repaired(written_unit):
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

    claim = f"The patient's weight was 71 {written_unit} on 2006-06-06 ["
    result = gate_indepth_claims(
        [claim],
        facts,
        mode="enforce",
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == [claim]
    assert result["flagged"] == [1]
    assert "citation_repair" not in result["checks"][0]


def test_existing_citation_does_not_authorize_cleaning_a_second_uncited_claim():
    claim = "Weight was 71 kg [19]. Monitoring is recommended ["

    result = gate_indepth_claims(
        [claim], FACTS, mode="enforce"
    )

    assert result["status"] == "needs_review"
    assert result["claims"] == [claim]
    assert result["flagged"] == [1]
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

    off = gate_indepth_claims([claim], facts, mode="off")
    warn = gate_indepth_claims([claim], facts, mode="warn")

    assert off["claims"] == [claim]
    assert warn["claims"] == [claim]
    assert "citation_repair" not in off["checks"][0]
    assert "citation_repair" not in warn["checks"][0]
    assert warn["checks"][0]["gate"]["status"] == "fail"


def test_grouped_numeric_citations_are_canonicalized_in_enforce_mode():
    claim = "The recorded vital signs are supported by sources [18, 17, 15]."

    result = gate_indepth_claims([claim], FACTS, mode="enforce")

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

    result = gate_indepth_claims([claim], FACTS, mode="warn")

    assert result["claims"] == [claim]
    assert "citation_canonicalization" not in result["checks"][0]


def test_grouped_numeric_citations_deduplicate_without_reordering():
    claim = "The recorded vital signs are supported by sources [18, 18, 17]."

    result = gate_indepth_claims([claim], FACTS, mode="enforce")

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
def test_malformed_grouped_citations_are_flagged_in_enforce_mode(citation):
    claim = f"The recorded vital signs are supported by sources {citation}."

    result = gate_indepth_claims([claim], FACTS, mode="enforce")

    assert result["status"] == "needs_review"
    assert result["claims"] == [claim]
    assert result["flagged"] == [1]
    citation_checks = [
        check
        for check in result["checks"][0]["gate"]["checks"]
        if check["id"] == "citation_format"
    ]
    assert len(citation_checks) == 1
    assert citation_checks[0]["severity"] == "block"


def _flags(result):
    return sorted(
        {
            f"{check['id']}:{check['status']}"
            for claim in result["checks"]
            for check in claim["gate"]["checks"]
            if check["status"] in {"fail", "warn"}
        }
    )


def test_a_claim_stating_one_value_passes_when_it_asserts_no_trend():
    # Under a question asking how weight changed, this claim used to fail as a CD4 trend.
    claim = (
        "The patient's low CD4 count of 341 cells/uL recorded on 2006-04-04 [178] "
        "places them at risk for opportunistic infections."
    )

    result = gate_indepth_claims([claim], MULTI_COUNT_FACTS, mode="enforce")

    assert result["status"] == "checked"
    assert result["claims"] == [claim]
    assert _flags(result) == []


def test_a_claim_asserting_a_trend_over_one_point_still_fails():
    result = gate_indepth_claims(
        ["The CD4 count has declined [178]."], MULTI_COUNT_FACTS, mode="warn"
    )

    assert result["status"] == "needs_review"
    assert result["flagged"] == [1]
    assert "single_point_trend:fail" in _flags(result)


def test_generic_count_trend_claim_resolves_series_from_citation_without_question():
    claim = "The count has declined to 341 cells/uL [178]."

    warn_result = gate_indepth_claims([claim], MULTI_COUNT_FACTS, mode="warn")
    assert warn_result["status"] == "needs_review"
    assert warn_result["flagged"] == [1]
    assert "single_point_trend:fail" in _flags(warn_result)

    enforce_result = gate_indepth_claims([claim], MULTI_COUNT_FACTS, mode="enforce")
    assert enforce_result["status"] == "edited"
    assert enforce_result["flagged"] == []
    assert enforce_result["claims"] == [
        "The chart shows one dated CD4 count measurement: 341 cells/uL on 2006-04-04 [178]. "
        "A trend cannot be determined from one point."
    ]


def test_engine_judges_indepth_claims_without_question_while_answer_gate_uses_question(
    monkeypatch,
):
    import asyncio
    from dataclasses import replace
    from server import engine, team
    from server.levels_loader import get_profile
    from tests.factories import patient_source_registry

    chart = (
        "[1] (2026-01-01) CD4 count: 341 cells/uL\n"
        "[2] (2026-01-01) Weight (kg): 70 kg\n"
    )
    single_value_claim = "The CD4 count was 341 cells/uL on 2026-01-01 [1]."

    async def fake_answer(*_args, **_kwargs):
        return "The weight was 70 kg on 2026-01-01 [2].", [2], []

    async def fake_substantive(*_args, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def fake_ground(_client, _model, _answer, refs, _map, **_kwargs):
        return [{**ref, "groundingStatus": "verified", "grounded": True} for ref in refs]

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_ensure_substantive_answer", fake_substantive)
    monkeypatch.setattr(team, "_ground_references", fake_ground)
    monkeypatch.setattr(team, "_write_trace", lambda *_a, **_k: None)

    class _Counter:
        async def count(self, *_args):
            return 1

        async def count_chat(self, *_args):
            return 1

    registry = patient_source_registry(
        chart,
        [
            {
                "resourceType": "obs",
                "resourceUuid": "cd4",
                "date": "2026-01-01",
                "text": "(2026-01-01) CD4 count: 341 cells/uL",
            },
            {
                "resourceType": "obs",
                "resourceUuid": "weight",
                "date": "2026-01-01",
                "text": "(2026-01-01) Weight (kg): 70 kg",
            },
        ],
    )
    profile = get_profile("single-12b-checked")
    profile = replace(
        profile,
        stages=tuple(stage for stage in profile.stages if stage != "review"),
        policies={**profile.policies, "anchor": "2026-06-20", "drug_safety": False},
    )
    request = engine.ExecutionRequest(
        profile=profile,
        messages=[{"role": "user", "content": "How has the weight changed over the past year?"}],
        patient="synthetic",
        source_registry=registry,
        token_counter=_Counter(),
    )

    async def fake_indepth_single_value(*_args, **_kwargs):
        return [single_value_claim], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_gen_indepth", fake_indepth_single_value)
    payload = json.loads(asyncio.run(engine.drain_profile(request)))
    assert payload["temporalGate"]["status"] == "fail"
    assert payload["temporalGate"]["applied"] == "patch"
    assert payload["inDepth"]["answer"] == f"- {single_value_claim}"
    assert payload["inDepth"]["validation"]["status"] == "checked"

    async def fake_indepth_unsupported_trend(*_args, **_kwargs):
        return ["The CD4 count has declined on 2026-01_01 [2]."], {"level": "green", "note": ""}

    monkeypatch.setattr(team, "_gen_indepth", fake_indepth_unsupported_trend)
    flagged_payload = json.loads(asyncio.run(engine.drain_profile(request)))
    assert flagged_payload["inDepth"]["validation"]["status"] == "needs_review"
    assert flagged_payload["inDepth"]["validation"]["flagged"] == [1]


def test_risk_and_outcome_words_are_not_read_as_the_patients_trend():
    claims = [
        "The low CD4 count reflects a severely compromised immune status, increasing the risk of infection.",
        "The low CD4 count of 341 cells/uL [178] indicates increasing risk of infection.",
        "The low CD4 count of 341 cells/uL [178] increases the risk of infection.",
        "Antiretroviral therapy should start regardless of the CD4 count to improve outcomes.",
        "A single CD4 count of 341 cells/uL [178] is insufficient to establish a trend.",
    ]

    result = gate_indepth_claims(claims, MULTI_COUNT_FACTS, mode="enforce")

    assert result["flagged"] == []
    assert result["claims"] == claims
    assert _flags(result) == []


def test_trend_denial_is_scoped_to_its_own_series_and_clause():
    chart = (
        "[1] (2025-10-22) Finding — Weight: 52 kg\n"
        "[2] (2026-01-07) Finding — Weight: 41 kg\n"
        "[3] (2025-11-24) Test — CD4 count: 341 cells/uL\n"
        "[4] (2026-01-07) Encounter — Clinic visit\n"
    )
    facts = build_temporal_facts(chart, "2026-06-20")

    cross_series = (
        "Weight increased [1][2], while a single CD4 count of 341 cells/uL [3] "
        "is insufficient to establish a trend."
    )
    cross_result = gate_indepth_claims([cross_series], facts, mode="enforce")
    assert cross_result["status"] == "needs_review"
    assert cross_result["flagged"] == [1]
    assert cross_result["claims"] == [cross_series]
    assert _flags(cross_result) == ["trend_direction:fail"]

    single_positive = "The CD4 count is declining based on a single value [3]."
    single_warn = gate_indepth_claims([single_positive], facts, mode="warn")
    assert single_warn["status"] == "needs_review"
    assert single_warn["flagged"] == [1]
    assert _flags(single_warn) == ["single_point_trend:fail"]

    other_series_denial = "There is no clear trend in weight, but the CD4 count is declining [3]."
    other_warn = gate_indepth_claims([other_series_denial], facts, mode="warn")
    assert other_warn["status"] == "needs_review"
    assert other_warn["flagged"] == [1]
    assert _flags(other_warn) == ["single_point_trend:fail"]

    contrast_trend = "The CD4 count shows a declining trend, not a stable one [3]."
    contrast_warn = gate_indepth_claims([contrast_trend], facts, mode="warn")
    assert contrast_warn["status"] == "needs_review"
    assert contrast_warn["flagged"] == [1]
    assert _flags(contrast_warn) == ["single_point_trend:fail"]


def test_measurement_explicitly_placed_at_last_visit_is_checked_against_last_encounter():
    chart = (
        "[1] (2025-10-22) Finding — Weight: 52 kg\n"
        "[2] (2026-01-07) Finding — Weight: 41 kg\n"
        "[4] (2026-01-07) Encounter — Clinic visit\n"
    )
    facts = build_temporal_facts(chart, "2026-06-20")
    claim = "At the last visit, weight was 52 kg on 2025-10-22 [1]."

    result = gate_indepth_claims([claim], facts, mode="enforce")
    assert result["status"] == "needs_review"
    assert result["flagged"] == [1]
    assert result["claims"] == [claim]
    assert "last_visit:fail" in _flags(result)


def test_coordinated_dates_and_shared_leading_date_bind_values_across_clauses():
    chart = (
        "[1] (2025-10-22) Finding — Weight: 52 kg\n"
        "[2] (2026-01-07) Finding — Weight: 41 kg\n"
        "[3] (2025-10-22) Test — CD4 count: 200 cells/uL\n"
        "[4] (2026-01-07) Test — CD4 count: 341 cells/uL\n"
    )
    facts = build_temporal_facts(chart, "2026-06-20")

    coordinated = "Weight was 52 kg on 2025-10-22 and 2026-01-07 [1][2]."
    coord_warn = gate_indepth_claims([coordinated], facts, mode="warn")
    assert coord_warn["status"] == "needs_review"
    assert coord_warn["flagged"] == [1]
    assert "date_value_binding:fail" in _flags(coord_warn)

    shared_leading = "On 2025-10-22, weight was 52 kg and CD4 count was 341 cells/uL [4]."
    shared_warn = gate_indepth_claims([shared_leading], facts, mode="warn")
    assert shared_warn["status"] == "needs_review"
    assert shared_warn["flagged"] == [1]
    assert "date_value_binding:fail" in _flags(shared_warn)


@pytest.mark.parametrize(
    "goldens_file", ["indepth-gate-goldens.json", "indepth-gate-goldens-2026-10-27.json"]
)
def test_claims_the_gate_questioned_in_past_runs_get_the_reviewed_verdict(goldens_file):
    """Every In-Depth claim the gate questioned in the 26 and 27 October 2026 runs, replayed
    claim-only: wrong or invented dates still fail; claims about risks, single values and the
    correct last visit do not."""
    goldens = json.loads((FIXTURES / goldens_file).read_text())
    facts = {}
    for case in goldens["cases"]:
        chart = case["chart"]
        if chart not in facts:
            text = (FIXTURES / "charts" / f"{chart}.snapshot.txt").read_text()
            facts[chart] = build_temporal_facts(text, goldens["reference_date"])

        result = gate_indepth_claims([case["claim"]], facts[chart], mode="enforce")

        assert _flags(result) == case["expected"], case["scenario"]
        assert result["claims"] == [case.get("expected_claim", case["claim"])], case["scenario"]
