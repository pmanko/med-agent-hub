"""Unit tests for Slice 2 WAHS Multi-Source Perturbation Generator & Evaluation Rubric Builder."""

from __future__ import annotations

from typing import Any, Dict

from scripts.build_wahs_eval_fixtures import (
    NATURAL_ANOMALIES_BY_PATIENT,
    build_eval_cases,
    inject_byron_multisource_perturbations,
    inject_lamar_multisource_perturbations,
    inject_maurice_multisource_perturbations,
)
from server.fhir import (
    condense_bundle_to_budget,
    detect_condition_conflicts,
    detect_medication_conflicts,
    extract_bundle,
    normalize_drug_ingredient,
)
from tests.test_fhir_condenser import _make_synthetic_wahs_bundle


def test_detect_medication_conflicts_brand_generic_and_statin_class() -> None:
    """Brand vs. generic duplicates and concurrent statins are deterministically flagged."""
    assert normalize_drug_ingredient("Plavix 75 MG Oral Tablet") == "clopidogrel"
    assert normalize_drug_ingredient("Clopidogrel 75 MG Oral Tablet") == "clopidogrel"
    assert normalize_drug_ingredient("Prinivil 10 MG Oral Tablet") == "lisinopril"
    assert normalize_drug_ingredient("Lipitor 40 MG Oral Tablet") == "atorvastatin"

    active_meds = [
        {
            "resourceUuid": "med-clop-generic",
            "display": "Clopidogrel 75 MG Oral Tablet",
            "ingredient_key": "clopidogrel",
            "date": "2023-11-28",
        },
        {
            "resourceUuid": "med-plavix-brand",
            "display": "Plavix 75 MG Oral Tablet [clopidogrel]",
            "ingredient_key": "clopidogrel",
            "date": "2023-11-29",
        },
        {
            "resourceUuid": "med-simva",
            "display": "Simvastatin 20 MG Oral Tablet",
            "ingredient_key": "simvastatin",
            "date": "2023-11-20",
        },
        {
            "resourceUuid": "med-lipitor",
            "display": "Lipitor 40 MG Oral Tablet [atorvastatin]",
            "ingredient_key": "atorvastatin",
            "date": "2023-11-29",
        },
    ]
    conflicts = detect_medication_conflicts(active_meds)
    assert "med-clop-generic" in conflicts
    assert "med-plavix-brand" in conflicts
    assert "duplicate_therapy" in conflicts["med-clop-generic"]
    assert "clopidogrel" in conflicts["med-clop-generic"]
    assert "med-simva" in conflicts
    assert "med-lipitor" in conflicts
    assert "duplicate_therapy" in conflicts["med-lipitor"]
    assert "statin" in conflicts["med-lipitor"]


def test_detect_condition_conflicts_active_vs_resolved_cross_clinic() -> None:
    """Active vs. resolved status conflicts for the same SNOMED condition are flagged on both items."""
    active = [
        {
            "resourceUuid": "cond-sinus-active",
            "code": "40055000",
            "display": "Chronic sinusitis (disorder)",
            "onset": "2008-02-29",
            "status": "active",
        }
    ]
    resolved = [
        {
            "resourceUuid": "cond-sinus-resolved",
            "code": "40055000",
            "display": "Chronic sinusitis (disorder)",
            "onset": "2008-02-29",
            "abatement": "2024-09-10",
            "status": "resolved",
        }
    ]
    conflicts = detect_condition_conflicts(active, resolved)
    assert "cond-sinus-active" in conflicts
    assert "cond-sinus-resolved" in conflicts
    assert "status_conflict" in conflicts["cond-sinus-active"]
    assert "status_conflict" in conflicts["cond-sinus-resolved"]


def test_multisource_perturbation_functions_preserve_anomaly_uuids_across_budgets() -> None:
    """Perturbation injectors are deterministic, non-mutating, and retain 100% of injected UUIDs down to 2,048 tokens."""
    base_bundle: Dict[str, Any] = _make_synthetic_wahs_bundle()
    original_entry_count = len(base_bundle["entry"])

    byron_b, byron_anoms = inject_byron_multisource_perturbations(base_bundle)
    lamar_b, lamar_anoms = inject_lamar_multisource_perturbations(base_bundle)
    maurice_b, maurice_anoms = inject_maurice_multisource_perturbations(base_bundle)

    # Input bundle was not mutated
    assert len(base_bundle["entry"]) == original_entry_count
    assert len(byron_b["entry"]) == original_entry_count + 2
    assert len(lamar_b["entry"]) == original_entry_count + 3
    assert len(maurice_b["entry"]) == original_entry_count + 3

    # Repeated invocation produces identical output
    byron_b2, _ = inject_byron_multisource_perturbations(base_bundle)
    assert byron_b == byron_b2

    for perturbed_bundle, anoms in (
        (byron_b, byron_anoms),
        (lamar_b, lamar_anoms),
        (maurice_b, maurice_anoms),
    ):
        extracted = extract_bundle(perturbed_bundle)
        injected_uuids = [
            uid
            for a in anoms
            for uid in a["expected_resource_uuids"]
            if uid.startswith(("imm-", "cond-", "med-"))
        ]
        assert injected_uuids
        for budget in (16384, 8192, 4096, 2048):
            condensed = condense_bundle_to_budget(
                extracted,
                target_tokens=budget,
                mode="reconcile",
            )
            assert condensed.actual_tokens <= budget
            present = {
                str(u)
                for m in condensed.mappings
                for u in ([m.get("resourceUuid")] + list(m.get("memberUuids") or []))
                if u
            }
            for uid in injected_uuids:
                assert uid in present, f"Missing {uid} at budget {budget}"


def test_build_eval_cases_covers_all_wahs_use_cases() -> None:
    """`build_eval_cases` produces machine-checkable rubrics across Use Cases A, B-1, B-2, and B-3."""
    cases = build_eval_cases()
    assert len(cases) >= 30

    by_use_case: Dict[str, int] = {}
    for c in cases:
        use_case = str(c["use_case"])
        by_use_case[use_case] = by_use_case.get(use_case, 0) + 1
        assert c["case_id"]
        assert str(c["patient_bundle"]).endswith(".json")
        stem = str(c["patient_bundle"]).replace("_multisource.json", "").replace(".json", "")
        assert stem in NATURAL_ANOMALIES_BY_PATIENT
        assert c["mode"] in {"reconcile", "summary", "qa"}
        assert c["question"]

    assert by_use_case["A_reconcile"] >= 6
    assert by_use_case.get("B1_encounter_summary", 0) + by_use_case.get("B1_longitudinal_summary", 0) >= 4
    assert by_use_case["B2_directed_summary"] >= 3
    assert by_use_case["B3_qa_answerable"] >= 6
    assert by_use_case["B3_qa_abstention"] >= 3

    # Verify abstention cases explicitly require abstention and expect zero facts
    for c in cases:
        if c["use_case"] == "B3_qa_abstention":
            assert c["expected_answer_type"] == "abstain_not_in_chart"
            assert c["expected_facts"] == []
