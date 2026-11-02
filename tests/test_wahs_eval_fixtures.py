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
    build_tier_items,
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

    # Linked order + statement + administration for the same therapy must NOT be flagged as duplicate
    linked_meds = [
        {
            "resourceUuid": "med-order-1",
            "display": "Lisinopril 10 MG Oral Tablet",
            "ingredient_key": "lisinopril",
            "date": "2024-01-10",
            "linked_order_uuids": [],
        },
        {
            "resourceUuid": "med-stmt-1",
            "display": "Prinivil 10 MG Oral Tablet",
            "ingredient_key": "lisinopril",
            "date": "2024-01-11",
            "linked_order_uuids": ["med-order-1"],
        },
        {
            "resourceUuid": "med-admin-1",
            "display": "Lisinopril 10 MG Oral Tablet",
            "ingredient_key": "lisinopril",
            "date": "2024-01-11",
            "linked_order_uuids": ["med-order-1"],
        },
    ]
    assert detect_medication_conflicts(linked_meds) == {}


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

    # Non-resolved statuses (remission, inactive) or missing abatement must not produce false status_conflict
    assert (
        detect_condition_conflicts(
            active,
            [
                {
                    "resourceUuid": "cond-sinus-remission",
                    "code": "40055000",
                    "display": "Chronic sinusitis (disorder)",
                    "onset": "2008-02-29",
                    "abatement": "2024-09-10",
                    "status": "remission",
                },
                {
                    "resourceUuid": "cond-sinus-no-abatement",
                    "code": "40055000",
                    "display": "Chronic sinusitis (disorder)",
                    "onset": "2008-02-29",
                    "abatement": "",
                    "status": "resolved",
                },
            ],
        )
        == {}
    )


def test_multisource_perturbation_functions_preserve_anomaly_uuids_across_budgets() -> None:
    """Perturbation injectors are deterministic, non-mutating, and retain 100% of injected UUIDs down to 1,024 tokens."""
    base_bundle: Dict[str, Any] = _make_synthetic_wahs_bundle()
    # Include the natural counterpart resources referenced by multi-source anomalies
    counterpart_entries = [
        {
            "fullUrl": "urn:uuid:83093df7-a1e4-b827-159b-4271c06f5179",
            "resource": {
                "resourceType": "Immunization",
                "id": "83093df7-a1e4-b827-159b-4271c06f5179",
                "status": "completed",
                "vaccineCode": {
                    "coding": [
                        {
                            "system": "http://hl7.org/fhir/sid/cvx",
                            "code": "140",
                            "display": "Influenza, seasonal, injectable, preservative free",
                        }
                    ],
                    "text": "Influenza, seasonal, injectable, preservative free",
                },
                "occurrenceDateTime": "2026-01-26T08:00:00-08:00",
            },
        },
        {
            "fullUrl": "urn:uuid:83093df7-a1e4-b827-cec7-5e37ae629c31",
            "resource": {
                "resourceType": "Immunization",
                "id": "83093df7-a1e4-b827-cec7-5e37ae629c31",
                "status": "completed",
                "vaccineCode": {
                    "coding": [
                        {
                            "system": "http://hl7.org/fhir/sid/cvx",
                            "code": "208",
                            "display": "SARS-COV-2 (COVID-19) vaccine, mRNA, spike protein, LNP, preservative free, 30 mcg/0.3mL dose",
                        }
                    ],
                    "text": "SARS-COV-2 (COVID-19) vaccine, mRNA, spike protein, LNP, preservative free, 30 mcg/0.3mL dose",
                },
                "occurrenceDateTime": "2022-01-17T08:00:00-08:00",
            },
        },
        {
            "fullUrl": "urn:uuid:4be6b5fb-2a19-943a-19f7-35a11a488712",
            "resource": {
                "resourceType": "Condition",
                "id": "4be6b5fb-2a19-943a-19f7-35a11a488712",
                "clinicalStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                            "code": "active",
                        }
                    ]
                },
                "code": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "40055000",
                            "display": "Chronic sinusitis (disorder)",
                        }
                    ],
                    "text": "Chronic sinusitis (disorder)",
                },
                "onsetDateTime": "2008-02-29T10:00:00-08:00",
            },
        },
        {
            "fullUrl": "urn:uuid:5693c080-f485-91e7-049d-0016c58b2394",
            "resource": {
                "resourceType": "MedicationRequest",
                "id": "5693c080-f485-91e7-049d-0016c58b2394",
                "status": "active",
                "intent": "order",
                "medicationCodeableConcept": {
                    "coding": [
                        {
                            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                            "code": "309362",
                            "display": "Clopidogrel 75 MG Oral Tablet",
                        }
                    ],
                    "text": "Clopidogrel 75 MG Oral Tablet",
                },
                "authoredOn": "2023-11-28T10:00:00-08:00",
            },
        },
        {
            "fullUrl": "urn:uuid:5693c080-f485-91e7-aca9-a1fd06a66487",
            "resource": {
                "resourceType": "MedicationRequest",
                "id": "5693c080-f485-91e7-aca9-a1fd06a66487",
                "status": "active",
                "intent": "order",
                "medicationCodeableConcept": {
                    "coding": [
                        {
                            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                            "code": "312961",
                            "display": "Simvastatin 20 MG Oral Tablet",
                        }
                    ],
                    "text": "Simvastatin 20 MG Oral Tablet",
                },
                "authoredOn": "2023-11-20T10:00:00-08:00",
            },
        },
        {
            "fullUrl": "urn:uuid:5693c080-f485-91e7-cdb1-e0bfb73e6655",
            "resource": {
                "resourceType": "Immunization",
                "id": "5693c080-f485-91e7-cdb1-e0bfb73e6655",
                "status": "completed",
                "vaccineCode": {
                    "coding": [
                        {
                            "system": "http://hl7.org/fhir/sid/cvx",
                            "code": "140",
                            "display": "Influenza, seasonal, injectable, preservative free",
                        }
                    ],
                    "text": "Influenza, seasonal, injectable, preservative free",
                },
                "occurrenceDateTime": "2025-09-28T08:00:00-07:00",
            },
        },
    ]
    base_bundle["entry"].extend(counterpart_entries)
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

    lamar_extracted = extract_bundle(lamar_b)
    lamar_items, _, _ = build_tier_items(lamar_extracted, 4)
    lamar_item_uuids = {
        str(u)
        for item in lamar_items
        for u in ([item.resource_uuid] + list(item.member_uuids or []))
        if u
    }
    assert "cond-ext-lamar-sinusitis-resolved" in lamar_item_uuids

    for label, perturbed_bundle, anoms in (
        ("byron", byron_b, byron_anoms),
        ("lamar", lamar_b, lamar_anoms),
        ("maurice", maurice_b, maurice_anoms),
    ):
        extracted = extract_bundle(perturbed_bundle)
        injected_uuids = [
            uid
            for a in anoms
            for uid in a["expected_resource_uuids"]
        ]
        assert injected_uuids
        for budget in (16384, 8192, 4096, 2048, 1024):
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
            for hpv_uid in ("imm-hpv-1", "imm-hpv-2", "imm-hpv-3"):
                assert hpv_uid in present, f"Missing {hpv_uid} at budget {budget}"
            if budget == 2048:
                if label in ("lamar", "maurice"):
                    assert "duplicate_therapy" in condensed.chart_text
                if label == "lamar":
                    assert "status_conflict" in condensed.chart_text


def test_build_eval_cases_covers_all_wahs_use_cases() -> None:
    """`build_eval_cases` produces machine-checkable rubrics across Use Cases A, B-1, B-2, and B-3."""
    cases = build_eval_cases()
    assert len(cases) == 33

    by_use_case: Dict[str, int] = {}
    case_ids = set()
    for c in cases:
        use_case = str(c["use_case"])
        by_use_case[use_case] = by_use_case.get(use_case, 0) + 1
        assert c["case_id"]
        case_ids.add(str(c["case_id"]))
        assert str(c["patient_bundle"]).endswith(".json")
        stem = str(c["patient_bundle"]).replace("_multisource.json", "").replace(".json", "")
        assert stem in NATURAL_ANOMALIES_BY_PATIENT
        assert c["mode"] in {"reconcile", "summary", "qa"}
        assert c["question"]

    assert by_use_case["A_reconcile"] == 14
    assert "uc_b3_maurice_qa_plavix_duplicate" in case_ids
    assert "uc_a_maurice-setb-flu-iis-dup" in case_ids
    assert by_use_case.get("B1_encounter_summary", 0) + by_use_case.get("B1_longitudinal_summary", 0) >= 4
    assert by_use_case["B2_directed_summary"] >= 3
    assert by_use_case["B3_qa_answerable"] >= 6
    assert by_use_case["B3_qa_abstention"] >= 3

    for c in cases:
        if c["use_case"] == "A_reconcile":
            assert c["expected_facts"]
            assert c["positive_rubric_items"]
            assert any(item.get("strict") is True for item in c["positive_rubric_items"])
            assert c["negative_penalty_items"]
        if c["use_case"] == "B3_qa_abstention":
            assert c["expected_answer_type"] == "abstain_not_in_chart"
            assert c["expected_facts"] == []
