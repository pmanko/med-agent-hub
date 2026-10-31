"""Deterministic Multi-Source Perturbation Generator and Evaluation Rubric Builder for WAHS+.

Generates:
1. Set A (Natural Synthea Bundles) & Set B (Multi-Source Perturbed Bundles):
   - Injects realistic cross-organization immunization duplicates (WA IIS vs clinic/pharmacy)
   - Injects cross-clinic brand vs generic active medication duplicates (Plavix vs Clopidogrel,
     Lipitor vs Simvastatin, Prinivil vs Lisinopril)
   - Injects cross-clinic active vs resolved condition status conflicts (Chronic sinusitis)
2. `ground_truth.json`: Machine-checkable anomaly and provenance ledger for all 6 bundles.
3. `wahs_eval_cases.jsonl`: 31 structured evaluation cases covering:
   - Use Case A (Reconciliation & Anomaly Detection: 12 cases)
   - Use Case B-1 (Encounter & Longitudinal Summarization: 4 cases, including Maurice CABG ICU stay)
   - Use Case B-2 (Directed / Purpose-Driven Summaries: 3 cases)
   - Use Case B-3 (Patient Q&A + Unanswerable Abstention Probes: 12 cases)
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from server.fhir import audit_cohort_code_coverage, condense_bundle_to_budget, extract_bundle

try:
    from scripts.wahs_uscore_archetypes import build_deidentified_real_clinical_summaries
except ImportError:  # pragma: no cover
    from wahs_uscore_archetypes import build_deidentified_real_clinical_summaries  # type: ignore[no-redef]


def inject_byron_multisource_perturbations(bundle: Mapping[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Inject WA IIS Influenza duplicate (+2d) and Pharmacy COVID-19 duplicate (+1d) into Byron's bundle."""
    perturbed = copy.deepcopy(dict(bundle))
    entries = list(perturbed.get("entry") or [])
    patient_id = "83093df7-a1e4-b827-72e6-c6d604fd7748"
    for e in entries:
        res = (e or {}).get("resource") or {}
        if res.get("resourceType") == "Patient" and res.get("id"):
            patient_id = str(res["id"])
            break

    flu_dup_id = "imm-iis-byron-flu-2026"
    covid_dup_id = "imm-iis-byron-covid-2022"

    entries.extend(
        [
            {
                "fullUrl": f"urn:uuid:{flu_dup_id}",
                "resource": {
                    "resourceType": "Immunization",
                    "id": flu_dup_id,
                    "status": "completed",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "140",
                                "display": "Influenza, split virus, trivalent, PF",
                            }
                        ],
                        "text": "Influenza, split virus, trivalent, PF",
                    },
                    "patient": {"reference": f"urn:uuid:{patient_id}"},
                    "occurrenceDateTime": "2026-01-28T10:15:00+00:00",
                    "lotNumber": "WA-IIS-FLU-9921",
                    "performer": [
                        {"actor": {"display": "WA State IIS Immunization Registry (Pharmacy Feed)"}}
                    ],
                },
            },
            {
                "fullUrl": f"urn:uuid:{covid_dup_id}",
                "resource": {
                    "resourceType": "Immunization",
                    "id": covid_dup_id,
                    "status": "completed",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "208",
                                "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
                            }
                        ],
                        "text": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
                    },
                    "patient": {"reference": f"urn:uuid:{patient_id}"},
                    "occurrenceDateTime": "2022-01-18T14:30:00+00:00",
                    "lotNumber": "EL9262-BARTELL",
                    "performer": [{"actor": {"display": "Bartell Drugs #42 - Seattle"}}],
                },
            },
        ]
    )
    perturbed["entry"] = entries

    injected_anomalies = [
        {
            "anomaly_id": "byron-setb-flu-iis-dup",
            "category": "cross_source_immunization_duplicate",
            "severity": "medium",
            "description": "Influenza vaccine recorded 2 days apart across clinic (2026-01-26) and WA State IIS pharmacy feed (2026-01-28).",
            "expected_resource_uuids": [
                "83093df7-a1e4-b827-159b-4271c06f5179",
                flu_dup_id,
            ],
        },
        {
            "anomaly_id": "byron-setb-covid-pharmacy-dup",
            "category": "cross_source_immunization_duplicate",
            "severity": "medium",
            "description": "COVID-19 mRNA vaccine recorded 1 day apart across clinic (2022-01-17) and Bartell Drugs retail pharmacy (2022-01-18).",
            "expected_resource_uuids": [
                "83093df7-a1e4-b827-cec7-5e37ae629c31",
                covid_dup_id,
            ],
        },
    ]
    return perturbed, injected_anomalies


def inject_lamar_multisource_perturbations(bundle: Mapping[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Inject cross-clinic Chronic sinusitis active-vs-resolved conflict and Prinivil/Lisinopril brand/generic duplicate."""
    perturbed = copy.deepcopy(dict(bundle))
    entries = list(perturbed.get("entry") or [])
    patient_id = "4be6b5fb-2a19-943a-9249-7068445be4cc"
    for e in entries:
        res = (e or {}).get("resource") or {}
        if res.get("resourceType") == "Patient" and res.get("id"):
            patient_id = str(res["id"])
            break

    sinus_resolved_id = "cond-ext-lamar-sinusitis-resolved"
    med_lisinopril_id = "med-pcp-lamar-lisinopril"
    med_prinivil_id = "med-cardio-lamar-prinivil"

    entries.extend(
        [
            {
                "fullUrl": f"urn:uuid:{sinus_resolved_id}",
                "resource": {
                    "resourceType": "Condition",
                    "id": sinus_resolved_id,
                    "clinicalStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                "code": "resolved",
                            }
                        ]
                    },
                    "verificationStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                                "code": "confirmed",
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
                    "subject": {"reference": f"urn:uuid:{patient_id}"},
                    "onsetDateTime": "2008-02-29T00:00:00+00:00",
                    "abatementDateTime": "2024-09-10T00:00:00+00:00",
                },
            },
            {
                "fullUrl": f"urn:uuid:{med_lisinopril_id}",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": med_lisinopril_id,
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "314076",
                                "display": "lisinopril 10 MG Oral Tablet",
                            }
                        ],
                        "text": "lisinopril 10 MG Oral Tablet",
                    },
                    "subject": {"reference": f"urn:uuid:{patient_id}"},
                    "authoredOn": "2026-03-17T09:00:00+00:00",
                    "performer": [{"actor": {"display": "SeaMar Community Health Primary Care"}}],
                },
            },
            {
                "fullUrl": f"urn:uuid:{med_prinivil_id}",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": med_prinivil_id,
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "206765",
                                "display": "Prinivil 10 MG Oral Tablet [lisinopril]",
                            }
                        ],
                        "text": "Prinivil 10 MG Oral Tablet [lisinopril]",
                    },
                    "subject": {"reference": f"urn:uuid:{patient_id}"},
                    "authoredOn": "2026-05-12T14:00:00+00:00",
                    "performer": [{"actor": {"display": "UW Medicine Cardiology Clinic"}}],
                },
            },
        ]
    )
    perturbed["entry"] = entries

    injected_anomalies = [
        {
            "anomaly_id": "lamar-setb-sinusitis-status-conflict",
            "category": "cross_source_condition_status_conflict",
            "severity": "medium",
            "description": "Chronic sinusitis (SNOMED 40055000) is marked ACTIVE in primary care (onset 2008-02-29) but recorded as RESOLVED (2024-09-10) in specialty clinic.",
            "expected_resource_uuids": [
                "4be6b5fb-2a19-943a-19f7-35a11a488712",
                sinus_resolved_id,
            ],
        },
        {
            "anomaly_id": "lamar-setb-lisinopril-prinivil-dup",
            "category": "cross_source_brand_generic_medication_duplicate",
            "severity": "high",
            "description": "Concurrent active ACE-inhibitor prescriptions for the same ingredient (lisinopril 10 MG Oral Tablet from PCP and Prinivil 10 MG Oral Tablet from Cardiology).",
            "expected_resource_uuids": [
                med_lisinopril_id,
                med_prinivil_id,
            ],
        },
    ]
    return perturbed, injected_anomalies


def inject_maurice_multisource_perturbations(bundle: Mapping[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Inject Plavix/Clopidogrel brand/generic duplicate, Lipitor/Simvastatin statin class duplicate,
    and WA IIS Influenza duplicate into Maurice's bundle."""
    perturbed = copy.deepcopy(dict(bundle))
    entries = list(perturbed.get("entry") or [])
    patient_id = "5693c080-f485-91e7-54b9-ad9a8c12af62"
    for e in entries:
        res = (e or {}).get("resource") or {}
        if res.get("resourceType") == "Patient" and res.get("id"):
            patient_id = str(res["id"])
            break

    med_plavix_id = "med-cardio-maurice-plavix"
    med_lipitor_id = "med-cardio-maurice-lipitor"
    flu_iis_id = "imm-iis-maurice-flu-2025"

    entries.extend(
        [
            {
                "fullUrl": f"urn:uuid:{med_plavix_id}",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": med_plavix_id,
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "213169",
                                "display": "Plavix 75 MG Oral Tablet [clopidogrel]",
                            }
                        ],
                        "text": "Plavix 75 MG Oral Tablet [clopidogrel]",
                    },
                    "subject": {"reference": f"urn:uuid:{patient_id}"},
                    "authoredOn": "2026-06-07T11:20:00+00:00",
                    "performer": [{"actor": {"display": "MultiCare Pulse Heart Institute"}}],
                },
            },
            {
                "fullUrl": f"urn:uuid:{med_lipitor_id}",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": med_lipitor_id,
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "617311",
                                "display": "Lipitor 40 MG Oral Tablet [atorvastatin]",
                            }
                        ],
                        "text": "Lipitor 40 MG Oral Tablet [atorvastatin]",
                    },
                    "subject": {"reference": f"urn:uuid:{patient_id}"},
                    "authoredOn": "2026-06-07T11:25:00+00:00",
                    "performer": [{"actor": {"display": "MultiCare Pulse Heart Institute"}}],
                },
            },
            {
                "fullUrl": f"urn:uuid:{flu_iis_id}",
                "resource": {
                    "resourceType": "Immunization",
                    "id": flu_iis_id,
                    "status": "completed",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "140",
                                "display": "Influenza, split virus, trivalent, PF",
                            }
                        ],
                        "text": "Influenza, split virus, trivalent, PF",
                    },
                    "patient": {"reference": f"urn:uuid:{patient_id}"},
                    "occurrenceDateTime": "2025-09-30T16:00:00+00:00",
                    "lotNumber": "WA-IIS-FLU-2025-88",
                    "performer": [{"actor": {"display": "WA State IIS Immunization Registry"}}],
                },
            },
        ]
    )
    perturbed["entry"] = entries

    injected_anomalies = [
        {
            "anomaly_id": "maurice-setb-clopidogrel-plavix-dup",
            "category": "cross_source_brand_generic_medication_duplicate",
            "severity": "high",
            "description": "Concurrent active antiplatelet prescriptions for clopidogrel (generic Clopidogrel 75 MG Oral Tablet and brand Plavix 75 MG Oral Tablet from Cardiology).",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-049d-0016c58b2394",
                med_plavix_id,
            ],
        },
        {
            "anomaly_id": "maurice-setb-simvastatin-lipitor-dup",
            "category": "cross_source_therapeutic_class_duplicate",
            "severity": "high",
            "description": "Concurrent active statin prescriptions (Simvastatin 20 MG Oral Tablet and Lipitor 40 MG Oral Tablet [atorvastatin]).",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-aca9-a1fd06a66487",
                med_lipitor_id,
            ],
        },
        {
            "anomaly_id": "maurice-setb-flu-iis-dup",
            "category": "cross_source_immunization_duplicate",
            "severity": "medium",
            "description": "Influenza vaccine recorded 2 days apart across primary clinic (2025-09-28) and WA State IIS registry (2025-09-30).",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-cdb1-e0bfb73e6655",
                flu_iis_id,
            ],
        },
    ]
    return perturbed, injected_anomalies


NATURAL_ANOMALIES_BY_PATIENT: Dict[str, List[Dict[str, Any]]] = {
    "Byron202_Emard19": [
        {
            "anomaly_id": "byron-seta-hpv-3dose-age11",
            "category": "acip_immunization_schedule_anomaly",
            "severity": "low",
            "description": "3rd HPV quadrivalent dose recorded on 2022-05-02 after series was initiated at age 11 (2020-12-28); CDC ACIP recommends a 2-dose schedule when started before age 15.",
            "expected_resource_uuids": [
                "83093df7-a1e4-b827-a187-1cceb00e687f",
                "83093df7-a1e4-b827-ddc9-a5f99189d24e",
                "83093df7-a1e4-b827-9ba3-181c9443b46b",
            ],
        }
    ],
    "Lamar193_Beer512": [
        {
            "anomaly_id": "lamar-seta-live-zoster-repeat",
            "category": "acip_immunization_schedule_anomaly",
            "severity": "medium",
            "description": "2 doses of live zoster vaccine (Zostavax, CVX 121) recorded on 2021-02-16 and 2022-02-22; live zoster was a single-dose vaccine superseded by 2-dose recombinant Shingrix.",
            "expected_resource_uuids": [
                "4be6b5fb-2a19-943a-d1d2-426855b8e379",
                "4be6b5fb-2a19-943a-fa10-c5d0fe05e7f5",
            ],
        },
        {
            "anomaly_id": "lamar-seta-sdoh-ipv-privacy",
            "category": "sdoh_privacy_sensitive_finding",
            "severity": "high",
            "description": "Active privacy-sensitive SDOH findings ('Victim of intimate partner abuse' and 'Limited social contact') on problem list requiring sharing warning or external redaction.",
            "expected_resource_uuids": [
                "4be6b5fb-2a19-943a-7617-5e43e4960522",
                "4be6b5fb-2a19-943a-09e1-bbaa61fbdd16",
            ],
        },
    ],
    "Maurice742_Auer97": [
        {
            "anomaly_id": "maurice-seta-ckd-stage-conflict",
            "category": "mutually_exclusive_active_conditions",
            "severity": "high",
            "description": "Chronic kidney disease stage 1 (2017-01-01), stage 2 (2017-09-03), and stage 3 (2018-09-30) are all simultaneously marked ACTIVE without resolving earlier stages.",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-4275-9febd44b35ce",
                "5693c080-f485-91e7-3c20-2954e787b490",
                "5693c080-f485-91e7-ac23-3ecc7b03578d",
            ],
        },
        {
            "anomaly_id": "maurice-seta-prediabetes-diabetes-conflict",
            "category": "mutually_exclusive_active_conditions",
            "severity": "medium",
            "description": "Prediabetes (1995-05-28) remains marked ACTIVE alongside established diabetic kidney disease and microalbuminuria/proteinuria due to type 2 diabetes mellitus.",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-387e-f0f4ab249a73",
                "5693c080-f485-91e7-368f-8ef38da4053f",
            ],
        },
        {
            "anomaly_id": "maurice-seta-live-zoster-repeat",
            "category": "acip_immunization_schedule_anomaly",
            "severity": "medium",
            "description": "2 doses of live zoster vaccine recorded on 2023-05-28 and 2024-04-28.",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-3664-af5ed6976651",
                "5693c080-f485-91e7-eb5b-3aa2e1d1bc35",
            ],
        },
        {
            "anomaly_id": "maurice-seta-sdoh-criminal-record-privacy",
            "category": "sdoh_privacy_sensitive_finding",
            "severity": "high",
            "description": "Active privacy-sensitive SDOH finding ('Has a criminal record (finding)', 2015-04-13) stored as an active Condition on the clinical problem list.",
            "expected_resource_uuids": [
                "5693c080-f485-91e7-02ce-a7601c6d6df7",
            ],
        },
    ],
}


def build_eval_cases() -> List[Dict[str, Any]]:
    """Return the 31 structured evaluation cases covering Use Cases A, B-1, B-2, and B-3."""
    cases: List[Dict[str, Any]] = []

    # --- USE CASE A: Reconciliation & Anomaly Detection (12 cases across Set A and Set B) ---
    for patient_key, anomalies in NATURAL_ANOMALIES_BY_PATIENT.items():
        for anom in anomalies:
            cases.append(
                {
                    "case_id": f"uc_a_{anom['anomaly_id']}",
                    "use_case": "A_reconcile",
                    "mode": "reconcile",
                    "dataset": "Set_A_Natural",
                    "patient_bundle": f"{patient_key}.json",
                    "question": "Audit this patient chart for clinical contradictions, duplicate medications or immunizations, outdated active problem list stages, and privacy-sensitive SDOH findings.",
                    "anomaly_id": anom["anomaly_id"],
                    "category": anom["category"],
                    "expected_resource_uuids": anom["expected_resource_uuids"],
                    "expected_description": anom["description"],
                }
            )

    set_b_specs = [
        (
            "Byron202_Emard19_multisource.json",
            "byron-setb-flu-iis-dup",
            "cross_source_immunization_duplicate",
            ["83093df7-a1e4-b827-159b-4271c06f5179", "imm-iis-byron-flu-2026"],
            "Detect duplicate Influenza immunization recorded 2 days apart (2026-01-26 clinic vs 2026-01-28 WA State IIS).",
        ),
        (
            "Byron202_Emard19_multisource.json",
            "byron-setb-covid-pharmacy-dup",
            "cross_source_immunization_duplicate",
            ["83093df7-a1e4-b827-cec7-5e37ae629c31", "imm-iis-byron-covid-2022"],
            "Detect duplicate COVID-19 mRNA immunization recorded 1 day apart (2022-01-17 clinic vs 2022-01-18 retail pharmacy).",
        ),
        (
            "Lamar193_Beer512_multisource.json",
            "lamar-setb-sinusitis-status-conflict",
            "cross_source_condition_status_conflict",
            ["4be6b5fb-2a19-943a-19f7-35a11a488712", "cond-ext-lamar-sinusitis-resolved"],
            "Detect cross-clinic status contradiction for Chronic sinusitis (marked ACTIVE since 2008-02-29 in PCP record vs RESOLVED on 2024-09-10 in specialty clinic).",
        ),
        (
            "Lamar193_Beer512_multisource.json",
            "lamar-setb-lisinopril-prinivil-dup",
            "cross_source_brand_generic_medication_duplicate",
            ["med-pcp-lamar-lisinopril", "med-cardio-lamar-prinivil"],
            "Detect brand vs generic duplicate active ACE-inhibitor prescriptions (lisinopril 10 MG vs Prinivil 10 MG).",
        ),
        (
            "Maurice742_Auer97_multisource.json",
            "maurice-setb-clopidogrel-plavix-dup",
            "cross_source_brand_generic_medication_duplicate",
            ["5693c080-f485-91e7-049d-0016c58b2394", "med-cardio-maurice-plavix"],
            "Detect brand vs generic duplicate active antiplatelet prescriptions (Clopidogrel 75 MG vs Plavix 75 MG).",
        ),
        (
            "Maurice742_Auer97_multisource.json",
            "maurice-setb-simvastatin-lipitor-dup",
            "cross_source_therapeutic_class_duplicate",
            ["5693c080-f485-91e7-aca9-a1fd06a66487", "med-cardio-maurice-lipitor"],
            "Detect concurrent active statin prescriptions (Simvastatin 20 MG vs Lipitor 40 MG [atorvastatin]).",
        ),
    ]
    for bundle_name, anom_id, cat, uuids, desc in set_b_specs:
        cases.append(
            {
                "case_id": f"uc_a_{anom_id}",
                "use_case": "A_reconcile",
                "mode": "reconcile",
                "dataset": "Set_B_MultiSource",
                "patient_bundle": bundle_name,
                "question": "Audit this multi-source patient chart for duplicate medications (including brand vs generic), duplicate immunizations across registries, and conflicting condition statuses.",
                "anomaly_id": anom_id,
                "category": cat,
                "expected_resource_uuids": uuids,
                "expected_description": desc,
            }
        )

    # --- USE CASE B-1: Encounter & Longitudinal Summarization (4 cases) ---
    cases.extend(
        [
            {
                "case_id": "uc_b1_maurice_cabg_icu_stay_2023",
                "use_case": "B1_encounter_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "sharing_posture": "patient_self",
                "question": "Summarize the patient's November 20–28, 2023 hospitalization, coronary artery bypass grafting (CABG) surgery, ICU stay, perioperative medications (including antibiotics), and discharge rehabilitation checks in plain language.",
                "expected_resource_uuids": [
                    "5693c080-f485-91e7-582f-e833229f53c0",  # Coronary artery bypass grafting (procedure) 2023-11-20
                    "5693c080-f485-91e7-f448-62985ae4c285",  # History of CABG (situation) 2023-11-20
                    "7cc9226f-0107-73ef-ed06-6dd1095f35ff",  # cefazolin 2000 MG Injection 2023-11-20
                    "5693c080-f485-91e7-df4a-bdb8522d08d3",  # ICU transfer encounter 2023-11-27
                    "5693c080-f485-91e7-c900-7f0ea458ca5e",  # Walking exercise test 2023-11-28
                ],
                "baseline_reference": "2023-11-20-maurice-hospital-stay.md (Bill Lober Claude Opus 5.5 baseline)",
            },
            {
                "case_id": "uc_b1_maurice_longitudinal_self",
                "use_case": "B1_longitudinal_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "sharing_posture": "patient_self",
                "question": "Provide a comprehensive patient-facing health summary of active conditions, current medications, devices, key lab trends, and any sensitive social history items I should be aware of before sharing my chart.",
                "expected_resource_uuids": [
                    "5693c080-f485-91e7-ac23-3ecc7b03578d",  # CKD stage 3
                    "5693c080-f485-91e7-368f-8ef38da4053f",  # Diabetic kidney disease
                    "5693c080-f485-91e7-049d-0016c58b2394",  # Clopidogrel 75 MG
                    "5693c080-f485-91e7-c785-40c0023e806d",  # Humulin 70/30 insulin
                    "5693c080-f485-91e7-02ce-a7601c6d6df7",  # Has a criminal record (SDOH/PRIVACY-SENSITIVE)
                ],
            },
            {
                "case_id": "uc_b1_maurice_longitudinal_external_share",
                "use_case": "B1_longitudinal_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "sharing_posture": "external_share",
                "question": "Provide a clinical health summary suitable for sharing with an external specialist, excluding redacted privacy-sensitive social findings.",
                "expected_resource_uuids": [
                    "5693c080-f485-91e7-ac23-3ecc7b03578d",
                    "5693c080-f485-91e7-049d-0016c58b2394",
                    "5693c080-f485-91e7-c785-40c0023e806d",
                ],
                "forbidden_resource_uuids": [
                    "5693c080-f485-91e7-02ce-a7601c6d6df7",  # Must be redacted under external_share
                ],
            },
            {
                "case_id": "uc_b1_lamar_longitudinal_self",
                "use_case": "B1_longitudinal_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Lamar193_Beer512.json",
                "sharing_posture": "patient_self",
                "question": "Summarize my active health conditions, devices, immunization history, and flag any sensitive personal/social findings recorded in my chart.",
                "expected_resource_uuids": [
                    "4be6b5fb-2a19-943a-7d72-3e9579eaf9c0",  # Prediabetes
                    "4be6b5fb-2a19-943a-be24-f21216f23853",  # Anemia
                    "4be6b5fb-2a19-943a-7617-5e43e4960522",  # Victim of intimate partner abuse
                    "4be6b5fb-2a19-943a-fa10-c5d0fe05e7f5",  # Live zoster 2nd dose
                ],
            },
        ]
    )

    # --- USE CASE B-2: Directed / Purpose-Driven Summaries (3 cases) ---
    cases.extend(
        [
            {
                "case_id": "uc_b2_maurice_directed_renal_diabetes",
                "use_case": "B2_directed_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "sharing_posture": "external_share",
                "question": "Generate a focused Endocrinology & Nephrology referral summary covering the patient's diabetes and kidney disease history, CKD staging discrepancies, longitudinal eGFR, Creatinine, and HbA1c trajectories, and current relevant medications.",
                "expected_resource_uuids": [
                    "5693c080-f485-91e7-ac23-3ecc7b03578d",  # CKD stage 3
                    "5693c080-f485-91e7-368f-8ef38da4053f",  # Disorder of kidney due to diabetes
                    "5693c080-f485-91e7-c785-40c0023e806d",  # Humulin 70/30 insulin
                    "5693c080-f485-91e7-6036-1d5f2bcbef75",  # Hydrochlorothiazide 25 MG
                ],
            },
            {
                "case_id": "uc_b2_byron_directed_school_immunizations",
                "use_case": "B2_directed_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Byron202_Emard19.json",
                "sharing_posture": "external_share",
                "question": "Generate a school & adolescent immunization summary for Byron listing all documented vaccines (HPV, Tdap, Meningococcal, COVID-19, Influenza) with dates and noting any schedule anomalies.",
                "expected_resource_uuids": [
                    "83093df7-a1e4-b827-9ba3-181c9443b46b",  # 3rd HPV dose
                    "83093df7-a1e4-b827-c2a2-c3faae510451",  # Tdap
                    "83093df7-a1e4-b827-b763-089cd8c363c6",  # Meningococcal MCV4P
                ],
            },
            {
                "case_id": "uc_b2_lamar_directed_sinus_dental",
                "use_case": "B2_directed_summary",
                "mode": "summary",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Lamar193_Beer512.json",
                "sharing_posture": "external_share",
                "question": "Generate a focused summary of Lamar's chronic sinusitis, loss of teeth, dental/denture devices, and related respiratory/ENT history.",
                "expected_resource_uuids": [
                    "4be6b5fb-2a19-943a-19f7-35a11a488712",  # Chronic sinusitis
                    "4be6b5fb-2a19-943a-752d-9383f84c8719",  # Loss of teeth
                ],
            },
        ]
    )

    # --- USE CASE B-3: Patient Q&A with Abstention Probes (12 cases: 8 Answerable + 4 Abstention) ---
    cases.extend(
        [
            {
                "case_id": "uc_b3_maurice_qa_egfr_trend",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "question": "How has my kidney function (eGFR and blood creatinine) changed from 2017 to 2026?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["124.48", "47.95", "2017-01-01", "2026-08-16", "down"],
            },
            {
                "case_id": "uc_b3_maurice_qa_cabg_antibiotic",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "question": "What antibiotic injection was I given during my coronary artery bypass grafting (CABG) hospitalization on 2023-11-20?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["cefazolin", "2000 MG", "2023-11-20"],
                "expected_resource_uuids": ["7cc9226f-0107-73ef-ed06-6dd1095f35ff"],
            },
            {
                "case_id": "uc_b3_maurice_qa_active_meds",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "question": "What are my currently active medications and when were they prescribed?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": [
                    "Clopidogrel",
                    "Simvastatin",
                    "metoprolol",
                    "Nitroglycerin",
                    "Humulin",
                    "Hydrochlorothiazide",
                ],
                "expected_resource_uuids": [
                    "5693c080-f485-91e7-049d-0016c58b2394",
                    "5693c080-f485-91e7-c785-40c0023e806d",
                ],
            },
            {
                "case_id": "uc_b3_maurice_qa_plavix_duplicate",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_B_MultiSource",
                "patient_bundle": "Maurice742_Auer97_multisource.json",
                "question": "Are there any duplicate blood thinner or cholesterol medications on my active medication list across my clinics?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["Clopidogrel", "Plavix", "Simvastatin", "Lipitor"],
                "expected_resource_uuids": [
                    "5693c080-f485-91e7-049d-0016c58b2394",
                    "med-cardio-maurice-plavix",
                    "5693c080-f485-91e7-aca9-a1fd06a66487",
                    "med-cardio-maurice-lipitor",
                ],
            },
            {
                "case_id": "uc_b3_lamar_qa_zoster_vaccines",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Lamar193_Beer512.json",
                "question": "When did I receive shingles (zoster) vaccines, and is there anything unusual about the doses recorded?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["2021-02-16", "2022-02-22", "zoster vaccine, live"],
                "expected_resource_uuids": [
                    "4be6b5fb-2a19-943a-d1d2-426855b8e379",
                    "4be6b5fb-2a19-943a-fa10-c5d0fe05e7f5",
                ],
            },
            {
                "case_id": "uc_b3_lamar_qa_sinusitis_conflict",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_B_MultiSource",
                "patient_bundle": "Lamar193_Beer512_multisource.json",
                "question": "Is my chronic sinusitis still active or has it resolved according to my records?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["2008-02-29", "2024-09-10", "active", "resolved"],
                "expected_resource_uuids": [
                    "4be6b5fb-2a19-943a-19f7-35a11a488712",
                    "cond-ext-lamar-sinusitis-resolved",
                ],
            },
            {
                "case_id": "uc_b3_byron_qa_hpv_doses",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Byron202_Emard19.json",
                "question": "How many HPV vaccine doses has Byron received, on what dates, and why is the third dose flagged?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["2020-12-28", "2022-01-03", "2022-05-02", "age 11", "2-dose"],
                "expected_resource_uuids": [
                    "83093df7-a1e4-b827-a187-1cceb00e687f",
                    "83093df7-a1e4-b827-ddc9-a5f99189d24e",
                    "83093df7-a1e4-b827-9ba3-181c9443b46b",
                ],
            },
            {
                "case_id": "uc_b3_byron_qa_latest_flu_duplicate",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "Set_B_MultiSource",
                "patient_bundle": "Byron202_Emard19_multisource.json",
                "question": "Were any of Byron's January 2026 flu shots entered twice across different reporting systems?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["2026-01-26", "2026-01-28", "WA State IIS"],
                "expected_resource_uuids": [
                    "83093df7-a1e4-b827-159b-4271c06f5179",
                    "imm-iis-byron-flu-2026",
                ],
            },
            # 4 Unanswerable Abstention Probes (testing closed-world guardrails)
            {
                "case_id": "uc_b3_maurice_qa_abstain_genetic_apoe",
                "use_case": "B3_qa_abstention",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "question": "What was the result of Maurice's APOE4 genetic test and brain MRI for Alzheimer's disease?",
                "expected_answer_type": "abstain_not_in_chart",
                "expected_facts": [],
            },
            {
                "case_id": "uc_b3_maurice_qa_abstain_drug_allergy",
                "use_case": "B3_qa_abstention",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Maurice742_Auer97.json",
                "question": "Which specific antibiotic caused Maurice's documented anaphylactic drug allergy?",
                "expected_answer_type": "abstain_not_in_chart",
                "expected_facts": [],
            },
            {
                "case_id": "uc_b3_lamar_qa_abstain_echo_ejection_fraction",
                "use_case": "B3_qa_abstention",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Lamar193_Beer512.json",
                "question": "What was Lamar's left ventricular ejection fraction (LVEF %) on his most recent echocardiogram?",
                "expected_answer_type": "abstain_not_in_chart",
                "expected_facts": [],
            },
            {
                "case_id": "uc_b3_byron_qa_abstain_peanut_allergy",
                "use_case": "B3_qa_abstention",
                "mode": "qa",
                "dataset": "Set_A_Natural",
                "patient_bundle": "Byron202_Emard19.json",
                "question": "What was Byron's peanut-specific IgE level and epinephrine auto-injector dose?",
                "expected_answer_type": "abstain_not_in_chart",
                "expected_facts": [],
            },
            # --- US Core / IPS Multi-Domain Clinical Archetype Cases (Elena, Arthur, Margaret) ---
            {
                "case_id": "uc_a_elena_cardiorenal_hyperkalemia_reconcile",
                "use_case": "A_reconcile",
                "mode": "reconcile",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Elena_Rostova_Cardiorenal.json",
                "question": "Audit Elena Rostova's cardiorenal chart for conflicting CKD stages, high-risk potassium/diuretic interactions, duplicate antiplatelet prescriptions, and discharge note hold contradictions.",
                "anomaly_id": "elena-hyperkalemia-and-ckd-reconcile",
                "category": "multi_domain_cardiorenal_reconciliation",
                "expected_resource_uuids": [
                    "mimic-cond-ckd2",
                    "mimic-cond-ckd3",
                    "mimic-med-aldactone",
                    "mimic-med-kcl",
                    "mimic-med-clopidogrel",
                    "mimic-med-plavix",
                    "mimic-note-discharge",
                ],
                "expected_description": "Detect CKD stage 2 vs 3B conflict, spironolactone + potassium chloride hyperkalemia hazard, Plavix vs Clopidogrel duplicate, and discharge note hold instruction.",
            },
            {
                "case_id": "uc_b3_elena_qa_potassium_spironolactone",
                "use_case": "B3_qa_answerable",
                "mode": "qa",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Elena_Rostova_Cardiorenal.json",
                "question": "Why did the hospital discharge note say to hold spironolactone and potassium chloride, and what were Elena's potassium and eGFR levels?",
                "expected_answer_type": "grounded_answer",
                "expected_facts": ["Aldactone", "Potassium Chloride", "5.7", "31.0"],
                "expected_resource_uuids": [
                    "mimic-med-aldactone",
                    "mimic-med-kcl",
                    "mimic-note-discharge",
                ],
            },
            {
                "case_id": "uc_a_arthur_anticoag_bleed_reconcile",
                "use_case": "A_reconcile",
                "mode": "reconcile",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Arthur_Pendelton_Anticoag.json",
                "question": "Audit Arthur Pendelton's chart for dual oral anticoagulation, NSAID bleeding interactions, active vs resolved GI hemorrhage status conflicts, and discharge note medication stops.",
                "anomaly_id": "arthur-dual-anticoag-and-gib-reconcile",
                "category": "multi_domain_anticoagulation_reconciliation",
                "expected_resource_uuids": [
                    "arthur-med-eliquis",
                    "arthur-med-warfarin",
                    "arthur-med-naproxen",
                    "arthur-cond-gib-active",
                    "arthur-cond-gib-resolved",
                    "arthur-note-discharge",
                ],
                "expected_description": "Detect concurrent Eliquis + Warfarin dual oral anticoagulation, Naproxen NSAID bleeding hazard, active vs resolved GI hemorrhage, and discharge note stop instruction.",
            },
            {
                "case_id": "uc_b1_arthur_gi_bleed_encounter_summary",
                "use_case": "B1_encounter_summary",
                "mode": "summary",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Arthur_Pendelton_Anticoag.json",
                "sharing_posture": "patient_self",
                "question": "Summarize Arthur Pendelton's February 2026 hospitalization for gastrointestinal hemorrhage, endoscopic treatment, mechanical aortic valve anticoagulation, and hemoglobin/INR trends.",
                "expected_resource_uuids": [
                    "arthur-enc-gib-admit",
                    "arthur-proc-egd",
                    "arthur-dev-valve",
                    "arthur-med-warfarin",
                ],
            },
            {
                "case_id": "uc_a_margaret_geriatric_polypharmacy_reconcile",
                "use_case": "A_reconcile",
                "mode": "reconcile",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Margaret_Chen_Polypharmacy.json",
                "question": "Audit Margaret Chen's geriatric chart for Beers Criteria sedative-hypnotic duplication, Prediabetes vs Type 2 Diabetes conflicts, duplicate pneumococcal vaccines, and note discontinuation orders.",
                "anomaly_id": "margaret-beers-sedative-and-glycemic-reconcile",
                "category": "multi_domain_geriatric_polypharmacy_reconciliation",
                "expected_resource_uuids": [
                    "smart-cond-predm",
                    "smart-cond-t2dm",
                    "margaret-med-ativan",
                    "margaret-med-ambien",
                    "margaret-imm-pneumo-clinic",
                    "margaret-imm-pneumo-iis",
                    "margaret-note-geriatrics",
                ],
                "expected_description": "Detect Ativan + Ambien Beers Criteria sedative duplication, Prediabetes + T2DM stage conflict, duplicate PCV20 pneumococcal vaccine, and note stop order.",
            },
            {
                "case_id": "uc_b2_margaret_external_share_privacy",
                "use_case": "B2_directed_summary",
                "mode": "summary",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Margaret_Chen_Polypharmacy.json",
                "sharing_posture": "external_share",
                "question": "Generate an external physical therapy & fall-prevention referral summary for Margaret Chen, ensuring privacy-sensitive abuse and social isolation records are excluded.",
                "expected_resource_uuids": [
                    "smart-cond-t2dm",
                    "margaret-cond-falls",
                    "margaret-dev-walker",
                ],
                "forbidden_resource_uuids": [
                    "smart-cond-sdoh-ipv",
                    "margaret-cond-sdoh-iso",
                ],
            },
            {
                "case_id": "uc_b3_arthur_qa_abstain_colonoscopy",
                "use_case": "B3_qa_abstention",
                "mode": "qa",
                "dataset": "US_Core_Archetypes",
                "patient_bundle": "Arthur_Pendelton_Anticoag.json",
                "question": "What did Arthur Pendelton's colonoscopy polyp biopsy pathology report show?",
                "expected_answer_type": "abstain_not_in_chart",
                "expected_facts": [],
            },
        ]
    )


    for case in cases:
        _enrich_case_with_rubric_and_entailment(case)

    return cases


def _enrich_case_with_rubric_and_entailment(case: Dict[str, Any]) -> None:
    """Attach HealthBench-style weighted criteria, VeriFact atomic entailment assertions,
    question horizon, and patient question category to each evaluation case."""
    cid = str(case.get("case_id") or "")
    uc = str(case.get("use_case") or "")
    cat = str(case.get("category") or "")

    # 1. Assign question_horizon (for Slice 4 condensation & retrieval ablation)
    if "cabg" in cid or "antibiotic" in cid:
        case.setdefault("question_horizon", "full_text_note")
    elif "egfr" in cid or "zoster" in cid or "hpv" in cid or "sinusitis" in cid:
        case.setdefault("question_horizon", "trend_24m")
    elif uc == "B3_qa_abstention":
        case.setdefault("question_horizon", "deep_historical")
    else:
        case.setdefault("question_horizon", "current_active_state")

    # 2. Assign patient_category (6-category Patient Question Taxonomy)
    if "plavix" in cid or "simvastatin" in cid or "lisinopril" in cid or "active_meds" in cid:
        case.setdefault("patient_category", "medication_reconciliation")
    elif "ckd" in cid or "prediabetes" in cid or "egfr" in cid or "renal" in cid:
        case.setdefault("patient_category", "lab_trends")
    elif "cabg" in cid:
        case.setdefault("patient_category", "hospital_icu_stay")
    elif "flu" in cid or "covid" in cid or "hpv" in cid or "zoster" in cid or "school" in cid:
        case.setdefault("patient_category", "immunization_readiness")
    elif "sdoh" in cid or case.get("sharing_posture") == "external_share":
        case.setdefault("patient_category", "privacy_sharing_preview")
    else:
        case.setdefault("patient_category", "next_visit_checklist")

    # 3. Build positive_rubric_items and negative_penalty_items
    facts = [str(f) for f in (case.get("expected_facts") or []) if str(f).strip()]
    pos_items: List[Dict[str, Any]] = []
    for idx, fact in enumerate(facts):
        pos_items.append(
            {
                "id": f"{cid}_fact_{idx + 1}",
                "criterion": f"Grounds clinical fact '{fact}' from FHIR bundle",
                "required_any": [fact],
                "weight": 1.5 if idx == 0 else 1.0,
            }
        )

    neg_items: List[Dict[str, Any]] = []
    entailment_items: List[Dict[str, Any]] = []
    exp_uuids = [str(u) for u in (case.get("expected_resource_uuids") or [])]

    if cid == "uc_b3_maurice_qa_egfr_trend":
        neg_items.append(
            {
                "id": "neg_egfr_improved",
                "criterion": "Must not claim eGFR improved from 2017 to 2026",
                "forbidden_any": ["egfr improved from 2017", "kidney function improved overall"],
                "penalty": 0.5,
            }
        )
        entailment_items.append(
            {
                "resource_uuid": "",
                "must_mention_any": ["124.48", "47.95"],
                "must_not_distort": ["egfr 95.0", "egfr 110.0"],
            }
        )
    elif cid == "uc_b3_maurice_qa_cabg_antibiotic":
        entailment_items.append(
            {
                "resource_uuid": "7cc9226f-0107-73ef-ed06-6dd1095f35ff",
                "must_mention_any": ["cefazolin", "2000 mg"],
                "must_not_distort": ["vancomycin 1000 mg", "piperacillin"],
            }
        )
    elif cid == "uc_b3_maurice_qa_plavix_duplicate":
        entailment_items.append(
            {
                "resource_uuid": "med-cardio-maurice-plavix",
                "must_mention_any": ["plavix", "clopidogrel"],
                "must_not_distort": ["no duplicate medications", "no active blood thinners"],
            }
        )
    elif cid == "uc_b3_lamar_qa_sinusitis_conflict":
        entailment_items.append(
            {
                "resource_uuid": "cond-ext-lamar-sinusitis-resolved",
                "must_mention_any": ["resolved", "2024-09-10"],
                "must_not_distort": ["no conflict", "never had sinusitis"],
            }
        )
    elif exp_uuids and uc != "B3_qa_abstention":
        entailment_items.append(
            {
                "resource_uuid": exp_uuids[0],
                "must_mention_any": facts[:1] if facts else [],
                "must_not_distort": [],
            }
        )

    case.setdefault("positive_rubric_items", pos_items)
    case.setdefault("negative_penalty_items", neg_items)
    case.setdefault("citation_entailment_assertions", entailment_items)


def inject_messy_ehr_perturbations(
    multisource_bundle: Mapping[str, Any], patient_stem: str
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Pathway A (Set C — Pragmatic Messy Multi-Source):
    1. Deterministically strips standard SNOMED/RxNorm/CVX/LOINC `coding` arrays on ~25% of
       clinical entries (leaving only `text` or local EHR descriptions).
    2. Injects a free-text `DocumentReference` clinical note that contradicts an active
       `MedicationRequest.status == 'active'` (note-vs-structured hold/discontinue conflict).
    """
    messy = copy.deepcopy(dict(multisource_bundle))
    entries = list(messy.get("entry") or [])
    patient_id = "patient-1"
    clinical_idx = 0

    for e in entries:
        res = (e or {}).get("resource") or {}
        rtype = str(res.get("resourceType") or "")
        if rtype == "Patient" and res.get("id"):
            patient_id = str(res["id"])
        if rtype in {"Condition", "MedicationRequest", "Immunization", "Observation"}:
            clinical_idx += 1
            if clinical_idx % 4 == 0:
                for field_name in ("code", "medicationCodeableConcept", "vaccineCode"):
                    codeable = res.get(field_name)
                    if isinstance(codeable, dict):
                        codings = codeable.get("coding") or []
                        fallback_txt = str(codeable.get("text") or "").strip()
                        if not fallback_txt and codings and isinstance(codings[0], dict):
                            fallback_txt = str(codings[0].get("display") or codings[0].get("code") or "")
                        if fallback_txt:
                            codeable["text"] = fallback_txt
                            codeable["coding"] = []

    if patient_stem == "Byron202_Emard19":
        entries.append(
            {
                "fullUrl": "urn:uuid:med-messy-byron-cetirizine",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-messy-byron-cetirizine",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "text": "cetirizine hydrochloride 10 MG Oral Tablet"
                    },
                    "subject": {"reference": f"urn:uuid:{patient_id}"},
                    "authoredOn": "2026-01-15T10:00:00+00:00",
                    "requester": {"display": "Pediatric Primary Care Clinic"},
                },
            }
        )

    note_specs = {
        "Maurice742_Auer97": (
            "note-messy-maurice-hold-simvastatin",
            "2026-06-10T15:00:00+00:00",
            "# Chief Complaint\nPost-Cardiology Medication Review\n# Assessment and Plan\nDiscontinue simvastatin immediately while patient is taking Lipitor 40 MG daily to prevent statin myopathy.",
            "5693c080-f485-91e7-aca9-a1fd06a66487",
            "Clinical note (2026-06-10) instructs to discontinue simvastatin, while structured MedicationRequest remains ACTIVE.",
        ),
        "Lamar193_Beer512": (
            "note-messy-lamar-hold-lisinopril",
            "2026-05-20T11:00:00+00:00",
            "# Chief Complaint\nFollow-up Hypertension & Renal Lab Check\n# Assessment and Plan\nHold lisinopril until potassium and renal panel are rechecked next week; patient already filled Prinivil from cardiology.",
            "med-pcp-lamar-lisinopril",
            "Clinical note (2026-05-20) instructs to hold lisinopril, while structured MedicationRequest remains ACTIVE.",
        ),
        "Byron202_Emard19": (
            "note-messy-byron-hold-cetirizine",
            "2026-02-01T09:30:00+00:00",
            "# Chief Complaint\nAllergy Clinic Pre-Visit Note\n# Assessment and Plan\nHold cetirizine for 7 days prior to allergy skin testing panel.",
            "med-messy-byron-cetirizine",
            "Clinical note (2026-02-01) instructs to hold cetirizine prior to allergy testing.",
        ),
    }
    note_id, note_dt, note_body, linked_uuid, note_desc = note_specs[patient_stem]
    b64_payload = base64.b64encode(note_body.encode("utf-8")).decode("ascii")
    entries.append(
        {
            "fullUrl": f"urn:uuid:{note_id}",
            "resource": {
                "resourceType": "DocumentReference",
                "id": note_id,
                "status": "current",
                "type": {"text": "Outpatient Progress Note (Unstructured Text)"},
                "subject": {"reference": f"urn:uuid:{patient_id}"},
                "date": note_dt,
                "author": [{"display": "Regional HIE Care Coordination Note"}],
                "content": [
                    {
                        "attachment": {
                            "contentType": "text/plain; charset=utf-8",
                            "data": b64_payload,
                        }
                    }
                ],
            },
        }
    )
    messy["entry"] = entries
    messy_anomalies = [
        {
            "anomaly_id": f"{patient_stem.lower()}-setc-note-med-contradiction",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": note_desc,
            "expected_resource_uuids": [note_id, linked_uuid],
        }
    ]
    return messy, messy_anomalies


def generate_wahs_eval_suite(source_dir: Path, output_dir: Path) -> Dict[str, Any]:
    """Generate Set B multi-source & Set C messy FHIR Bundles, de-identified real clinical summaries,
    `ground_truth.json`, and `wahs_eval_cases.jsonl`."""
    output_dir.mkdir(parents=True, exist_ok=True)

    perturb_fns = {
        "Byron202_Emard19": inject_byron_multisource_perturbations,
        "Lamar193_Beer512": inject_lamar_multisource_perturbations,
        "Maurice742_Auer97": inject_maurice_multisource_perturbations,
    }

    bundles_manifest: Dict[str, Any] = {}
    for patient_stem, perturb_fn in perturb_fns.items():
        src_path = source_dir / f"{patient_stem}.json"
        if not src_path.exists():
            raise FileNotFoundError(f"Missing source bundle: {src_path}")
        natural_bundle = json.loads(src_path.read_text(encoding="utf-8"))
        perturbed_bundle, injected_anomalies = perturb_fn(natural_bundle)
        messy_bundle, messy_anomalies = inject_messy_ehr_perturbations(
            perturbed_bundle, patient_stem
        )

        out_bundle_path = output_dir / f"{patient_stem}_multisource.json"
        out_bundle_path.write_text(
            json.dumps(perturbed_bundle, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )
        out_messy_path = output_dir / f"{patient_stem}_messy.json"
        out_messy_path.write_text(
            json.dumps(messy_bundle, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )

        # Verify that Set A, Set B, and Set C preserve 100% of ground-truth anomaly UUIDs across condensation budgets
        nat_anomalies = NATURAL_ANOMALIES_BY_PATIENT[patient_stem]
        all_set_b_anomalies = nat_anomalies + injected_anomalies
        all_set_c_anomalies = all_set_b_anomalies + messy_anomalies

        for label, b_obj, anom_list in (
            (f"{patient_stem}.json", natural_bundle, nat_anomalies),
            (f"{patient_stem}_multisource.json", perturbed_bundle, all_set_b_anomalies),
            (f"{patient_stem}_messy.json", messy_bundle, all_set_c_anomalies),
        ):
            ext = extract_bundle(b_obj)
            cov = audit_cohort_code_coverage(ext)
            raw_bytes = json.dumps(b_obj, sort_keys=True).encode("utf-8")
            sha256_hex = hashlib.sha256(raw_bytes).hexdigest()
            required_uuids = {
                u for a in anom_list for u in a["expected_resource_uuids"]
            }
            budget_checks: Dict[str, Any] = {}
            for budget in (16384, 8192, 4096, 2048):
                res = condense_bundle_to_budget(ext, target_tokens=budget)
                present = {
                    u
                    for m in res.mappings
                    for u in ([m.get("resourceUuid")] + list(m.get("memberUuids") or []))
                    if u
                }
                missing = sorted(required_uuids - present)
                if missing:
                    raise AssertionError(
                        f"Bundle {label} at budget {budget} lost ground-truth anomaly UUIDs: {missing}"
                    )
                budget_checks[str(budget)] = {
                    "tier": res.tier,
                    "actual_tokens": res.actual_tokens,
                    "rendered_lines": len(res.records),
                    "all_ground_truth_uuids_present": True,
                }
            bundles_manifest[label] = {
                "sha256": sha256_hex,
                "total_bundle_entries": ext.total_bundle_entries,
                "extracted_clinical_items": len(ext.items),
                "code_coverage": {
                    "precoded_pct": cov["precoded_pct"],
                    "text_fallback_recovered_pct": cov["text_fallback_recovered_pct"],
                    "overall_resolution_pct": round(cov["resolution_rate"] * 100.0, 1),
                },
                "anomalies": anom_list,
                "budget_verification": budget_checks,
            }

    # Pathway B: De-identified real clinical summary fixtures
    for real_label, (real_bundle, real_anoms) in build_deidentified_real_clinical_summaries().items():
        real_path = output_dir / real_label
        real_path.write_text(json.dumps(real_bundle, indent=2, sort_keys=False) + "\n", encoding="utf-8")
        ext_r = extract_bundle(real_bundle)
        cov_r = audit_cohort_code_coverage(ext_r)
        sha_r = hashlib.sha256(json.dumps(real_bundle, sort_keys=True).encode("utf-8")).hexdigest()
        req_r = {u for a in real_anoms for u in a["expected_resource_uuids"]}
        b_checks_r: Dict[str, Any] = {}
        for budget in (16384, 8192, 4096, 2048):
            res_r = condense_bundle_to_budget(ext_r, target_tokens=budget)
            present_r = {
                u
                for m in res_r.mappings
                for u in ([m.get("resourceUuid")] + list(m.get("memberUuids") or []))
                if u
            }
            missing_r = sorted(req_r - present_r)
            if missing_r:
                raise AssertionError(f"Real clinical summary {real_label} lost UUIDs: {missing_r}")
            b_checks_r[str(budget)] = {
                "tier": res_r.tier,
                "actual_tokens": res_r.actual_tokens,
                "rendered_lines": len(res_r.records),
                "all_ground_truth_uuids_present": True,
            }
        bundles_manifest[real_label] = {
            "sha256": sha_r,
            "total_bundle_entries": ext_r.total_bundle_entries,
            "extracted_clinical_items": len(ext_r.items),
            "code_coverage": {
                "precoded_pct": cov_r["precoded_pct"],
                "text_fallback_recovered_pct": cov_r["text_fallback_recovered_pct"],
                "overall_resolution_pct": round(cov_r["resolution_rate"] * 100.0, 1),
            },
            "anomalies": real_anoms,
            "budget_verification": b_checks_r,
        }

    eval_cases = build_eval_cases()
    cases_path = output_dir / "wahs_eval_cases.jsonl"
    with cases_path.open("w", encoding="utf-8") as fh:
        for case in eval_cases:
            fh.write(json.dumps(case, sort_keys=False) + "\n")

    ground_truth = {
        "schema_version": "wahs_eval_suite.v1",
        "total_bundles": len(bundles_manifest),
        "total_eval_cases": len(eval_cases),
        "use_case_counts": {
            "A_reconcile": sum(1 for c in eval_cases if c["use_case"] == "A_reconcile"),
            "B1_summary": sum(1 for c in eval_cases if c["use_case"].startswith("B1_")),
            "B2_directed_summary": sum(1 for c in eval_cases if c["use_case"] == "B2_directed_summary"),
            "B3_qa_answerable": sum(1 for c in eval_cases if c["use_case"] == "B3_qa_answerable"),
            "B3_qa_abstention": sum(1 for c in eval_cases if c["use_case"] == "B3_qa_abstention"),
        },
        "bundles": bundles_manifest,
    }
    gt_path = output_dir / "ground_truth.json"
    gt_path.write_text(json.dumps(ground_truth, indent=2) + "\n", encoding="utf-8")
    return ground_truth


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build WAHS Set A / Set B evaluation fixtures and ground truth.")
    parser.add_argument("--source-dir", type=Path, required=True, help="Directory containing Byron, Lamar, and Maurice FHIR JSON bundles.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory to write perturbed bundles, ground_truth.json, and wahs_eval_cases.jsonl.")
    args = parser.parse_args(argv)
    gt = generate_wahs_eval_suite(args.source_dir, args.output_dir)
    print(
        f"Generated {gt['total_bundles']} bundle manifests and {gt['total_eval_cases']} evaluation cases "
        f"({gt['use_case_counts']}) in {args.output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
