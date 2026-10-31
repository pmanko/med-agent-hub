"""Interactive WAHS+ Demo Dataset Exporter (`window.WAHS_DEMO_DATA`).

Separates static interactive demo payload construction (`PERSONA_BIOS`,
`QUESTIONS_BY_PATIENT`, `build_demo_patients_payload`, and `write_wahs_demo_data_js`)
from the cohort evaluation runner and Markdown report renderer in
`scripts/generate_wahs_eval_report.py`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from server.fhir import (
    analyze_data_quality_and_conflicts,
    analyze_duplicates_and_bloat,
    analyze_patterns_and_summaries,
    answer_wahs_question,
    build_patient_skills_and_condensation_matrix,
    condense_bundle_to_budget,
    extract_bundle_lossless,
    run_chart_hygiene_and_reconciliation_skills,
)

PRIMARY_PERSONA_IDS: List[str] = [
    "Maurice742_Auer97_set_b",
    "Elena_Rostova_Cardiorenal_set_b",
    "Arthur_Pendelton_Anticoag_set_b",
    "Margaret_Chen_Polypharmacy_set_b",
    "Lamar193_Beer512_set_b",
    "Byron202_Emard19_set_b",
]

QUESTIONS_BY_PATIENT: Dict[str, List[str]] = {
    "default": [
        "What duplicate medications or dangerous drug conflicts are on my active list?",
        "Why are there conflicting condition stages or clinical note hold contradictions, and what are my lab trends?",
        "Summarize my most recent hospital, emergency, or high-acuity procedural encounter.",
        "Are there any duplicate vaccines or ACIP immunization schedule anomalies?",
        "What duplicate lab panels or repeated notes were consolidated in this chart?",
        "Does this patient have a documented Penicillin allergy or recent colonoscopy?",
    ],
    "Elena_Rostova_Cardiorenal": [
        "Why did the hospital discharge note say to hold spironolactone and potassium chloride, and what were Elena's potassium and eGFR levels?",
        "What duplicate medications or dangerous drug conflicts are on my active list?",
        "Why are there conflicting Chronic Kidney Disease stages and what is my eGFR trend?",
        "Summarize my most recent hospital, emergency, or high-acuity procedural encounter.",
        "What duplicate lab panels or repeated notes were consolidated in this chart?",
        "Does this patient have a documented Penicillin allergy or recent colonoscopy?",
    ],
    "Arthur_Pendelton_Anticoag": [
        "Why is Arthur prescribed both Eliquis and Warfarin alongside Naproxen, and what did the discharge note instruct?",
        "Is Arthur's gastrointestinal hemorrhage still active or resolved across his clinic records?",
        "Summarize Arthur's hospitalization for gastrointestinal hemorrhage, EGD procedure, mechanical valve, and hemoglobin/INR trends.",
        "What duplicate medications or dangerous drug conflicts are on my active list?",
        "What duplicate lab panels or repeated notes were consolidated in this chart?",
        "What did Arthur Pendelton's colonoscopy polyp biopsy pathology report show?",
    ],
    "Margaret_Chen_Polypharmacy": [
        "Why are Ativan and Ambien flagged on Margaret's medication list, and what did the geriatrics note say about her falls?",
        "Why does Margaret's chart list both Prediabetes and Type 2 Diabetes as active, and what is her HbA1c?",
        "Were any of Margaret's pneumococcal vaccines entered twice across the clinic and WA State IIS registry?",
        "Provide a safe external sharing summary for physical therapy that protects sensitive abuse and social isolation records.",
        "Summarize my most recent hospital, emergency, or high-acuity procedural encounter.",
        "Does this patient have a documented Penicillin allergy or recent colonoscopy?",
    ],
}

PERSONA_BIOS: Dict[str, Dict[str, str]] = {
    "Maurice742_Auer97": {
        "short_tag": "Complex Post-CABG & Renal Progression (18.8 MB Chart)",
        "persona_bio": (
            "Older adult following triple coronary bypass (CABG) surgery with chronic kidney disease, "
            "multi-clinic prescriptions (brand vs. generic Plavix/clopidogrel, duplicate statins), "
            "1,599 raw FHIR entries, and clinic room-equipment export artifacts."
        ),
        "key_highlights": "CABG Surgery • CKD Stages 1–3 • Brand/Generic Plavix • 1,599 Raw FHIR Entries",
    },
    "Elena_Rostova_Cardiorenal": {
        "short_tag": "Heart Failure & Cardiorenal Hospital Discharge",
        "persona_bio": (
            "73-year-old woman after a heart failure & kidney admission where the hospital discharge note "
            "instructed to HOLD spironolactone and potassium due to high potassium (5.8 mEq/L), "
            "while outpatient clinic orders still list both pills as active."
        ),
        "key_highlights": "Discharge Note 'Hold' Mismatch • Hyperkalemia Risk • CKD Stage 3a vs 3b",
    },
    "Arthur_Pendelton_Anticoag": {
        "short_tag": "Mechanical Heart Valve, GI Bleed & Blood Thinners",
        "persona_bio": (
            "67-year-old man with a mechanical aortic valve and recent upper GI bleed hospitalization "
            "whose multi-clinic medication list carries overlapping blood thinners (Eliquis + Warfarin) "
            "alongside an active NSAID (Naproxen)."
        ),
        "key_highlights": "Eliquis + Warfarin + Naproxen • Mechanical Valve • GI Bleed Status Conflict",
    },
    "Margaret_Chen_Polypharmacy": {
        "short_tag": "Geriatric Fall Risk, Diabetes & Privacy-Sensitive Care",
        "persona_bio": (
            "80-year-old woman evaluated after a fall with two overlapping sedative-hypnotic prescriptions "
            "(Ativan + Ambien), concurrent Prediabetes and Type 2 Diabetes on her problem list, "
            "duplicate registry vaccines, and sensitive social-safety records."
        ),
        "key_highlights": "Ativan + Ambien Fall Risk • Prediabetes + T2DM • Sensitive SDOH Preview",
    },
    "Lamar193_Beer512": {
        "short_tag": "Adult Preventive Care, Sinusitis & Multi-Clinic Records",
        "persona_bio": (
            "Middle-aged adult with longitudinal primary-care and urgent-care visits, cross-clinic "
            "chronic sinusitis status discrepancies, NSAID + ACE-inhibitor prescriptions, and "
            "state immunization registry sync entries."
        ),
        "key_highlights": "Active vs. Resolved Sinusitis • Lisinopril + Ibuprofen • WA IIS Sync",
    },
    "Byron202_Emard19": {
        "short_tag": "Adolescent / Young Adult Well-Child & Vaccine Series",
        "persona_bio": (
            "Young patient with longitudinal pediatric/adolescent growth curves, multi-dose vaccine "
            "series (HPV, Influenza), repeated wellness panels, and social/school readiness records."
        ),
        "key_highlights": "HPV & Flu Vaccine Series • Pediatric Growth Trends • School Readiness",
    },
}


def build_demo_patients_payload(
    synthea_patients: Sequence[str],
    us_core_patients: Sequence[str],
    bundles_set_a: Mapping[str, Dict[str, Any]],
    bundles_set_b: Mapping[str, Dict[str, Any]],
    bundles_set_c: Mapping[str, Dict[str, Any]],
    raw_bytes_a: Mapping[str, int],
    raw_tokens_a: Mapping[str, int],
) -> Dict[str, Any]:
    """Build precomputed interactive demo payload for all 6 patients across Set B, Set C, and Set A."""
    demo_patients: Dict[str, Any] = {}
    all_demo_patients = list(synthea_patients) + list(us_core_patients)

    for p in all_demo_patients:
        is_us_core = p in us_core_patients
        patient_questions = QUESTIONS_BY_PATIENT.get(p, QUESTIONS_BY_PATIENT["default"])
        ds_variants = (
            [("set_b", "US Core / IPS Multi-Clinic Transition", bundles_set_b)]
            if is_us_core
            else [
                ("set_b", "Set B (Multi-Source HIE + WA IIS + Urgent Care)", bundles_set_b),
                ("set_c", "Set C (Messy Uncoded + Note Contradictions)", bundles_set_c),
                ("set_a", "Set A (Natural Synthea Baseline)", bundles_set_a),
            ]
        )
        for ds_key, ds_label, b_map in ds_variants:
            combo_id = f"{p}_{ds_key}"
            bundle = b_map[p]
            extracted = extract_bundle_lossless(bundle)
            postures_data: Dict[str, Any] = {}
            for posture in ("patient_self", "external_share"):
                cond_8k = condense_bundle_to_budget(
                    extracted,
                    target_tokens=8192,
                    sharing_posture=posture,
                    condense_mode="lossless_structural",
                )
                cond_2k = condense_bundle_to_budget(
                    extracted,
                    target_tokens=2048,
                    sharing_posture=posture,
                    condense_mode="lossless_structural",
                )
                bloat = analyze_duplicates_and_bloat(
                    extracted, cond_8k, target_tokens=8192, sharing_posture=posture
                )
                dq = analyze_data_quality_and_conflicts(
                    extracted, cond_8k, target_tokens=8192, sharing_posture=posture
                )
                patterns = analyze_patterns_and_summaries(
                    extracted, cond_8k, target_tokens=8192, sharing_posture=posture
                )
                skill_report = run_chart_hygiene_and_reconciliation_skills(
                    extracted, mappings=cond_8k.mappings
                )
                qa_samples = [
                    answer_wahs_question(
                        extracted, q, cond_8k, target_tokens=8192, sharing_posture=posture
                    )
                    for q in patient_questions
                ]
                postures_data[posture] = {
                    "tier_8k": {
                        "tier": cond_8k.tier,
                        "tier_name": cond_8k.tier_name,
                        "condense_mode": "lossless_structural",
                        "target_tokens": 8192,
                        "actual_tokens": cond_8k.actual_tokens,
                        "ledger_lines": len(cond_8k.records),
                        "chart_text": cond_8k.chart_text,
                        "mappings": [
                            {
                                "index": m.get("index"),
                                "resourceType": m.get("resourceType"),
                                "resourceUuid": m.get("resourceUuid"),
                                "memberCount": len(m.get("memberUuids") or []),
                            }
                            for m in cond_8k.mappings[:120]
                        ],
                        "redacted_count": len(cond_8k.redacted_for_privacy),
                    },
                    "tier_2k": {
                        "tier": cond_2k.tier,
                        "tier_name": cond_2k.tier_name,
                        "condense_mode": "lossless_structural",
                        "target_tokens": 2048,
                        "actual_tokens": cond_2k.actual_tokens,
                        "ledger_lines": len(cond_2k.records),
                        "chart_text": cond_2k.chart_text,
                    },
                    "duplicates_and_bloat": bloat,
                    "data_quality_and_conflicts": dq,
                    "patterns_and_summaries": patterns,
                    "skill_findings": skill_report,
                    "qa_samples": qa_samples,
                }

            p_bio = PERSONA_BIOS.get(p, {})
            w15_ws = (
                build_patient_skills_and_condensation_matrix(
                    bundle, raw_json_tokens=raw_tokens_a[p]
                )
                if ds_key == "set_b"
                else None
            )
            demo_patients[combo_id] = {
                "id": combo_id,
                "patient_id": p,
                "patient_name": extracted.patient_name,
                "birth_date": extracted.birth_date,
                "reference_date": extracted.reference_date,
                "cohort_type": "us_core_archetype" if is_us_core else "ehignite_synthea",
                "dataset_key": ds_key,
                "dataset_label": ds_label,
                "short_tag": p_bio.get("short_tag", ds_label),
                "persona_bio": p_bio.get("persona_bio", ""),
                "key_highlights": p_bio.get("key_highlights", ""),
                "raw_mb": round(max(raw_bytes_a[p] / (1024 * 1024), 0.01), 2),
                "raw_est_tokens": raw_tokens_a[p],
                "total_bundle_entries": extracted.total_bundle_entries,
                "postures": postures_data,
                **(
                    {
                        "fhir_browser": w15_ws["fhir_browser"],
                        "prep_lever_summary": w15_ws["prep_lever_summary"],
                        "condensation_matrix": w15_ws["condensation_matrix"],
                        "skills": w15_ws["skills"],
                    }
                    if w15_ws is not None
                    else {}
                ),
            }

    return demo_patients


def write_wahs_demo_data_js(results: Mapping[str, Any], demo_data_out: Path) -> int:
    """Serialize `window.WAHS_DEMO_DATA` static JavaScript file for the 6 primary patient personas."""
    demo_data_out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": "2026-10-31",
        "cohort_size": len(PRIMARY_PERSONA_IDS),
        "primary_persona_ids": PRIMARY_PERSONA_IDS,
        "code_coverage": results["code_coverage"],
        "ablation_rows": results["ablation_rows"],
        "patients": {
            pid: results["demo_patients"][pid]
            for pid in PRIMARY_PERSONA_IDS
            if pid in results["demo_patients"]
        },
    }
    js_content = (
        "// Precomputed WAHS+ Phase 2 Evaluation & Interactive Demo Dataset\n"
        "window.WAHS_DEMO_DATA = "
        + json.dumps(payload, separators=(",", ":"))
        + ";\n"
    )
    demo_data_out.write_text(js_content, encoding="utf-8")
    return len(js_content)


__all__ = [
    "PERSONA_BIOS",
    "PRIMARY_PERSONA_IDS",
    "QUESTIONS_BY_PATIENT",
    "build_demo_patients_payload",
    "write_wahs_demo_data_js",
]
