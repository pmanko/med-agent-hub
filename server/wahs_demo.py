"""FastAPI router and interactive sample UX for Washington Health Summary (WAHS+).

Exposes:
- `GET /wahs/demo`: Interactive single-page UX for Suggested Analyses (Duplicate Labs & Bloat,
  Data Quality & Conflicts, Longitudinal Patterns & Encounter Summaries) and Grounded Chat
- `GET /wahs/api/patients`: Available patient charts (built-in multi-source fixtures + optional
  `WAHS_BUNDLE_DIR` directory scan)
- `POST /wahs/api/analyze`: Runs all 3 Suggested Analyses + 5-Tier FHIR condensation at a chosen
  token budget (`target_tokens`) and privacy posture (`patient_self` | `external_share`)
- `POST /wahs/api/chat`: Grounded Q&A over a patient chart with `[N] -> FHIR resourceUuid`
  provenance and deterministic abstention on unrecorded clinical facts
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .fhir import (
    analyze_data_quality_and_conflicts,
    analyze_duplicates_and_bloat,
    analyze_patterns_and_summaries,
    answer_wahs_question,
    condense_bundle_to_budget,
    extract_bundle,
    run_chart_hygiene_and_reconciliation_skills,
)

router = APIRouter(prefix="/wahs", tags=["wahs"])


def _builtin_sample_bundles() -> Dict[str, Dict[str, Any]]:
    """Compact, realistic multi-source FHIR R4 sample bundles for zero-config demo & tests."""
    maurice_sample: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "pat-maurice-742",
                    "name": [{"given": ["Maurice742"], "family": "Auer97"}],
                    "birthDate": "1941-10-09",
                    "gender": "male",
                }
            },
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "org-pcp",
                    "name": "UW Medicine Primary Care",
                }
            },
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "org-cardio",
                    "name": "Northwest Heart & Vascular Institute",
                }
            },
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "org-wa-iis",
                    "name": "Washington Immunization Information System (WA IIS)",
                }
            },
            {
                "resource": {
                    "resourceType": "Provenance",
                    "id": "prov-cardio",
                    "target": [
                        {"reference": "urn:uuid:med-ext-maurice-plavix"},
                        {"reference": "urn:uuid:med-ext-maurice-lipitor"},
                    ],
                    "agent": [{"who": {"reference": "Organization/org-cardio"}}],
                }
            },
            {
                "resource": {
                    "resourceType": "Provenance",
                    "id": "prov-iis",
                    "target": [{"reference": "urn:uuid:imm-iis-maurice-flu"}],
                    "agent": [{"who": {"reference": "Organization/org-wa-iis"}}],
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-ckd-1",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "431855005",
                                "display": "Chronic kidney disease stage 1 (disorder)",
                            }
                        ]
                    },
                    "onsetDateTime": "1998-03-14",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-ckd-2",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "431856006",
                                "display": "Chronic kidney disease stage 2 (disorder)",
                            }
                        ]
                    },
                    "onsetDateTime": "2006-08-19",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-ckd-3",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "433144002",
                                "display": "Chronic kidney disease stage 3 (disorder)",
                            }
                        ]
                    },
                    "onsetDateTime": "2022-04-02",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-predm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "714628002",
                                "display": "Prediabetes (finding)",
                            }
                        ]
                    },
                    "onsetDateTime": "1996-11-30",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-t2dm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "44054006",
                                "display": "Diabetes mellitus type 2 (disorder)",
                            }
                        ]
                    },
                    "onsetDateTime": "2012-05-12",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-nstemi",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "401314000",
                                "display": "Acute non-ST segment elevation myocardial infarction (disorder)",
                            }
                        ]
                    },
                    "onsetDateTime": "2023-11-20",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-sdoh-ipv",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "706893006",
                                "display": "Victim of intimate partner abuse (finding)",
                            }
                        ]
                    },
                    "onsetDateTime": "2021-09-18",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-pcp-clopidogrel",
                    "status": "active",
                    "authoredOn": "2023-11-28",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "309362",
                                "display": "Clopidogrel 75 MG Oral Tablet",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-ext-maurice-plavix",
                    "status": "active",
                    "authoredOn": "2023-12-02",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "213169",
                                "display": "Plavix 75 MG Oral Tablet",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-pcp-simvastatin",
                    "status": "active",
                    "authoredOn": "2023-11-28",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "312961",
                                "display": "Simvastatin 20 MG Oral Tablet",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-ext-maurice-lipitor",
                    "status": "active",
                    "authoredOn": "2023-12-02",
                    "medicationCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                "code": "617310",
                                "display": "Lipitor 20 MG Oral Tablet",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Procedure",
                    "id": "proc-cabg-2023",
                    "status": "completed",
                    "performedPeriod": {
                        "start": "2023-11-21T08:00:00Z",
                        "end": "2023-11-21T12:30:00Z",
                    },
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "119561005",
                                "display": "Off-pump coronary artery bypass (procedure)",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Device",
                    "id": "dev-aortic-valve",
                    "status": "active",
                    "type": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "25510005",
                                "display": "Cardiac valve prosthesis (physical object)",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Immunization",
                    "id": "imm-zoster-1",
                    "status": "completed",
                    "occurrenceDateTime": "2012-12-08",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "121",
                                "display": "zoster live",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Immunization",
                    "id": "imm-zoster-2",
                    "status": "completed",
                    "occurrenceDateTime": "2013-12-14",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "121",
                                "display": "zoster live",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Immunization",
                    "id": "imm-pcp-maurice-flu",
                    "status": "completed",
                    "occurrenceDateTime": "2024-11-16",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "140",
                                "display": "Influenza, seasonal, injectable, preservative free",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Immunization",
                    "id": "imm-iis-maurice-flu",
                    "status": "completed",
                    "occurrenceDateTime": "2024-11-16",
                    "vaccineCode": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/cvx",
                                "code": "140",
                                "display": "Influenza, seasonal, injectable, preservative free",
                            }
                        ]
                    },
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-egfr-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "33914-3",
                                "display": "Glomerular filtration rate/1.73 sq M.predicted",
                            }
                        ]
                    },
                    "effectiveDateTime": "2022-04-02",
                    "valueQuantity": {"value": 55.6, "unit": "mL/min/{1.73_m2}"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-egfr-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "33914-3",
                                "display": "Glomerular filtration rate/1.73 sq M.predicted",
                            }
                        ]
                    },
                    "effectiveDateTime": "2023-11-22",
                    "valueQuantity": {"value": 41.3, "unit": "mL/min/{1.73_m2}"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-egfr-3",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "33914-3",
                                "display": "Glomerular filtration rate/1.73 sq M.predicted",
                            }
                        ]
                    },
                    "effectiveDateTime": "2025-11-22",
                    "valueQuantity": {"value": 72.1, "unit": "mL/min/{1.73_m2}"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-a1c-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "4548-4",
                                "display": "Hemoglobin A1c/Hemoglobin.total in Blood",
                            }
                        ]
                    },
                    "effectiveDateTime": "2023-11-20",
                    "valueQuantity": {"value": 6.1, "unit": "%"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-a1c-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "4548-4",
                                "display": "Hemoglobin A1c/Hemoglobin.total in Blood",
                            }
                        ]
                    },
                    "effectiveDateTime": "2025-11-22",
                    "valueQuantity": {"value": 6.3, "unit": "%"},
                }
            },
        ],
    }
    elena_sample: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "pat-elena-1001",
                    "name": [{"given": ["Elena"], "family": "Rostova"}],
                    "birthDate": "1954-04-12",
                    "gender": "female",
                }
            },
            {
                "resource": {
                    "resourceType": "Encounter",
                    "id": "enc-elena-hf",
                    "status": "finished",
                    "class": {"code": "IMP"},
                    "type": [{"text": "Inpatient Cardiology Admission (Decompensated HFrEF & Hyperkalemia)"}],
                    "period": {"start": "2026-01-15", "end": "2026-01-19"},
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-elena-ckd2",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Chronic kidney disease stage 2 (disorder)"},
                    "onsetDateTime": "2021-06-10",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-elena-ckd3b",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "700379002", "display": "Chronic kidney disease stage 3B (disorder)"}]
                    },
                    "onsetDateTime": "2025-11-04",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-elena-aldactone",
                    "status": "active",
                    "authoredOn": "2025-09-12",
                    "medicationCodeableConcept": {"text": "Aldactone 25 MG Oral Tablet [spironolactone]"},
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-elena-kcl",
                    "status": "active",
                    "authoredOn": "2025-10-04",
                    "medicationCodeableConcept": {"text": "Potassium Chloride 20 mEq Extended Release Oral Tablet"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-elena-k-peak",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-01-15T07:30:00Z",
                    "valueQuantity": {"value": 5.7, "unit": "mmol/L"},
                }
            },
        ],
    }
    return {
        "Maurice742_Auer97_sample": maurice_sample,
        "Elena_Rostova_Cardiorenal_sample": elena_sample,
    }


def _load_patient_catalog() -> Dict[str, Dict[str, Any]]:
    catalog: Dict[str, Dict[str, Any]] = {}
    bundle_dir = os.environ.get("WAHS_BUNDLE_DIR", "").strip()
    if bundle_dir and Path(bundle_dir).is_dir():
        b_path = Path(bundle_dir)
        for p in sorted(b_path.glob("*.json")):
            if p.name == "ground_truth.json":
                continue
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict) and data.get("resourceType") == "Bundle":
                    catalog[p.stem] = data
            except Exception:
                continue
        eval_sub = b_path / "eval"
        if eval_sub.is_dir():
            for p in sorted(eval_sub.glob("*.json")):
                if p.name == "ground_truth.json":
                    continue
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(data, dict) and data.get("resourceType") == "Bundle":
                        catalog[p.stem] = data
                except Exception:
                    continue

    if not catalog:
        catalog.update(_builtin_sample_bundles())
    return catalog



class AnalyzeRequest(BaseModel):
    patient_id: Optional[str] = Field(
        default=None,
        description="Patient identifier from /wahs/api/patients (optional if fhir_bundle is supplied).",
    )
    fhir_bundle: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Raw FHIR R4 Bundle JSON object (overrides patient_id if provided).",
    )
    target_tokens: int = Field(
        default=8192,
        ge=512,
        le=65536,
        description="Target token budget for progressive 5-Tier condensation.",
    )
    sharing_posture: str = Field(
        default="patient_self",
        pattern="^(patient_self|external_share)$",
        description="Privacy posture ('patient_self' or 'external_share').",
    )
    condense_mode: str = Field(
        default="lossless_structural",
        pattern="^(tiered|lossless_structural)$",
        description="Condensation mode ('lossless_structural' W12 non-destructive mode or 'tiered' 5-Tier ladder).",
    )



class ChatRequest(AnalyzeRequest):
    question: str = Field(
        ...,
        min_length=1,
        description="Clinical or patient question to answer against the condensed FHIR chart + WAHS KB.",
    )


def _resolve_bundle(req: AnalyzeRequest) -> Dict[str, Any]:
    if req.fhir_bundle and isinstance(req.fhir_bundle, dict):
        if req.fhir_bundle.get("resourceType") != "Bundle":
            raise HTTPException(status_code=400, detail="fhir_bundle must have resourceType='Bundle'")
        return req.fhir_bundle

    catalog = _load_patient_catalog()
    pid = req.patient_id or next(iter(catalog.keys()))
    if pid not in catalog:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown patient_id '{pid}'. Available: {sorted(catalog.keys())}",
        )
    return catalog[pid]


@router.get("/api/patients")
def list_patients() -> Dict[str, Any]:
    catalog = _load_patient_catalog()
    patients: List[Dict[str, Any]] = []
    for pid, bundle in catalog.items():
        extracted = extract_bundle(bundle)
        patients.append(
            {
                "id": pid,
                "patient_name": extracted.patient_name,
                "birth_date": extracted.birth_date,
                "reference_date": extracted.reference_date,
                "dataset": "Set B (Multi-Source Perturbed)" if "multisource" in pid else "Set A (Natural Synthea)",
                "total_bundle_entries": extracted.total_bundle_entries,
            }
        )
    return {"patients": patients}


@router.post("/api/analyze")
def run_wahs_analysis(req: AnalyzeRequest) -> Dict[str, Any]:
    bundle = _resolve_bundle(req)
    extracted = extract_bundle(bundle)
    condensed = condense_bundle_to_budget(
        bundle if req.condense_mode == "lossless_structural" else extracted,
        target_tokens=req.target_tokens,
        condense_mode=req.condense_mode,
        sharing_posture=req.sharing_posture,
    )
    duplicates_and_bloat = analyze_duplicates_and_bloat(
        extracted,
        condensed,
        target_tokens=req.target_tokens,
        sharing_posture=req.sharing_posture,
    )
    data_quality = analyze_data_quality_and_conflicts(
        extracted,
        condensed,
        target_tokens=req.target_tokens,
        sharing_posture=req.sharing_posture,
    )
    patterns = analyze_patterns_and_summaries(
        extracted,
        condensed,
        target_tokens=req.target_tokens,
        sharing_posture=req.sharing_posture,
    )
    skill_findings = run_chart_hygiene_and_reconciliation_skills(
        bundle,
        mappings=condensed.mappings,
    )
    return {
        "patient_name": extracted.patient_name,
        "birth_date": extracted.birth_date,
        "reference_date": extracted.reference_date,
        "condensation": {
            "tier": condensed.tier,
            "tier_name": condensed.tier_name,
            "condense_mode": req.condense_mode,
            "target_tokens": condensed.target_tokens,
            "actual_tokens": condensed.actual_tokens,
            "sharing_posture": req.sharing_posture,
            "chart_text": condensed.chart_text,
            "mappings": condensed.mappings,
            "completeness_manifest": condensed.completeness_manifest,
            "redacted_for_privacy": condensed.redacted_for_privacy,
            "compression_stats": condensed.compression_stats,
        },
        "analyses": {
            "duplicates_and_bloat": duplicates_and_bloat,
            "data_quality_and_conflicts": data_quality,
            "patterns_and_summaries": patterns,
            "chart_hygiene_and_reconciliation_skills": skill_findings,
        },
    }


@router.post("/api/chat")
def run_wahs_chat(req: ChatRequest) -> Dict[str, Any]:
    bundle = _resolve_bundle(req)
    extracted = extract_bundle(bundle)
    condensed = condense_bundle_to_budget(
        extracted,
        target_tokens=req.target_tokens,
        sharing_posture=req.sharing_posture,
    )
    return answer_wahs_question(
        extracted,
        req.question,
        condensed,
        target_tokens=req.target_tokens,
        sharing_posture=req.sharing_posture,
    )


@router.get("/demo", response_class=HTMLResponse)
def serve_wahs_demo_ui() -> HTMLResponse:
    """Interactive WAHS+ Clinical Chart Condenser API overview & link to the Patient-Facing Explorer."""
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WAHS+ Clinical Chart Condenser, Data Quality &amp; Grounded Q&amp;A</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f6f8fa; color: #1f2328; margin: 0; padding: 2rem; line-height: 1.5; }
    .card { max-width: 820px; margin: 0 auto; background: #fff; border: 1px solid #d0d7de; border-radius: 10px; padding: 1.75rem 2rem; }
    h1 { margin-top: 0; font-size: 1.35rem; }
    code { background: #eff2f5; padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.88rem; }
    a.btn { display: inline-block; margin-top: 0.75rem; background: #0969da; color: #fff; text-decoration: none; padding: 0.55rem 1rem; border-radius: 6px; font-weight: 600; }
  </style>
</head>
<body>
  <main class="card">
    <h1>WAHS+ Clinical Chart Condenser, Data Quality &amp; Grounded Q&amp;A</h1>
    <p>Backend FastAPI service for FHIR R4 / US Core / IPS chart preparation, non-destructive condensation, read-only clinical skills, and grounded Q&amp;A with <code>[N] -&gt; FHIR UUID</code> provenance.</p>
    <ul>
      <li><code>GET /wahs/api/patients</code> — List available FHIR R4 patient bundles and personas</li>
      <li><code>POST /wahs/api/analyze</code> — Condense a FHIR R4 bundle and run read-only data quality &amp; clinical skills</li>
      <li><code>POST /wahs/api/chat</code> — Ask a grounded question with citation provenance and abstention checks</li>
    </ul>
    <p><a class="btn" href="https://wahs-demo.openclinai.org/">Open Patient-Facing Interactive Explorer (wahs-demo.openclinai.org)</a></p>
  </main>
</body>
</html>"""
    return HTMLResponse(content=html)
