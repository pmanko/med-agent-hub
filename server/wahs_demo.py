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
    """Interactive WAHS+ Clinical Chart Explorer, Suggested Analyses, and Grounded Chat UX."""
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WAHS+ Interactive Clinical Summary &amp; Reconciliation Demo</title>
  <style>
    :root {
      --bg: #f6f8fa;
      --surface: #ffffff;
      --border: #d0d7de;
      --text: #1f2328;
      --muted: #59636e;
      --accent: #0969da;
      --accent-soft: #ddf4ff;
      --danger: #cf222e;
      --danger-soft: #ffebe9;
      --warn: #9a6700;
      --warn-soft: #fff8c5;
      --ok: #1a7f37;
      --ok-soft: #dafbe1;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }
    header {
      background: #0d1117;
      color: #fff;
      padding: 1rem 1.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 1rem;
    }
    header h1 { margin: 0; font-size: 1.2rem; }
    header p { margin: 0.2rem 0 0; font-size: 0.85rem; color: #8b949e; }
    .controls {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0.85rem 1.5rem;
      display: flex;
      flex-wrap: wrap;
      gap: 1rem;
      align-items: center;
    }
    .control-group { display: flex; flex-direction: column; gap: 0.25rem; }
    .control-group label { font-size: 0.75rem; font-weight: 600; color: var(--muted); text-transform: uppercase; }
    select, button, input {
      padding: 0.45rem 0.7rem;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 0.9rem;
      background: #fff;
    }
    button.primary { background: var(--accent); color: #fff; border-color: var(--accent); cursor: pointer; font-weight: 600; }
    .layout {
      display: grid;
      grid-template-columns: 1.35fr 1fr;
      gap: 1.25rem;
      padding: 1.25rem 1.5rem;
      max-width: 1600px;
      margin: 0 auto;
    }
    @media (max-width: 1024px) { .layout { grid-template-columns: 1fr; } }
    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .tabs {
      display: flex;
      border-bottom: 1px solid var(--border);
      background: #f0f3f6;
      overflow-x: auto;
    }
    .tab-btn {
      border: none;
      background: transparent;
      padding: 0.75rem 1rem;
      font-weight: 600;
      font-size: 0.85rem;
      color: var(--muted);
      cursor: pointer;
      border-bottom: 2px solid transparent;
      border-radius: 0;
    }
    .tab-btn.active {
      color: var(--accent);
      border-bottom-color: var(--accent);
      background: var(--surface);
    }
    .tab-body { padding: 1.1rem; overflow-y: auto; max-height: 74vh; }
    .card {
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 0.9rem;
      margin-bottom: 0.85rem;
    }
    .card.high { border-left: 4px solid var(--danger); background: #fffbfa; }
    .card.medium { border-left: 4px solid var(--warn); }
    .badge {
      display: inline-block;
      padding: 0.15rem 0.5rem;
      border-radius: 999px;
      font-size: 0.73rem;
      font-weight: 600;
    }
    .badge.high { background: var(--danger-soft); color: var(--danger); }
    .badge.medium { background: var(--warn-soft); color: var(--warn); }
    .badge.ok { background: var(--ok-soft); color: var(--ok); }
    .cite-chip {
      display: inline-block;
      background: var(--accent-soft);
      color: var(--accent);
      border: 1px solid #9cd6ff;
      border-radius: 4px;
      padding: 0.1rem 0.4rem;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.75rem;
      margin: 0.1rem;
      cursor: pointer;
    }
    .chat-box { display: flex; flex-direction: column; height: 74vh; }
    .chat-messages { flex: 1; padding: 1rem; overflow-y: auto; display: flex; flex-direction: column; gap: 0.85rem; }
    .msg { padding: 0.8rem 1rem; border-radius: 8px; font-size: 0.9rem; }
    .msg.user { background: var(--accent-soft); align-self: flex-end; max-width: 85%; }
    .msg.assistant { background: #f6f8fa; border: 1px solid var(--border); align-self: stretch; }
    .prompt-chips { padding: 0.6rem 1rem; border-top: 1px solid var(--border); display: flex; flex-wrap: wrap; gap: 0.4rem; background: #fafbfc; }
    .prompt-chip { font-size: 0.76rem; padding: 0.25rem 0.55rem; background: #fff; border: 1px solid var(--border); border-radius: 999px; cursor: pointer; }
    .chat-input { display: flex; gap: 0.5rem; padding: 0.75rem 1rem; border-top: 1px solid var(--border); }
    .chat-input input { flex: 1; }
    pre.ledger { font-size: 0.78rem; background: #0d1117; color: #e6edf3; padding: 1rem; border-radius: 6px; overflow-x: auto; white-space: pre-wrap; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>WAHS+ Clinical Chart Condenser, Data Quality &amp; Grounded Q&amp;A</h1>
      <p>5-Tier Progressive FHIR R4 / US Core / IPS Condenser + 4-Layer Clinical Knowledge Base</p>
    </div>
    <div id="tier-banner" style="font-size: 0.85rem;">Loading patient chart...</div>
  </header>

  <section class="controls" aria-label="Configuration controls">
    <div class="control-group">
      <label for="patient-select">Patient Bundle</label>
      <select id="patient-select"></select>
    </div>
    <div class="control-group">
      <label for="budget-select">Token Budget</label>
      <select id="budget-select">
        <option value="16384">16,384 tokens (Tier 1/2)</option>
        <option value="8192" selected>8,192 tokens (Tier 2/3)</option>
        <option value="4096">4,096 tokens (Tier 3/4)</option>
        <option value="2048">2,048 tokens (Tier 4 IPS Safety Core)</option>
        <option value="1024">1,024 tokens (Emergency Floor)</option>
      </select>
    </div>
    <div class="control-group">
      <label for="posture-select">Privacy &amp; Sharing Posture</label>
      <select id="posture-select">
        <option value="patient_self" selected>Patient Self-View (Full SDOH)</option>
        <option value="external_share">External Share (42 CFR Part 2 / WA MHMDA Redacted)</option>
      </select>
    </div>
    <button class="primary" id="refresh-btn" type="button">Run Analysis</button>
  </section>

  <main class="layout">
    <section class="panel" aria-label="Suggested Clinical Analyses">
      <div class="tabs">
        <button class="tab-btn active" data-tab="dq">1. Data Quality &amp; Conflicts</button>
        <button class="tab-btn" data-tab="bloat">2. Duplicate Labs &amp; Bloat</button>
        <button class="tab-btn" data-tab="patterns">3. Patterns &amp; Encounter Summary</button>
        <button class="tab-btn" data-tab="ledger">4. Condensed IPS Ledger</button>
      </div>
      <div class="tab-body" id="analysis-content"></div>
    </section>

    <section class="panel" aria-label="Grounded Patient and Clinician Chat">
      <div class="tabs">
        <button class="tab-btn active" style="cursor:default;">Grounded Chart Q&amp;A (with [N] -&gt; FHIR UUID Provenance)</button>
      </div>
      <div class="chat-box">
        <div class="chat-messages" id="chat-messages"></div>
        <div class="prompt-chips">
          <button class="prompt-chip" data-q="What duplicate medications or dangerous drug conflicts are on my active list?">Duplicate Meds &amp; Statins</button>
          <button class="prompt-chip" data-q="Why are there conflicting Chronic Kidney Disease stages and what is my eGFR trend?">CKD Stage Conflict &amp; eGFR</button>
          <button class="prompt-chip" data-q="Summarize the November 2023 hospital and ICU stay.">Nov 2023 CABG &amp; ICU Stay</button>
          <button class="prompt-chip" data-q="Are there any duplicate vaccines or ACIP immunization schedule anomalies?">Vaccine &amp; ACIP Audit</button>
          <button class="prompt-chip" data-q="Does this patient have a documented Penicillin allergy or recent colonoscopy?">Abstention Probe (Penicillin / Colonoscopy)</button>
        </div>
        <form class="chat-input" id="chat-form">
          <input type="text" id="chat-input" placeholder="Ask about medications, lab trends, conflicts, encounters, or test abstention..." required>
          <button type="submit" class="primary">Ask</button>
        </form>
      </div>
    </section>
  </main>

  <script>
    let currentData = null;
    let activeTab = 'dq';

    function renderCitations(cites) {
      if (!cites || !cites.length) return '';
      return cites.map(c => `<span class="cite-chip" title="${c.resource_type}/${c.resource_uuid}: ${(c.text || '').replace(/"/g, '&quot;')}">[${c.index}] ${c.resource_type}/${(c.resource_uuid || '').slice(0, 12)}</span>`).join(' ');
    }

    function renderActiveTab() {
      const container = document.getElementById('analysis-content');
      if (!currentData) { container.innerHTML = '<p>Loading...</p>'; return; }
      const { condensation, analyses } = currentData;

      if (activeTab === 'dq') {
        const dq = analyses.data_quality_and_conflicts;
        container.innerHTML = `
          <h3>Data Quality, Cross-Clinic Conflicts &amp; Schedule Anomalies (${dq.total_findings})</h3>
          ${dq.findings.map(f => `
            <div class="card ${f.severity}">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.35rem;">
                <strong>${f.title}</strong>
                <span class="badge ${f.severity}">${f.domain}</span>
              </div>
              <p style="margin:0.35rem 0;">${f.description}</p>
              <p style="margin:0.35rem 0;font-size:0.85rem;color:var(--muted);"><strong>Action:</strong> ${f.recommendation} <code>(${f.kb_rule_id})</code></p>
              <div>${renderCitations(f.citations)}</div>
            </div>
          `).join('')}
        `;
      } else if (activeTab === 'bloat') {
        const b = analyses.duplicates_and_bloat;
        const sm = b.summary_metrics;
        container.innerHTML = `
          <h3>Duplicate Labs, Repeated Panels &amp; Record Bloat</h3>
          <p><strong>${sm.total_bundle_entries.toLocaleString()}</strong> raw FHIR entries condensed to <strong>${sm.condensed_ledger_lines}</strong> numbered ledger lines (<strong>${sm.condensed_tokens.toLocaleString()}</strong> tokens, Tier ${sm.tier}).</p>
          ${b.cards.map(c => `
            <div class="card ${c.severity}">
              <strong>${c.title}</strong> <span class="badge ${c.severity}">${c.category}</span>
              <p style="margin:0.4rem 0;">${c.summary}</p>
              <div>${renderCitations(c.citations)}</div>
            </div>
          `).join('')}
        `;
      } else if (activeTab === 'patterns') {
        const p = analyses.patterns_and_summaries;
        const hl = p.highlight_encounter || {};
        container.innerHTML = `
          <h3>Longitudinal Biomarker Trajectories &amp; Encounter Summary</h3>
          ${hl.title ? `
            <div class="card high">
              <strong>${hl.title}</strong>
              <p style="margin:0.4rem 0;">${hl.narrative}</p>
              <div>${renderCitations(hl.citations)}</div>
            </div>
          ` : ''}
          ${p.trajectories.map(t => `
            <div class="card">
              <strong>${t.concept}</strong> (${t.count} measurements, ${t.direction})
              <p style="margin:0.3rem 0;font-size:0.88rem;">
                Initial: <code>${t.initial.value} ${t.unit}</code> (${t.initial.date}) &rarr;
                Range: <code>${t.min.value}&ndash;${t.max.value}</code> &rarr;
                <strong>Latest: <code>${t.latest.value} ${t.unit}</code> (${t.latest.date})</strong>
              </p>
              <div>${renderCitations(t.citations)}</div>
            </div>
          `).join('')}
        `;
      } else {
        container.innerHTML = `
          <h3>Condensed Numbered Ledger (Tier ${condensation.tier}: ${condensation.actual_tokens.toLocaleString()} tokens)</h3>
          <pre class="ledger">${condensation.chart_text.replace(/</g, '&lt;')}</pre>
        `;
      }
    }

    async function loadAnalysis() {
      const patient_id = document.getElementById('patient-select').value;
      const target_tokens = parseInt(document.getElementById('budget-select').value, 10);
      const sharing_posture = document.getElementById('posture-select').value;
      const res = await fetch('/wahs/api/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ patient_id, target_tokens, sharing_posture })
      });
      currentData = await res.json();
      const c = currentData.condensation;
      document.getElementById('tier-banner').innerHTML =
        `<strong>${currentData.patient_name}</strong> | Tier ${c.tier} (${c.tier_name}) | <strong>${c.actual_tokens.toLocaleString()}</strong> / ${c.target_tokens.toLocaleString()} tokens | Posture: <code>${c.sharing_posture}</code>`;
      renderActiveTab();
    }

    async function askQuestion(question) {
      const msgs = document.getElementById('chat-messages');
      msgs.innerHTML += `<div class="msg user">${question}</div>`;
      const patient_id = document.getElementById('patient-select').value;
      const target_tokens = parseInt(document.getElementById('budget-select').value, 10);
      const sharing_posture = document.getElementById('posture-select').value;
      const res = await fetch('/wahs/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ patient_id, target_tokens, sharing_posture, question })
      });
      const data = await res.json();
      const badge = data.abstained
        ? `<span class="badge high">ABSTAINED: ${data.abstention_reason}</span>`
        : `<span class="badge ok">GROUNDED IN CHART (${data.citations.length} citations)</span>`;
      const formatted = (data.answer || '').replace(/\\n/g, '<br>');
      msgs.innerHTML += `
        <div class="msg assistant">
          <div style="margin-bottom:0.4rem;">${badge}</div>
          <div>${formatted}</div>
          <div style="margin-top:0.5rem;">${renderCitations(data.citations)}</div>
        </div>
      `;
      msgs.scrollTop = msgs.scrollHeight;
    }

    async function init() {
      const pRes = await fetch('/wahs/api/patients');
      const pData = await pRes.json();
      const sel = document.getElementById('patient-select');
      sel.innerHTML = pData.patients.map(p =>
        `<option value="${p.id}">${p.patient_name} — ${p.dataset} (${p.total_bundle_entries.toLocaleString()} entries)</option>`
      ).join('');
      await loadAnalysis();
      await askQuestion("What duplicate medications or dangerous drug conflicts are on my active list?");
    }

    document.querySelectorAll('.tab-btn[data-tab]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn[data-tab]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeTab = btn.dataset.tab;
        renderActiveTab();
      });
    });
    document.getElementById('refresh-btn').addEventListener('click', loadAnalysis);
    document.getElementById('patient-select').addEventListener('change', loadAnalysis);
    document.getElementById('budget-select').addEventListener('change', loadAnalysis);
    document.getElementById('posture-select').addEventListener('change', loadAnalysis);
    document.querySelectorAll('.prompt-chip').forEach(chip => {
      chip.addEventListener('click', () => askQuestion(chip.dataset.q));
    });
    document.getElementById('chat-form').addEventListener('submit', e => {
      e.preventDefault();
      const inp = document.getElementById('chat-input');
      const q = inp.value.trim();
      if (q) { inp.value = ''; askQuestion(q); }
    });
    init();
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)
