"""US Core / IPS De-Identified Multi-Clinic Transition Archetype Builders for WAHS+.

Defines three non-Synthea US Core / IPS clinical transition patient bundles:
- `Elena_Rostova_Cardiorenal.json`: Decompensated HFrEF, cardiorenal syndrome, hyperkalemia,
  and discharge note hold mismatch (`Aldactone` + `Potassium Chloride` + `Lisinopril`).
- `Arthur_Pendelton_Anticoag.json`: Mechanical aortic valve, atrial fibrillation, upper GI
  hemorrhage, dual oral anticoagulation (`Eliquis` + `Warfarin`) + NSAID (`Naproxen`), and
  cross-clinic active vs. resolved bleed status discrepancy.
- `Margaret_Chen_Polypharmacy.json`: Geriatric fall evaluation, orthostatic hypotension,
  AGS Beers Criteria duplicate CNS sedative-hypnotics (`Ativan` + `Ambien`), concurrent
  `Prediabetes` + `Type 2 Diabetes`, duplicate WA IIS pneumococcal vaccines, and sensitive
  SDOH privacy records.
"""

from __future__ import annotations

import base64
from typing import Any, Dict, List, Tuple


def build_deidentified_real_clinical_summaries() -> Dict[str, Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """Pathway B: Three multi-domain US Core / IPS clinical transition patient bundles
    (Elena Rostova — Cardiorenal HFrEF & Hyperkalemia; Arthur Pendelton — AFib + Mechanical Valve
    Dual-Anticoagulation & Upper GI Bleed; Margaret Chen — Geriatric Polypharmacy, Beers Criteria
    CNS Sedation & Falls), plus backward-compatible MIMIC-IV / SMART IPS aliases."""
    elena_note = (
        "# Chief Complaint\nDecompensated HFrEF, cardiorenal syndrome & hyperkalemia (peak K+ 5.7 mmol/L)\n"
        "# Assessment and Plan\nHold spironolactone and potassium chloride at hospital discharge due to "
        "hyperkalemia (5.7 mmol/L) and acute-on-chronic kidney injury (eGFR nadir 31.0 mL/min/1.73m2). "
        "Hold lisinopril while continuing Entresto 49/51 MG BID and Furosemide 40 MG daily; reconcile outpatient Plavix vs Clopidogrel bottles."
    )
    elena_note_b64 = base64.b64encode(elena_note.encode("utf-8")).decode("ascii")
    elena_bundle: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "id": "elena-rostova-cardiorenal",
        "entry": [
            {
                "fullUrl": "urn:uuid:mimic-pat-1001",
                "resource": {
                    "resourceType": "Patient",
                    "id": "mimic-pat-1001",
                    "name": [{"use": "official", "family": "Rostova", "given": ["Elena"]}],
                    "gender": "female",
                    "birthDate": "1954-04-12",
                },
            },
            {
                "fullUrl": "urn:uuid:elena-org-cardio",
                "resource": {"resourceType": "Organization", "id": "elena-org-cardio", "name": "UW Medicine Heart Institute"},
            },
            {
                "fullUrl": "urn:uuid:elena-org-nephro",
                "resource": {"resourceType": "Organization", "id": "elena-org-nephro", "name": "Northwest Kidney Centers"},
            },
            {
                "fullUrl": "urn:uuid:elena-enc-admit",
                "resource": {
                    "resourceType": "Encounter",
                    "id": "elena-enc-admit",
                    "status": "finished",
                    "class": {"code": "IMP"},
                    "type": [{"text": "Inpatient Cardiology & Nephrology Admission (Decompensated HFrEF & Hyperkalemia)"}],
                    "period": {"start": "2026-01-15", "end": "2026-01-19"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-allergy-1",
                "resource": {
                    "resourceType": "AllergyIntolerance",
                    "id": "mimic-allergy-1",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Iodinated contrast media (anaphylactoid reaction)"},
                    "criticality": "high",
                },
            },
            {
                "fullUrl": "urn:uuid:elena-cond-hfref",
                "resource": {
                    "resourceType": "Condition",
                    "id": "elena-cond-hfref",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "441481004", "display": "Chronic systolic heart failure (disorder)"}],
                        "text": "Chronic systolic heart failure (disorder)",
                    },
                    "onsetDateTime": "2022-08-14",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-cond-ckd2",
                "resource": {
                    "resourceType": "Condition",
                    "id": "mimic-cond-ckd2",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Chronic kidney disease stage 2 (disorder)"},
                    "onsetDateTime": "2021-06-10",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-cond-ckd3",
                "resource": {
                    "resourceType": "Condition",
                    "id": "mimic-cond-ckd3",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "700379002", "display": "Chronic kidney disease stage 3B (disorder)"}],
                        "text": "Chronic kidney disease stage 3B (disorder)",
                    },
                    "onsetDateTime": "2025-11-04",
                },
            },
            {
                "fullUrl": "urn:uuid:elena-cond-hyperk",
                "resource": {
                    "resourceType": "Condition",
                    "id": "elena-cond-hyperk",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "14140009", "display": "Hyperkalemia (disorder)"}],
                        "text": "Hyperkalemia (disorder)",
                    },
                    "onsetDateTime": "2026-01-15",
                },
            },
            {
                "fullUrl": "urn:uuid:elena-proc-echo",
                "resource": {
                    "resourceType": "Procedure",
                    "id": "elena-proc-echo",
                    "status": "completed",
                    "code": {"text": "Transthoracic echocardiography and Doppler hemodynamic assessment"},
                    "performedDateTime": "2026-01-16",
                },
            },
            {
                "fullUrl": "urn:uuid:elena-dev-icd",
                "resource": {
                    "resourceType": "Device",
                    "id": "elena-dev-icd",
                    "status": "active",
                    "type": {"text": "Dual-chamber implantable cardioverter-defibrillator (ICD)"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-med-aldactone",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic-med-aldactone",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "313096", "display": "Aldactone 25 MG Oral Tablet [spironolactone]"}],
                        "text": "Aldactone 25 MG Oral Tablet [spironolactone]",
                    },
                    "authoredOn": "2025-09-12",
                    "performer": [{"actor": {"display": "UW Medicine Heart Institute"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-med-kcl",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic-med-kcl",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "628958", "display": "Potassium Chloride 20 mEq Extended Release Oral Tablet"}],
                        "text": "Potassium Chloride 20 mEq Extended Release Oral Tablet",
                    },
                    "authoredOn": "2025-10-04",
                    "performer": [{"actor": {"display": "Community Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-med-lisinopril",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic-med-lisinopril",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {"text": "lisinopril 20 MG Oral Tablet"},
                    "authoredOn": "2025-08-14",
                    "performer": [{"actor": {"display": "Community Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-med-clopidogrel",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic-med-clopidogrel",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "309362", "display": "Clopidogrel 75 MG Oral Tablet"}],
                        "text": "Clopidogrel 75 MG Oral Tablet",
                    },
                    "authoredOn": "2025-10-01",
                    "performer": [{"actor": {"display": "Community Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-med-plavix",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic-med-plavix",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {"text": "Plavix 75 MG Oral Tablet"},
                    "authoredOn": "2026-01-19",
                    "performer": [{"actor": {"display": "Harborview Inpatient Cardiology"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:elena-med-furosemide",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "elena-med-furosemide",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "310429", "display": "Furosemide 40 MG Oral Tablet"}],
                        "text": "Furosemide 40 MG Oral Tablet",
                    },
                    "authoredOn": "2026-01-19",
                    "performer": [{"actor": {"display": "UW Medicine Heart Institute"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-egfr-2023",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-egfr-2023",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate/1.73 sq M.predicted"}]},
                    "effectiveDateTime": "2023-05-10",
                    "valueQuantity": {"value": 58.0, "unit": "mL/min/1.73_m2"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-obs-egfr-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "mimic-obs-egfr-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate/1.73 sq M.predicted"}]},
                    "effectiveDateTime": "2026-01-15T07:30:00Z",
                    "valueQuantity": {"value": 31.0, "unit": "mL/min/1.73_m2"},
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-egfr-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-egfr-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate/1.73 sq M.predicted"}]},
                    "effectiveDateTime": "2026-01-19T06:00:00Z",
                    "valueQuantity": {"value": 38.5, "unit": "mL/min/1.73_m2"},
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-k-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-k-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "6298-4", "display": "Potassium [Moles/volume] in Blood"}]},
                    "effectiveDateTime": "2025-11-04",
                    "valueQuantity": {"value": 4.4, "unit": "mmol/L"},
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-k-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-k-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-01-15T07:30:00Z",
                    "valueQuantity": {"value": 5.7, "unit": "mmol/L"},
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-k-3",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-k-3",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-01-19T06:00:00Z",
                    "valueQuantity": {"value": 4.5, "unit": "mmol/L"},
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-bnp-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-bnp-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33762-6", "display": "Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-01-15T07:30:00Z",
                    "valueQuantity": {"value": 4850.0, "unit": "pg/mL"},
                },
            },
            {
                "fullUrl": "urn:uuid:elena-obs-bnp-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "elena-obs-bnp-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33762-6", "display": "Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-01-19T06:00:00Z",
                    "valueQuantity": {"value": 1890.0, "unit": "pg/mL"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic-note-discharge",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "mimic-note-discharge",
                    "status": "current",
                    "type": {"text": "Inpatient Cardiology Discharge Summary"},
                    "date": "2026-01-19T16:00:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": elena_note_b64}}],
                },
            },
            {
                "fullUrl": "urn:uuid:elena-note-dup",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "elena-note-dup",
                    "status": "current",
                    "type": {"text": "Duplicate HIE Discharge Copy"},
                    "date": "2026-01-19T16:05:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": elena_note_b64}}],
                },
            },
        ],
    }
    elena_anomalies = [
        {
            "anomaly_id": "mimic-ckd-stage2-vs-stage3",
            "category": "mutually_exclusive_condition_stages",
            "severity": "high",
            "description": "Uncoded CKD Stage 2 and coded CKD Stage 3B simultaneously active in cardiorenal summary.",
            "expected_resource_uuids": ["mimic-cond-ckd2", "mimic-cond-ckd3"],
        },
        {
            "anomaly_id": "elena-hyperkalemia-spironolactone-kcl",
            "category": "high_risk_drug_electrolyte_interaction",
            "severity": "high",
            "description": "Concurrent active Aldactone 25 MG [spironolactone] and Potassium Chloride 20 mEq with hyperkalemia (K+ 5.7 mmol/L).",
            "expected_resource_uuids": ["mimic-med-aldactone", "mimic-med-kcl"],
        },
        {
            "anomaly_id": "mimic-plavix-clopidogrel-dup",
            "category": "cross_source_brand_generic_medication_duplicate",
            "severity": "high",
            "description": "Uncoded discharge Plavix 75 MG duplicates outpatient Clopidogrel 75 MG.",
            "expected_resource_uuids": ["mimic-med-clopidogrel", "mimic-med-plavix"],
        },
        {
            "anomaly_id": "mimic-note-hold-lisinopril",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": "Discharge summary instructs to hold spironolactone, potassium chloride, and lisinopril while outpatient MedicationRequests remain active.",
            "expected_resource_uuids": ["mimic-med-aldactone", "mimic-med-kcl", "mimic-med-lisinopril", "mimic-note-discharge"],
        },
    ]

    arthur_note = (
        "# Chief Complaint\nAcute upper gastrointestinal hemorrhage & supratherapeutic INR (4.8)\n"
        "# Assessment and Plan\nStop Eliquis and Naproxen immediately. Patient has a mechanical aortic valve "
        "requiring single-agent Warfarin anticoagulation (target INR 2.5-3.5) with Pantoprazole 40 MG BID GI prophylaxis. "
        "Upper GI bleeding resolved following endoscopic hemoclip placement on 2026-02-11."
    )
    arthur_note_b64 = base64.b64encode(arthur_note.encode("utf-8")).decode("ascii")
    arthur_bundle: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "id": "arthur-pendelton-anticoag",
        "entry": [
            {
                "fullUrl": "urn:uuid:arthur-pat-2001",
                "resource": {
                    "resourceType": "Patient",
                    "id": "arthur-pat-2001",
                    "name": [{"use": "official", "family": "Pendelton", "given": ["Arthur"]}],
                    "gender": "male",
                    "birthDate": "1947-08-19",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-enc-gib-admit",
                "resource": {
                    "resourceType": "Encounter",
                    "id": "arthur-enc-gib-admit",
                    "status": "finished",
                    "class": {"code": "IMP"},
                    "type": [{"text": "Inpatient Gastroenterology & Cardiology Admission (Upper GI Bleed & Coagulopathy)"}],
                    "period": {"start": "2026-02-10", "end": "2026-02-14"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-allergy-sulfa",
                "resource": {
                    "resourceType": "AllergyIntolerance",
                    "id": "arthur-allergy-sulfa",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Sulfonamide antibiotics (maculopapular rash)"},
                    "criticality": "low",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-cond-afib",
                "resource": {
                    "resourceType": "Condition",
                    "id": "arthur-cond-afib",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "49436004", "display": "Atrial fibrillation (disorder)"}],
                        "text": "Atrial fibrillation (disorder)",
                    },
                    "onsetDateTime": "2019-04-12",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-cond-gib-active",
                "resource": {
                    "resourceType": "Condition",
                    "id": "arthur-cond-gib-active",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "74474003", "display": "Gastrointestinal hemorrhage (disorder)"}],
                        "text": "Gastrointestinal hemorrhage (disorder)",
                    },
                    "onsetDateTime": "2026-02-10",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-cond-gib-resolved",
                "resource": {
                    "resourceType": "Condition",
                    "id": "arthur-cond-gib-resolved",
                    "clinicalStatus": {"coding": [{"code": "resolved"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "74474003", "display": "Gastrointestinal hemorrhage (disorder)"}],
                        "text": "Gastrointestinal hemorrhage (disorder)",
                    },
                    "onsetDateTime": "2026-02-10",
                    "abatementDateTime": "2026-02-14",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-proc-egd",
                "resource": {
                    "resourceType": "Procedure",
                    "id": "arthur-proc-egd",
                    "status": "completed",
                    "code": {"text": "Esophagogastroduodenoscopy (EGD) with endoscopic hemoclip hemostasis of duodenal ulcer"},
                    "performedDateTime": "2026-02-11",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-proc-transfusion",
                "resource": {
                    "resourceType": "Procedure",
                    "id": "arthur-proc-transfusion",
                    "status": "completed",
                    "code": {"text": "Transfusion of packed red blood cells (2 units)"},
                    "performedDateTime": "2026-02-10",
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-dev-valve",
                "resource": {
                    "resourceType": "Device",
                    "id": "arthur-dev-valve",
                    "status": "active",
                    "type": {"text": "St. Jude Mechanical Aortic Valve Prosthesis"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-med-eliquis",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "arthur-med-eliquis",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "1364430", "display": "Eliquis 5 MG Oral Tablet [apixaban]"}],
                        "text": "Eliquis 5 MG Oral Tablet [apixaban]",
                    },
                    "authoredOn": "2026-01-20",
                    "performer": [{"actor": {"display": "Eastside Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-med-warfarin",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "arthur-med-warfarin",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "855332", "display": "Warfarin Sodium 5 MG Oral Tablet"}],
                        "text": "Warfarin Sodium 5 MG Oral Tablet",
                    },
                    "authoredOn": "2025-05-12",
                    "performer": [{"actor": {"display": "Swedish Heart & Vascular Anticoagulation Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-med-naproxen",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "arthur-med-naproxen",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "849574", "display": "Naproxen 220 MG Oral Tablet"}],
                        "text": "Naproxen 220 MG Oral Tablet",
                    },
                    "authoredOn": "2026-01-28",
                    "performer": [{"actor": {"display": "Urgent Care Walk-In Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-med-pantoprazole",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "arthur-med-pantoprazole",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "314153", "display": "Pantoprazole 40 MG Delayed Release Oral Tablet"}],
                        "text": "Pantoprazole 40 MG Delayed Release Oral Tablet",
                    },
                    "authoredOn": "2026-02-11",
                    "performer": [{"actor": {"display": "Virginia Mason Gastroenterology"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-obs-hgb-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "arthur-obs-hgb-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood"}]},
                    "effectiveDateTime": "2025-09-15",
                    "valueQuantity": {"value": 13.4, "unit": "g/dL"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-obs-hgb-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "arthur-obs-hgb-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood"}]},
                    "effectiveDateTime": "2026-02-10T06:15:00Z",
                    "valueQuantity": {"value": 7.6, "unit": "g/dL"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-obs-hgb-3",
                "resource": {
                    "resourceType": "Observation",
                    "id": "arthur-obs-hgb-3",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood"}]},
                    "effectiveDateTime": "2026-02-14T08:00:00Z",
                    "valueQuantity": {"value": 10.2, "unit": "g/dL"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-obs-inr-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "arthur-obs-inr-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "6301-6", "display": "INR in Platelet poor plasma by Coagulation assay"}]},
                    "effectiveDateTime": "2025-11-20",
                    "valueQuantity": {"value": 2.6, "unit": "{INR}"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-obs-inr-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "arthur-obs-inr-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "6301-6", "display": "INR in Platelet poor plasma by Coagulation assay"}]},
                    "effectiveDateTime": "2026-02-10T06:15:00Z",
                    "valueQuantity": {"value": 4.8, "unit": "{INR}"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-obs-inr-3",
                "resource": {
                    "resourceType": "Observation",
                    "id": "arthur-obs-inr-3",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "6301-6", "display": "INR in Platelet poor plasma by Coagulation assay"}]},
                    "effectiveDateTime": "2026-02-14T08:00:00Z",
                    "valueQuantity": {"value": 2.7, "unit": "{INR}"},
                },
            },
            {
                "fullUrl": "urn:uuid:arthur-note-discharge",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "arthur-note-discharge",
                    "status": "current",
                    "type": {"text": "Gastroenterology & Anticoagulation Discharge Summary"},
                    "date": "2026-02-14T15:30:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": arthur_note_b64}}],
                },
            },
        ],
    }
    arthur_anomalies = [
        {
            "anomaly_id": "arthur-dual-oral-anticoagulant",
            "category": "cross_source_therapeutic_class_duplicate",
            "severity": "high",
            "description": "Concurrent active Eliquis 5 MG [apixaban] and Warfarin Sodium 5 MG across primary care and cardiology.",
            "expected_resource_uuids": ["arthur-med-eliquis", "arthur-med-warfarin"],
        },
        {
            "anomaly_id": "arthur-anticoag-nsaid-bleed-hazard",
            "category": "high_risk_drug_bleeding_interaction",
            "severity": "high",
            "description": "Concurrent systemic NSAID (Naproxen 220 MG) with oral anticoagulation precipitating upper GI hemorrhage.",
            "expected_resource_uuids": ["arthur-med-naproxen", "arthur-med-warfarin", "arthur-med-eliquis"],
        },
        {
            "anomaly_id": "arthur-gib-active-vs-resolved",
            "category": "cross_source_condition_status_conflict",
            "severity": "medium",
            "description": "Gastrointestinal hemorrhage marked ACTIVE in primary care but RESOLVED after endoscopic hemoclip therapy.",
            "expected_resource_uuids": ["arthur-cond-gib-active", "arthur-cond-gib-resolved"],
        },
        {
            "anomaly_id": "arthur-note-stop-eliquis-naproxen",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": "Discharge note instructs to stop Eliquis and Naproxen immediately while structured orders remain active.",
            "expected_resource_uuids": ["arthur-med-eliquis", "arthur-med-naproxen", "arthur-note-discharge"],
        },
    ]

    margaret_note = (
        "# Chief Complaint\nGeriatric fall evaluation, orthostatic hypotension & polypharmacy review\n"
        "# Assessment and Plan\nDiscontinue Ambien and lorazepam immediately due to recurrent falls and "
        "AGS Beers Criteria CNS sedative-hypnotic duplication. Hold glipizide to prevent nocturnal hypoglycemia "
        "(HbA1c 6.7% on metformin monotherapy)."
    )
    margaret_note_b64 = base64.b64encode(margaret_note.encode("utf-8")).decode("ascii")
    margaret_bundle: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "document",
        "id": "margaret-chen-polypharmacy",
        "entry": [
            {
                "fullUrl": "urn:uuid:smart-pat-2002",
                "resource": {
                    "resourceType": "Patient",
                    "id": "smart-pat-2002",
                    "name": [{"use": "official", "family": "Chen", "given": ["Margaret"]}],
                    "gender": "female",
                    "birthDate": "1943-11-03",
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-enc-ed-fall",
                "resource": {
                    "resourceType": "Encounter",
                    "id": "margaret-enc-ed-fall",
                    "status": "finished",
                    "class": {"code": "EMER"},
                    "type": [{"text": "Emergency Department Evaluation for Mechanical Fall & Orthostatic Syncope"}],
                    "period": {"start": "2026-01-28", "end": "2026-01-29"},
                },
            },
            {
                "fullUrl": "urn:uuid:smart-cond-predm",
                "resource": {
                    "resourceType": "Condition",
                    "id": "smart-cond-predm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Prediabetes (finding)"},
                    "onsetDateTime": "2019-03-11",
                },
            },
            {
                "fullUrl": "urn:uuid:smart-cond-t2dm",
                "resource": {
                    "resourceType": "Condition",
                    "id": "smart-cond-t2dm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Diabetes mellitus type 2 (disorder)"}],
                        "text": "Diabetes mellitus type 2 (disorder)",
                    },
                    "onsetDateTime": "2024-07-18",
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-cond-ortho",
                "resource": {
                    "resourceType": "Condition",
                    "id": "margaret-cond-ortho",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "28651003", "display": "Orthostatic hypotension (disorder)"}],
                        "text": "Orthostatic hypotension (disorder)",
                    },
                    "onsetDateTime": "2025-10-14",
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-cond-falls",
                "resource": {
                    "resourceType": "Condition",
                    "id": "margaret-cond-falls",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "161898004", "display": "Falls (finding)"}],
                        "text": "Falls (finding)",
                    },
                    "onsetDateTime": "2026-01-28",
                },
            },
            {
                "fullUrl": "urn:uuid:smart-cond-sdoh-ipv",
                "resource": {
                    "resourceType": "Condition",
                    "id": "smart-cond-sdoh-ipv",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "706893006", "display": "Victim of intimate partner abuse (finding)"}],
                        "text": "Victim of intimate partner abuse (finding)",
                    },
                    "onsetDateTime": "2025-05-02",
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-cond-sdoh-iso",
                "resource": {
                    "resourceType": "Condition",
                    "id": "margaret-cond-sdoh-iso",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "55600006", "display": "Social isolation (finding)"}],
                        "text": "Social isolation (finding)",
                    },
                    "onsetDateTime": "2025-08-19",
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-dev-walker",
                "resource": {
                    "resourceType": "Device",
                    "id": "margaret-dev-walker",
                    "status": "active",
                    "type": {"text": "Four-wheeled rollator walker & bilateral digital hearing aids"},
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-med-ativan",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "margaret-med-ativan",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "197901", "display": "Ativan 1 MG Oral Tablet [lorazepam]"}],
                        "text": "Ativan 1 MG Oral Tablet [lorazepam]",
                    },
                    "authoredOn": "2025-11-14",
                    "performer": [{"actor": {"display": "Pacific Northwest Geriatrics"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-med-ambien",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "margaret-med-ambien",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "854873", "display": "Ambien 10 MG Oral Tablet [zolpidem]"}],
                        "text": "Ambien 10 MG Oral Tablet [zolpidem]",
                    },
                    "authoredOn": "2025-12-02",
                    "performer": [{"actor": {"display": "Overlake Sleep & Behavioral Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:smart-med-atorva",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "smart-med-atorva",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {"text": "Lipitor 40 MG Oral Tablet"},
                    "authoredOn": "2025-11-10",
                    "performer": [{"actor": {"display": "Pacific Northwest Geriatrics"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:smart-med-simva",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "smart-med-simva",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "312961", "display": "Simvastatin 20 MG Oral Tablet"}],
                        "text": "Simvastatin 20 MG Oral Tablet",
                    },
                    "authoredOn": "2025-04-02",
                    "performer": [{"actor": {"display": "Community Pharmacy Refill"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-med-glipizide",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "margaret-med-glipizide",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "310490", "display": "Glipizide 5 MG Extended Release Oral Tablet"}],
                        "text": "Glipizide 5 MG Extended Release Oral Tablet",
                    },
                    "authoredOn": "2025-06-15",
                    "performer": [{"actor": {"display": "Pacific Northwest Geriatrics"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-imm-pneumo-clinic",
                "resource": {
                    "resourceType": "Immunization",
                    "id": "margaret-imm-pneumo-clinic",
                    "status": "completed",
                    "vaccineCode": {
                        "coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": "216", "display": "Pneumococcal conjugate vaccine 20-valent (PCV20)"}],
                        "text": "Pneumococcal conjugate vaccine 20-valent (PCV20)",
                    },
                    "occurrenceDateTime": "2025-11-10",
                    "performer": [{"actor": {"display": "Pacific Northwest Geriatrics"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-imm-pneumo-iis",
                "resource": {
                    "resourceType": "Immunization",
                    "id": "margaret-imm-pneumo-iis",
                    "status": "completed",
                    "vaccineCode": {
                        "coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": "216", "display": "Pneumococcal conjugate vaccine 20-valent (PCV20)"}],
                        "text": "Pneumococcal conjugate vaccine 20-valent (PCV20)",
                    },
                    "occurrenceDateTime": "2025-11-12",
                    "performer": [{"actor": {"display": "WA State IIS Immunization Registry"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-obs-a1c-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "margaret-obs-a1c-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "4548-4", "display": "Hemoglobin A1c/Hemoglobin.total in Blood"}]},
                    "effectiveDateTime": "2024-07-18",
                    "valueQuantity": {"value": 7.4, "unit": "%"},
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-obs-a1c-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "margaret-obs-a1c-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "4548-4", "display": "Hemoglobin A1c/Hemoglobin.total in Blood"}]},
                    "effectiveDateTime": "2026-02-12",
                    "valueQuantity": {"value": 6.7, "unit": "%"},
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-obs-sbp-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "margaret-obs-sbp-1",
                    "status": "final",
                    "category": [{"coding": [{"code": "vital-signs"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6", "display": "Systolic blood pressure"}]},
                    "effectiveDateTime": "2026-01-28",
                    "valueQuantity": {"value": 96.0, "unit": "mm[Hg]"},
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-obs-sbp-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "margaret-obs-sbp-2",
                    "status": "final",
                    "category": [{"coding": [{"code": "vital-signs"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6", "display": "Systolic blood pressure"}]},
                    "effectiveDateTime": "2026-02-12",
                    "valueQuantity": {"value": 124.0, "unit": "mm[Hg]"},
                },
            },
            {
                "fullUrl": "urn:uuid:margaret-note-geriatrics",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "margaret-note-geriatrics",
                    "status": "current",
                    "type": {"text": "Geriatric Fall & Polypharmacy Consultation Note"},
                    "date": "2026-02-12T10:30:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": margaret_note_b64}}],
                },
            },
        ],
    }
    margaret_anomalies = [
        {
            "anomaly_id": "smart-predm-t2dm-conflict",
            "category": "mutually_exclusive_condition_stages",
            "severity": "high",
            "description": "Uncoded Prediabetes and coded Type 2 Diabetes Mellitus simultaneously active.",
            "expected_resource_uuids": ["smart-cond-predm", "smart-cond-t2dm"],
        },
        {
            "anomaly_id": "margaret-beers-sedative-dup",
            "category": "cross_source_therapeutic_class_duplicate",
            "severity": "high",
            "description": "Concurrent active Ativan 1 MG [lorazepam] and Ambien 10 MG [zolpidem] in an older adult with falls.",
            "expected_resource_uuids": ["margaret-med-ativan", "margaret-med-ambien"],
        },
        {
            "anomaly_id": "smart-statin-class-dup",
            "category": "cross_source_therapeutic_class_duplicate",
            "severity": "high",
            "description": "Uncoded Lipitor 40 MG and coded Simvastatin 20 MG simultaneously active.",
            "expected_resource_uuids": ["smart-med-atorva", "smart-med-simva"],
        },
        {
            "anomaly_id": "margaret-pneumo-iis-dup",
            "category": "cross_source_immunization_duplicate",
            "severity": "medium",
            "description": "PCV20 pneumococcal vaccine recorded 2 days apart across clinic and WA IIS registry.",
            "expected_resource_uuids": ["margaret-imm-pneumo-clinic", "margaret-imm-pneumo-iis"],
        },
        {
            "anomaly_id": "margaret-note-stop-sedatives-glipizide",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": "Geriatrics note instructs to discontinue Ambien and lorazepam and hold glipizide while structured orders remain active.",
            "expected_resource_uuids": ["margaret-med-ambien", "margaret-med-ativan", "margaret-med-glipizide", "margaret-note-geriatrics"],
        },
    ]
    return {
        "Elena_Rostova_Cardiorenal.json": (elena_bundle, elena_anomalies),
        "Arthur_Pendelton_Anticoag.json": (arthur_bundle, arthur_anomalies),
        "Margaret_Chen_Polypharmacy.json": (margaret_bundle, margaret_anomalies),
        "MIMIC_IV_Cardiorenal_Summary.json": (elena_bundle, elena_anomalies),
        "SMART_IPS_Transition_Summary.json": (margaret_bundle, margaret_anomalies),
    }


__all__ = ["build_deidentified_real_clinical_summaries"]
