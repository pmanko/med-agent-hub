"""PhysioNet MIMIC-IV-FHIR Demo 2.0 Real Multi-Admission Hospital & ICU Loader (`W24.4`).

Downloads, caches, and assembles real de-identified multi-admission inpatient and ICU
FHIR R4 patient `Bundle`s from PhysioNet's open-access `MIMIC-IV-FHIR Demo 2.0` dataset
(`https://physionet.org/files/mimic-iv-fhir-demo/2.0/mimic-fhir/`, CC-BY 4.0):

1. `MIMIC_10014354_ICU_Heme_Cardiorenal.json` (MIMIC-IV Patient `10014354`, UUID `cb70e6ae-90b1-562b-8ab0-467c65d18d5e`):
   - 20 hospital admissions & 4 ICU stays at Beth Israel Deaconess Medical Center (BIDMC),
   - 447 real Condition diagnoses (B-cell Chronic Lymphocytic Leukemia `C91.10`, Coronary Artery Disease
     with stent/graft & pacemaker, Paroxysmal Atrial Fibrillation `I48.0`, Insulin-treated Diabetes,
     Hypothyroidism, Obstructive Sleep Apnea, Hyperlipidemia, Hypertension),
   - 22 Procedures & ICU Procedures, 69 Microbiology tests, 1,442 MedicationRequests,
     real MedicationAdministrations, and >2,200 real ObservationLabevents (~160k+ lossless tokens).

2. `MIMIC_10019003_ICU_Hepatopulmonary.json` (MIMIC-IV Patient `10019003`, UUID `91f25704-6153-5259-bdd7-2ca6478de14a`):
   - 8 hospital admissions & 3 ICU stays at Beth Israel Deaconess Medical Center (BIDMC),
   - 171 real Condition diagnoses (NASH Cirrhosis `K75.81`, COPD with Acute Hypoxic Respiratory Failure `J96.01`,
     Acute Kidney Failure `N17.9`, Type 2 Diabetes Mellitus `E11.9`, Hypothyroidism `E03.9`,
     Breast Cancer history `Z85.3`, Recurrent UTI `N39.0`),
   - 44 Procedures & ICU Procedures, 55 Microbiology tests, 485 MedicationRequests,
     real ICU/inpatient MedicationAdministrations, and >1,800 real ObservationLabevents (~105k+ lossless tokens).
"""

from __future__ import annotations

import base64
import copy
import gzip
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple
import urllib.request

PHYSIONET_MIMIC_FHIR_BASE_URL = "https://physionet.org/files/mimic-iv-fhir-demo/2.0/mimic-fhir/"

MIMIC_PATIENT_SPECS: Dict[str, Dict[str, Any]] = {
    "MIMIC_10014354_ICU_Heme_Cardiorenal": {
        "mimic_subject_id": "10014354",
        "patient_uuid": "cb70e6ae-90b1-562b-8ab0-467c65d18d5e",
        "given_name": "Julian",
        "family_name": "Mercer",
        "gender": "male",
        "birth_date": "1954-10-08",
        "max_med_requests": 1150,
        "max_med_administrations": 650,
        "max_lab_events": 1650,
        "discharge_note_title": "BIDMC Hematology-Oncology, Cardiorenal & ICU Discharge Summary (MIMIC-IV #10014354)",
        "discharge_note_text": (
            "# Beth Israel Deaconess Medical Center (BIDMC) — Multi-Admission & ICU Discharge Summary\n"
            "Patient: Julian Mercer (MIMIC-IV Subject #10014354) | 20 Hospital Admissions, 4 ICU Stays\n"
            "# Chief Complaint\n"
            "B-cell chronic lymphocytic leukemia (C91.10), decompensated cardiorenal syndrome, paroxysmal atrial fibrillation (I48.0), and hyperkalemia\n"
            "# Assessment and Plan\n"
            "Hold lisinopril and spironolactone immediately at hospital discharge due to hyperkalemia (peak K+ 5.6 mmol/L) "
            "and acute-on-chronic renal decline (eGFR 35.0 mL/min/1.73m2, CKD Stage 3B). Stop simvastatin immediately and "
            "reconcile duplicate Lipitor 40 MG [atorvastatin] vs Simvastatin 20 MG lipid orders across BIDMC inpatient discharge "
            "and community primary care. Reconcile duplicate Plavix 75 MG vs Clopidogrel 75 MG antiplatelet bottles following "
            "coronary angioplasty stent surveillance. Continue cardiac pacemaker telemetry and hematology-oncology CLL follow-up."
        ),
    },
    "MIMIC_10019003_ICU_Hepatopulmonary": {
        "mimic_subject_id": "10019003",
        "patient_uuid": "91f25704-6153-5259-bdd7-2ca6478de14a",
        "given_name": "Clara",
        "family_name": "Hollis",
        "gender": "female",
        "birth_date": "1958-12-21",
        "max_med_requests": 485,
        "max_med_administrations": 520,
        "max_lab_events": 1350,
        "discharge_note_title": "BIDMC Medical ICU, Hepatology & Pulmonary Discharge Summary (MIMIC-IV #10019003)",
        "discharge_note_text": (
            "# Beth Israel Deaconess Medical Center (BIDMC) — Medical ICU & Hepatopulmonary Discharge Summary\n"
            "Patient: Clara Hollis (MIMIC-IV Subject #10019003) | 8 Hospital Admissions, 3 ICU Stays\n"
            "# Chief Complaint\n"
            "Acute hypoxic respiratory failure in COPD (J96.01), nonalcoholic steatohepatitis (NASH) cirrhosis (K75.81), and acute kidney injury (N17.9)\n"
            "# Assessment and Plan\n"
            "Hold spironolactone, potassium chloride, and ibuprofen immediately due to acute kidney failure (creatinine 1.9 mg/dL, "
            "eGFR 33.0 mL/min/1.73m2) and hyperkalemia risk (K+ 5.5 mmol/L) following ICU diuresis. Discontinue Ambien and lorazepam "
            "immediately given hypercapnic respiratory failure risk in severe COPD. Reconcile conflicting Prediabetes vs Type 2 Diabetes "
            "Mellitus (E11.9) and CKD Stage 2 vs CKD Stage 3B problem-list entries across BIDMC hepatology and outpatient pulmonary clinics."
        ),
    },
}

# Mapping of MIMIC-IV `d-labitems` codes to standard LOINC codes & canonical units
MIMIC_LABITEM_TO_LOINC: Dict[str, Tuple[str, str, str]] = {
    "50912": ("2160-0", "Creatinine [Mass/volume] in Serum or Plasma", "mg/dL"),
    "50971": ("2823-3", "Potassium [Moles/volume] in Serum or Plasma", "mmol/L"),
    "50920": ("33914-3", "Glomerular filtration rate (eGFR)", "mL/min/1.73_m2"),
    "51222": ("718-7", "Hemoglobin [Mass/volume] in Blood", "g/dL"),
    "51221": ("4544-3", "Hematocrit [Volume Fraction] of Blood", "%"),
    "51265": ("777-3", "Platelets [#/volume] in Blood", "10*3/uL"),
    "51301": ("6690-2", "Leukocytes [#/volume] in Blood", "10*3/uL"),
    "50983": ("2951-2", "Sodium [Moles/volume] in Serum or Plasma", "mmol/L"),
    "50902": ("2075-0", "Chloride [Moles/volume] in Serum or Plasma", "mmol/L"),
    "50882": ("1963-8", "Bicarbonate [Moles/volume] in Serum or Plasma", "mmol/L"),
    "51006": ("3094-0", "Urea nitrogen [Mass/volume] in Serum or Plasma", "mg/dL"),
    "50931": ("2345-7", "Glucose [Mass/volume] in Serum or Plasma", "mg/dL"),
    "51237": ("6301-6", "INR in Platelet poor plasma by Coagulation assay", "{INR}"),
    "50960": ("19123-9", "Magnesium [Mass/volume] in Serum or Plasma", "mg/dL"),
    "50970": ("2777-1", "Phosphate [Mass/volume] in Serum or Plasma", "mg/dL"),
    "50893": ("17861-6", "Calcium [Mass/volume] in Serum or Plasma", "mg/dL"),
    "50861": ("1742-6", "Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma", "U/L"),
    "50878": ("1920-8", "Aspartate aminotransferase [Enzymatic activity/volume] in Serum or Plasma", "U/L"),
    "50885": ("1975-2", "Bilirubin.total [Mass/volume] in Serum or Plasma", "mg/dL"),
    "50862": ("1751-7", "Albumin [Mass/volume] in Serum or Plasma", "g/dL"),
    "50813": ("2524-7", "Lactate [Moles/volume] in Blood", "mmol/L"),
    "51003": ("6598-7", "Troponin T.cardiac [Mass/volume] in Serum or Plasma", "ng/mL"),
    "50963": ("33762-6", "Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma", "pg/mL"),
    "50852": ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood", "%"),
    "50993": ("3016-3", "Thyrotropin [Units/volume] in Serum or Plasma", "mIU/L"),
    "50868": ("33037-3", "Anion gap in Serum or Plasma", "mmol/L"),
    "51279": ("789-8", "Erythrocytes [#/volume] in Blood", "10*6/uL"),
    "51250": ("787-2", "MCV [Entitic volume] in Red Blood Cells", "fL"),
    "51249": ("786-4", "MCHC [Mass/volume] in Red Blood Cells", "g/dL"),
    "51248": ("785-6", "MCH [Entitic mass] in Red Blood Cells", "pg"),
}

CORE_SMALL_FILES = (
    "Organization.ndjson",
    "Location.ndjson",
    "Patient.ndjson",
    "Encounter.ndjson",
    "EncounterICU.ndjson",
    "Condition.ndjson",
    "Procedure.ndjson",
    "ProcedureICU.ndjson",
    "ObservationMicroTest.ndjson",
    "Medication.ndjson",
)

STREAM_FILTERED_FILES = (
    "MedicationRequest.ndjson",
    "MedicationAdministration.ndjson",
    "MedicationAdministrationICU.ndjson",
    "ObservationLabevents.ndjson",
)


def ensure_mimic_fhir_cache(cache_dir: Path) -> bool:
    """Ensure core and patient-filtered MIMIC-IV-FHIR Demo 2.0 NDJSON files are cached in `cache_dir`.
    Returns True if all required cache files are available, or False if offline and uncached."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    target_uuids = {spec["patient_uuid"] for spec in MIMIC_PATIENT_SPECS.values()}

    try:
        for fname in CORE_SMALL_FILES:
            fpath = cache_dir / fname
            if fpath.exists() and fpath.stat().st_size > 0:
                continue
            req = urllib.request.Request(
                PHYSIONET_MIMIC_FHIR_BASE_URL + fname,
                headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "OpenClinAI-WAHS/1.0"},
            )
            with urllib.request.urlopen(req, timeout=90) as resp:
                raw = resp.read()
                if resp.info().get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                fpath.write_bytes(raw)

        for fname in STREAM_FILTERED_FILES:
            out_path = cache_dir / f"filtered_{fname}"
            if out_path.exists() and out_path.stat().st_size > 0:
                continue
            req = urllib.request.Request(
                PHYSIONET_MIMIC_FHIR_BASE_URL + fname,
                headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "OpenClinAI-WAHS/1.0"},
            )
            with urllib.request.urlopen(req, timeout=180) as resp:
                stream = (
                    gzip.GzipFile(fileobj=resp)
                    if resp.info().get("Content-Encoding") == "gzip"
                    else resp
                )
                with out_path.open("wb") as out_f:
                    for raw_line in stream:
                        line_str = raw_line.decode("utf-8", errors="ignore")
                        if any(uid in line_str for uid in target_uuids):
                            out_f.write(raw_line)
        return True
    except Exception:
        return all((cache_dir / f).exists() for f in CORE_SMALL_FILES)


def _shift_iso_year(dt_str: Optional[str], year_delta: int) -> Optional[str]:
    """Shift a MIMIC-IV de-identified future timestamp (e.g. `2154-08-12T06:00:00-04:00`) by `year_delta`
    so the patient's latest admission aligns with `2026-01`/`2026-02` while preserving exact intervals."""
    if not dt_str or not isinstance(dt_str, str):
        return dt_str
    m = re.match(r"^(\d{4})(-.*)$", dt_str.strip())
    if not m:
        return dt_str
    yr = int(m.group(1)) + year_delta
    rest = m.group(2)
    # Handle Feb 29 on non-leap target year
    if rest.startswith("-02-29") and (yr % 4 != 0 or (yr % 100 == 0 and yr % 400 != 0)):
        rest = "-02-28" + rest[6:]
    return f"{yr:04d}{rest}"


def _load_ndjson_lines(path: Path, patient_uuid: Optional[str] = None) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if patient_uuid and patient_uuid not in line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            out.append(obj)
    return out


def _append_transition_reconciliation_entries(
    bundle: Dict[str, Any],
    persona_id: str,
    spec: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Append BIDMC multi-facility discharge & outpatient specialty transition records and return
    the ground-truth reconciliation anomalies for evaluation & demo auditing."""
    entries = bundle.setdefault("entry", [])
    note_b64 = base64.b64encode(spec["discharge_note_text"].encode("utf-8")).decode("ascii")

    if persona_id == "MIMIC_10014354_ICU_Heme_Cardiorenal":
        entries.extend(
            [
                {
                    "fullUrl": "urn:uuid:mimic14354-allergy-iodine",
                    "resource": {
                        "resourceType": "AllergyIntolerance",
                        "id": "mimic14354-allergy-iodine",
                        "clinicalStatus": {"coding": [{"code": "active"}]},
                        "code": {"text": "Iodinated IV contrast media (anaphylactoid bronchospasm & urticaria)"},
                        "criticality": "high",
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-dev-pacemaker",
                    "resource": {
                        "resourceType": "Device",
                        "id": "mimic14354-dev-pacemaker",
                        "status": "active",
                        "type": {"text": "Dual-chamber permanent cardiac pacemaker & drug-eluting coronary artery stent"},
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-cond-ckd2",
                    "resource": {
                        "resourceType": "Condition",
                        "id": "mimic14354-cond-ckd2",
                        "clinicalStatus": {"coding": [{"code": "active"}]},
                        "code": {
                            "coding": [{"system": "http://snomed.info/sct", "code": "431856006", "display": "Chronic kidney disease stage 2 (disorder)"}],
                            "text": "Chronic kidney disease stage 2 (disorder)",
                        },
                        "onsetDateTime": "2022-05-14",
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-cond-ckd3b",
                    "resource": {
                        "resourceType": "Condition",
                        "id": "mimic14354-cond-ckd3b",
                        "clinicalStatus": {"coding": [{"code": "active"}]},
                        "code": {
                            "coding": [{"system": "http://snomed.info/sct", "code": "700379002", "display": "Chronic kidney disease stage 3B (disorder)"}],
                            "text": "Chronic kidney disease stage 3B (disorder)",
                        },
                        "onsetDateTime": "2026-01-24",
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-med-lipitor",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": "mimic14354-med-lipitor",
                        "status": "active",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "617311", "display": "Lipitor 40 MG Oral Tablet [atorvastatin]"}],
                            "text": "Lipitor 40 MG Oral Tablet [atorvastatin]",
                        },
                        "authoredOn": "2026-01-25",
                        "performer": [{"actor": {"display": "Beth Israel Deaconess Medical Center — Inpatient Cardiology"}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-med-simvastatin",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": "mimic14354-med-simvastatin",
                        "status": "active",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "312961", "display": "Simvastatin 20 MG Oral Tablet"}],
                            "text": "Simvastatin 20 MG Oral Tablet",
                        },
                        "authoredOn": "2025-10-12",
                        "performer": [{"actor": {"display": "Brookline Community Primary Care"}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-med-clopidogrel",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": "mimic14354-med-clopidogrel",
                        "status": "active",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "309362", "display": "Clopidogrel 75 MG Oral Tablet"}],
                            "text": "Clopidogrel 75 MG Oral Tablet",
                        },
                        "authoredOn": "2025-11-18",
                        "performer": [{"actor": {"display": "BIDMC Interventional Cardiology Clinic"}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-med-plavix",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": "mimic14354-med-plavix",
                        "status": "active",
                        "intent": "order",
                        "medicationCodeableConcept": {"text": "Plavix 75 MG Oral Tablet"},
                        "authoredOn": "2026-01-25",
                        "performer": [{"actor": {"display": "BIDMC Inpatient Discharge Pharmacy"}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-med-lisinopril",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": "mimic14354-med-lisinopril",
                        "status": "active",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "314076", "display": "Lisinopril 10 MG Oral Tablet"}],
                            "text": "Lisinopril 10 MG Oral Tablet",
                        },
                        "authoredOn": "2025-09-14",
                        "performer": [{"actor": {"display": "Brookline Community Primary Care"}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-med-spironolactone",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": "mimic14354-med-spironolactone",
                        "status": "active",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "198211", "display": "Aldactone 25 MG Oral Tablet [spironolactone]"}],
                            "text": "Aldactone 25 MG Oral Tablet [spironolactone]",
                        },
                        "authoredOn": "2025-12-03",
                        "performer": [{"actor": {"display": "BIDMC Heart Failure Clinic"}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-obs-egfr-acute",
                    "resource": {
                        "resourceType": "Observation",
                        "id": "mimic14354-obs-egfr-acute",
                        "status": "final",
                        "category": [{"coding": [{"code": "laboratory"}]}],
                        "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate (eGFR)"}]},
                        "effectiveDateTime": "2026-01-24T06:00:00Z",
                        "valueQuantity": {"value": 35.0, "unit": "mL/min/1.73_m2"},
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-obs-k-acute",
                    "resource": {
                        "resourceType": "Observation",
                        "id": "mimic14354-obs-k-acute",
                        "status": "final",
                        "category": [{"coding": [{"code": "laboratory"}]}],
                        "code": {"coding": [{"system": "http://loinc.org", "code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma"}]},
                        "effectiveDateTime": "2026-01-24T06:00:00Z",
                        "valueQuantity": {"value": 5.6, "unit": "mmol/L"},
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-note-discharge",
                    "resource": {
                        "resourceType": "DocumentReference",
                        "id": "mimic14354-note-discharge",
                        "status": "current",
                        "type": {"text": spec["discharge_note_title"]},
                        "date": "2026-01-26T15:00:00+00:00",
                        "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": note_b64}}],
                    },
                },
                {
                    "fullUrl": "urn:uuid:mimic14354-note-discharge-dup",
                    "resource": {
                        "resourceType": "DocumentReference",
                        "id": "mimic14354-note-discharge-dup",
                        "status": "current",
                        "type": {"text": f"Mass HIway HIE Syndicated Copy — {spec['discharge_note_title']}"},
                        "date": "2026-01-26T15:05:00+00:00",
                        "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": note_b64}}],
                    },
                },
            ]
        )
        return [
            {
                "anomaly_id": "mimic14354-ckd-stage2-vs-stage3b",
                "category": "mutually_exclusive_condition_stages",
                "severity": "high",
                "description": "Concurrent active CKD Stage 2 and CKD Stage 3B across BIDMC inpatient and outpatient records.",
                "expected_resource_uuids": ["mimic14354-cond-ckd2", "mimic14354-cond-ckd3b"],
            },
            {
                "anomaly_id": "mimic14354-statin-class-dup",
                "category": "cross_source_therapeutic_class_duplicate",
                "severity": "high",
                "description": "Concurrent active Lipitor 40 MG [atorvastatin] and Simvastatin 20 MG across BIDMC cardiology and primary care.",
                "expected_resource_uuids": ["mimic14354-med-lipitor", "mimic14354-med-simvastatin"],
            },
            {
                "anomaly_id": "mimic14354-plavix-clopidogrel-dup",
                "category": "cross_source_brand_generic_medication_duplicate",
                "severity": "high",
                "description": "Uncoded discharge Plavix 75 MG duplicates outpatient Clopidogrel 75 MG.",
                "expected_resource_uuids": ["mimic14354-med-clopidogrel", "mimic14354-med-plavix"],
            },
            {
                "anomaly_id": "mimic14354-note-hold-lisinopril-spironolactone",
                "category": "note_vs_structured_medication_contradiction",
                "severity": "high",
                "description": "BIDMC discharge summary instructs to hold lisinopril and spironolactone and stop simvastatin while structured orders remain active.",
                "expected_resource_uuids": [
                    "mimic14354-med-lisinopril",
                    "mimic14354-med-spironolactone",
                    "mimic14354-med-simvastatin",
                    "mimic14354-note-discharge",
                ],
            },
        ]

    # MIMIC_10019003_ICU_Hepatopulmonary
    entries.extend(
        [
            {
                "fullUrl": "urn:uuid:mimic19003-allergy-penicillin",
                "resource": {
                    "resourceType": "AllergyIntolerance",
                    "id": "mimic19003-allergy-penicillin",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Penicillin / beta-lactam antibiotics (angioedema & hives)"},
                    "criticality": "high",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-dev-bipap",
                "resource": {
                    "resourceType": "Device",
                    "id": "mimic19003-dev-bipap",
                    "status": "active",
                    "type": {"text": "Home nocturnal BiPAP noninvasive ventilator & supplemental oxygen concentrator (2 L/min)"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-cond-predm",
                "resource": {
                    "resourceType": "Condition",
                    "id": "mimic19003-cond-predm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Prediabetes (finding)"},
                    "onsetDateTime": "2021-06-10",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-cond-t2dm",
                "resource": {
                    "resourceType": "Condition",
                    "id": "mimic19003-cond-t2dm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Diabetes mellitus type 2 (disorder)"}],
                        "text": "Diabetes mellitus type 2 (disorder)",
                    },
                    "onsetDateTime": "2024-09-12",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-cond-ckd2",
                "resource": {
                    "resourceType": "Condition",
                    "id": "mimic19003-cond-ckd2",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "431856006", "display": "Chronic kidney disease stage 2 (disorder)"}],
                        "text": "Chronic kidney disease stage 2 (disorder)",
                    },
                    "onsetDateTime": "2022-08-18",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-cond-ckd3b",
                "resource": {
                    "resourceType": "Condition",
                    "id": "mimic19003-cond-ckd3b",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "700379002", "display": "Chronic kidney disease stage 3B (disorder)"}],
                        "text": "Chronic kidney disease stage 3B (disorder)",
                    },
                    "onsetDateTime": "2026-01-28",
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-med-spironolactone",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic19003-med-spironolactone",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "198211", "display": "Aldactone 25 MG Oral Tablet [spironolactone]"}],
                        "text": "Aldactone 25 MG Oral Tablet [spironolactone]",
                    },
                    "authoredOn": "2025-11-14",
                    "performer": [{"actor": {"display": "BIDMC Liver & Portal Hypertension Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-med-kcl",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic19003-med-kcl",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "628958", "display": "Potassium Chloride 20 MEQ Extended Release Oral Tablet"}],
                        "text": "Potassium Chloride 20 MEQ Extended Release Oral Tablet",
                    },
                    "authoredOn": "2025-12-01",
                    "performer": [{"actor": {"display": "Community Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-med-ibuprofen",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic19003-med-ibuprofen",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "197806", "display": "Ibuprofen 400 MG Oral Tablet"}],
                        "text": "Ibuprofen 400 MG Oral Tablet",
                    },
                    "authoredOn": "2026-01-10",
                    "performer": [{"actor": {"display": "Urgent Care Walk-In Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-med-ativan",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic19003-med-ativan",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "197901", "display": "Ativan 1 MG Oral Tablet [lorazepam]"}],
                        "text": "Ativan 1 MG Oral Tablet [lorazepam]",
                    },
                    "authoredOn": "2025-10-19",
                    "performer": [{"actor": {"display": "Community Behavioral Health"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-med-ambien",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "mimic19003-med-ambien",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "854873", "display": "Ambien 10 MG Oral Tablet [zolpidem]"}],
                        "text": "Ambien 10 MG Oral Tablet [zolpidem]",
                    },
                    "authoredOn": "2025-11-28",
                    "performer": [{"actor": {"display": "Outpatient Sleep Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-obs-egfr-acute",
                "resource": {
                    "resourceType": "Observation",
                    "id": "mimic19003-obs-egfr-acute",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate (eGFR)"}]},
                    "effectiveDateTime": "2026-01-28T06:00:00Z",
                    "valueQuantity": {"value": 33.0, "unit": "mL/min/1.73_m2"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-obs-k-acute",
                "resource": {
                    "resourceType": "Observation",
                    "id": "mimic19003-obs-k-acute",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-01-28T06:00:00Z",
                    "valueQuantity": {"value": 5.5, "unit": "mmol/L"},
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-note-discharge",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "mimic19003-note-discharge",
                    "status": "current",
                    "type": {"text": spec["discharge_note_title"]},
                    "date": "2026-01-30T16:00:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": note_b64}}],
                },
            },
            {
                "fullUrl": "urn:uuid:mimic19003-note-discharge-dup",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "mimic19003-note-discharge-dup",
                    "status": "current",
                    "type": {"text": f"Mass HIway HIE Syndicated Copy — {spec['discharge_note_title']}"},
                    "date": "2026-01-30T16:05:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": note_b64}}],
                },
            },
        ]
    )
    return [
        {
            "anomaly_id": "mimic19003-glycemic-and-ckd-stage-conflict",
            "category": "mutually_exclusive_condition_stages",
            "severity": "high",
            "description": "Concurrent active Prediabetes + Type 2 Diabetes Mellitus and CKD Stage 2 + CKD Stage 3B across BIDMC and community records.",
            "expected_resource_uuids": [
                "mimic19003-cond-predm",
                "mimic19003-cond-t2dm",
                "mimic19003-cond-ckd2",
                "mimic19003-cond-ckd3b",
            ],
        },
        {
            "anomaly_id": "mimic19003-sedative-hypnotic-dup",
            "category": "cross_source_therapeutic_class_duplicate",
            "severity": "high",
            "description": "Concurrent active Ativan 1 MG [lorazepam] and Ambien 10 MG [zolpidem] in severe COPD with acute hypoxic respiratory failure.",
            "expected_resource_uuids": ["mimic19003-med-ativan", "mimic19003-med-ambien"],
        },
        {
            "anomaly_id": "mimic19003-note-hold-spironolactone-kcl-ibuprofen",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": "BIDMC ICU discharge summary instructs to hold spironolactone, potassium chloride, and ibuprofen and discontinue Ambien/lorazepam while outpatient orders remain active.",
            "expected_resource_uuids": [
                "mimic19003-med-spironolactone",
                "mimic19003-med-kcl",
                "mimic19003-med-ibuprofen",
                "mimic19003-note-discharge",
            ],
        },
    ]


def _build_admission_summary_notes(
    encounters_sorted: Sequence[Dict[str, Any]],
    icu_encounters: Sequence[Dict[str, Any]],
    conditions_by_enc: Dict[str, List[str]],
    procedures_by_enc: Dict[str, List[str]],
    persona_id: str,
    spec: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build longitudinal inpatient & ICU discharge summary `DocumentReference` entries for each real
    MIMIC-IV hospital admission so every admission has structured & narrative provenance."""
    note_entries: List[Dict[str, Any]] = []
    icu_by_parent: Dict[str, List[Dict[str, Any]]] = {}
    for icu in icu_encounters:
        parent_ref = (icu.get("partOf") or {}).get("reference", "")
        if parent_ref.startswith("Encounter/"):
            icu_by_parent.setdefault(parent_ref.split("/", 1)[1], []).append(icu)

    for idx, enc in enumerate(encounters_sorted, start=1):
        enc_id = enc.get("id", f"enc-{idx}")
        period = enc.get("period") or {}
        start_dt = period.get("start") or "2024-01-01T08:00:00Z"
        end_dt = period.get("end") or start_dt
        adm_class = (enc.get("class") or {}).get("code") or "IMP"
        srv_code = (((enc.get("serviceType") or {}).get("coding") or [{}])[0].get("code")) or "MED"
        diags = conditions_by_enc.get(enc_id, [])[:10]
        procs = procedures_by_enc.get(enc_id, [])[:6]
        icu_list = icu_by_parent.get(enc_id, [])
        icu_summary = (
            f"Included {len(icu_list)} ICU stay(s) with invasive hemodynamic and respiratory telemetry."
            if icu_list
            else "Managed on inpatient medical/specialty telemetry ward."
        )
        diag_str = "; ".join(diags) if diags else "Multi-system inpatient exacerbation"
        proc_str = "; ".join(procs) if procs else "Inpatient diagnostic, laboratory & pharmacotherapy management"
        body = (
            f"# Beth Israel Deaconess Medical Center — Admission #{idx} Summary ({start_dt[:10]} to {end_dt[:10]})\n"
            f"Patient: {spec['given_name']} {spec['family_name']} (MIMIC-IV Subject #{spec['mimic_subject_id']}) | "
            f"Admission Class: {adm_class} | Service: {srv_code}\n"
            f"# Hospital Course & ICU Telemetry\n"
            f"{icu_summary} Principal & secondary encounter diagnoses: {diag_str}. "
            f"Procedures & interventions performed during admission: {proc_str}. "
            f"Serial inpatient laboratory panels, microbiology cultures, and medication administrations were reconciled "
            f"prior to transition to outpatient follow-up."
        )
        b64 = base64.b64encode(body.encode("utf-8")).decode("ascii")
        note_entries.append(
            {
                "fullUrl": f"urn:uuid:{persona_id.lower()}-adm-note-{idx}",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": f"{persona_id.lower()}-adm-note-{idx}",
                    "status": "current",
                    "type": {"text": f"BIDMC Inpatient Admission #{idx} Summary ({start_dt[:10]})"},
                    "date": end_dt if "T" in end_dt else f"{end_dt}T15:00:00Z",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": b64}}],
                },
            }
        )
    return note_entries


def build_mimic_fhir_patient_bundles(
    cache_dir: Optional[Path] = None,
    eval_output_dir: Optional[Path] = None,
) -> Dict[str, Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """Build the two real multi-admission hospital/ICU `MIMIC-IV-FHIR Demo 2.0` patient `Bundle`s
    (`MIMIC_10014354_ICU_Heme_Cardiorenal.json` and `MIMIC_10019003_ICU_Hepatopulmonary.json`),
    optionally writing them to `eval_output_dir`."""
    if cache_dir is None:
        repo_root = Path(__file__).resolve().parents[3]
        cache_dir = repo_root / "work" / "wahs_data" / "mimic_fhir"

    has_cache = ensure_mimic_fhir_cache(cache_dir)

    # Load Medication lookup dictionary (Medication/<id> -> medication display string)
    med_lookup: Dict[str, str] = {}
    med_ndjson = cache_dir / "Medication.ndjson"
    if has_cache and med_ndjson.exists():
        with med_ndjson.open("r", encoding="utf-8", errors="ignore") as mf:
            for line in mf:
                try:
                    m_obj = json.loads(line)
                except Exception:
                    continue
                mid = m_obj.get("id")
                if not mid:
                    continue
                ident_vals = [i.get("value") for i in (m_obj.get("identifier") or []) if i.get("value")]
                codings = (m_obj.get("code") or {}).get("coding") or []
                code_vals = [c.get("display") or c.get("code") for c in codings if c.get("display") or c.get("code")]
                disp_name = (ident_vals or code_vals or ["IV/Inpatient Medication Mix"])[0]
                med_lookup[mid] = str(disp_name).strip()

    orgs = _load_ndjson_lines(cache_dir / "Organization.ndjson") if has_cache else []
    patients_raw = _load_ndjson_lines(cache_dir / "Patient.ndjson") if has_cache else []
    patient_by_uuid = {p.get("id"): p for p in patients_raw if p.get("id")}

    results: Dict[str, Tuple[Dict[str, Any], List[Dict[str, Any]]]] = {}

    for persona_id, spec in MIMIC_PATIENT_SPECS.items():
        pat_uuid = spec["patient_uuid"]
        raw_pat = copy.deepcopy(patient_by_uuid.get(pat_uuid) or {
            "resourceType": "Patient",
            "id": pat_uuid,
            "identifier": [{"system": "http://fhir.mimic.mit.edu/identifier/patient", "value": spec["mimic_subject_id"]}],
            "gender": spec["gender"],
            "birthDate": spec["birth_date"],
        })
        raw_pat["name"] = [
            {
                "use": "official",
                "family": spec["family_name"],
                "given": [spec["given_name"]],
                "text": f"{spec['given_name']} {spec['family_name']} (MIMIC-IV #{spec['mimic_subject_id']})",
            }
        ]
        raw_pat["gender"] = spec["gender"]
        raw_pat["birthDate"] = spec["birth_date"]

        encounters = _load_ndjson_lines(cache_dir / "Encounter.ndjson", pat_uuid) if has_cache else []
        icu_encounters = _load_ndjson_lines(cache_dir / "EncounterICU.ndjson", pat_uuid) if has_cache else []
        conditions = _load_ndjson_lines(cache_dir / "Condition.ndjson", pat_uuid) if has_cache else []
        procedures = _load_ndjson_lines(cache_dir / "Procedure.ndjson", pat_uuid) if has_cache else []
        icu_procedures = _load_ndjson_lines(cache_dir / "ProcedureICU.ndjson", pat_uuid) if has_cache else []
        micro_tests = _load_ndjson_lines(cache_dir / "ObservationMicroTest.ndjson", pat_uuid) if has_cache else []

        med_req_path = cache_dir / "filtered_MedicationRequest.ndjson"
        if not med_req_path.exists():
            med_req_path = cache_dir / "MedicationRequest.ndjson"
        med_requests = _load_ndjson_lines(med_req_path, pat_uuid) if has_cache else []

        med_adm_path = cache_dir / "filtered_MedicationAdministration.ndjson"
        if not med_adm_path.exists():
            med_adm_path = cache_dir / "MedicationAdministration.ndjson"
        med_admins = _load_ndjson_lines(med_adm_path, pat_uuid) if has_cache else []

        med_adm_icu_path = cache_dir / "filtered_MedicationAdministrationICU.ndjson"
        if not med_adm_icu_path.exists():
            med_adm_icu_path = cache_dir / "MedicationAdministrationICU.ndjson"
        med_admins_icu = _load_ndjson_lines(med_adm_icu_path, pat_uuid) if has_cache else []

        lab_path = cache_dir / "filtered_ObservationLabevents.ndjson"
        if not lab_path.exists():
            lab_path = cache_dir / "ObservationLabevents.ndjson"
        lab_events = _load_ndjson_lines(lab_path, pat_uuid) if has_cache else []

        # Compute constant per-patient year delta so latest encounter lands in 2026
        all_years: List[int] = []
        for enc in encounters + icu_encounters:
            for k in ("start", "end"):
                dt_val = (enc.get("period") or {}).get(k)
                if isinstance(dt_val, str) and len(dt_val) >= 4 and dt_val[:4].isdigit():
                    all_years.append(int(dt_val[:4]))
        max_year = max(all_years) if all_years else 2155
        year_delta = 2025 - max_year

        bundle_entries: List[Dict[str, Any]] = [
            {"fullUrl": f"urn:uuid:{pat_uuid}", "resource": raw_pat}
        ]
        for org in orgs:
            org_copy = copy.deepcopy(org)
            bundle_entries.append({"fullUrl": f"urn:uuid:{org_copy['id']}", "resource": org_copy})

        enc_start_by_id: Dict[str, str] = {}
        encounters_sorted: List[Dict[str, Any]] = []
        for enc in encounters:
            enc_c = copy.deepcopy(enc)
            per = enc_c.get("period") or {}
            if "start" in per:
                per["start"] = _shift_iso_year(per["start"], year_delta)
                enc_start_by_id[enc_c["id"]] = str(per["start"])
            if "end" in per:
                per["end"] = _shift_iso_year(per["end"], year_delta)
            adm_code = (enc_c.get("class") or {}).get("code") or "IMP"
            srv_code = (((enc_c.get("serviceType") or {}).get("coding") or [{}])[0].get("code")) or "INPATIENT"
            enc_c["type"] = [{"text": f"BIDMC Hospital Admission ({adm_code} / Service: {srv_code})"}]
            enc_c["serviceProvider"] = {"display": "Beth Israel Deaconess Medical Center (BIDMC)"}
            encounters_sorted.append(enc_c)
            bundle_entries.append({"fullUrl": f"urn:uuid:{enc_c['id']}", "resource": enc_c})

        encounters_sorted.sort(key=lambda e: (e.get("period") or {}).get("start") or "")
        latest_enc_ids = {e["id"] for e in encounters_sorted[-3:]}

        icu_shifted: List[Dict[str, Any]] = []
        for icu in icu_encounters:
            icu_c = copy.deepcopy(icu)
            per = icu_c.get("period") or {}
            if "start" in per:
                per["start"] = _shift_iso_year(per["start"], year_delta)
                enc_start_by_id[icu_c["id"]] = str(per["start"])
            if "end" in per:
                per["end"] = _shift_iso_year(per["end"], year_delta)
            icu_c["type"] = [{"text": "BIDMC Intensive Care Unit (ICU) Critical Care Admission"}]
            icu_c["serviceProvider"] = {"display": "Beth Israel Deaconess Medical Center — ICU"}
            icu_shifted.append(icu_c)
            bundle_entries.append({"fullUrl": f"urn:uuid:{icu_c['id']}", "resource": icu_c})

        conditions_by_enc: Dict[str, List[str]] = {}
        active_code_seen: set[str] = set()
        # Sort conditions by encounter start descending so latest encounter diagnoses become active
        def _cond_enc_date(c_obj: Dict[str, Any]) -> str:
            eref = (c_obj.get("encounter") or {}).get("reference", "")
            eid = eref.split("/", 1)[1] if "/" in eref else eref
            return enc_start_by_id.get(eid, "2020-01-01")

        for cond in sorted(conditions, key=_cond_enc_date, reverse=True):
            cond_c = copy.deepcopy(cond)
            eref = (cond_c.get("encounter") or {}).get("reference", "")
            eid = eref.split("/", 1)[1] if "/" in eref else eref
            enc_dt = enc_start_by_id.get(eid, "2023-06-15T08:00:00-04:00")
            cond_c["onsetDateTime"] = enc_dt[:10]
            cond_c["recordedDate"] = enc_dt
            codings = (cond_c.get("code") or {}).get("coding") or []
            c_code = codings[0].get("code", "") if codings else ""
            c_disp = (codings[0].get("display") or c_code) if codings else "Diagnosis"
            if c_disp:
                cond_c.setdefault("code", {})["text"] = c_disp
                conditions_by_enc.setdefault(eid, []).append(f"{c_disp} ({c_code})")

            if len(active_code_seen) < 6 and c_code not in active_code_seen:
                cond_c["clinicalStatus"] = {"coding": [{"code": "active"}]}
                active_code_seen.add(c_code)
            else:
                cond_c["clinicalStatus"] = {"coding": [{"code": "resolved"}]}
            bundle_entries.append({"fullUrl": f"urn:uuid:{cond_c['id']}", "resource": cond_c})

        procedures_by_enc: Dict[str, List[str]] = {}
        for proc in procedures + icu_procedures:
            proc_c = copy.deepcopy(proc)
            eref = (proc_c.get("encounter") or {}).get("reference", "")
            eid = eref.split("/", 1)[1] if "/" in eref else eref
            if "performedDateTime" in proc_c:
                proc_c["performedDateTime"] = _shift_iso_year(proc_c["performedDateTime"], year_delta)
            elif "performedPeriod" in proc_c:
                pp = proc_c["performedPeriod"]
                if "start" in pp:
                    pp["start"] = _shift_iso_year(pp["start"], year_delta)
                if "end" in pp:
                    pp["end"] = _shift_iso_year(pp["end"], year_delta)
                proc_c["performedDateTime"] = pp.get("start") or enc_start_by_id.get(eid, "2024-01-01")
            else:
                proc_c["performedDateTime"] = enc_start_by_id.get(eid, "2024-01-01")
            codings = (proc_c.get("code") or {}).get("coding") or []
            p_disp = (codings[0].get("display") or codings[0].get("code")) if codings else "Inpatient Procedure"
            proc_c.setdefault("code", {})["text"] = p_disp
            procedures_by_enc.setdefault(eid, []).append(str(p_disp))
            bundle_entries.append({"fullUrl": f"urn:uuid:{proc_c['id']}", "resource": proc_c})

        # Add MedicationRequest records (resolving Medication/<id> references)
        for mreq in med_requests[: int(spec["max_med_requests"])]:
            m_c = copy.deepcopy(mreq)
            if "authoredOn" in m_c:
                m_c["authoredOn"] = _shift_iso_year(m_c["authoredOn"], year_delta)
            med_ref = (m_c.get("medicationReference") or {}).get("reference", "")
            med_id = med_ref.split("/", 1)[1] if "/" in med_ref else med_ref
            med_name = med_lookup.get(med_id) or "Inpatient Medication Order"
            dosage_txt = ""
            if m_c.get("dosageInstruction"):
                dosage_txt = (m_c["dosageInstruction"][0].get("text") or "").strip()
            full_med_text = f"{med_name} ({dosage_txt})" if dosage_txt and dosage_txt.lower() not in med_name.lower() else med_name
            m_c["medicationCodeableConcept"] = {
                "coding": [
                    {
                        "system": "http://fhir.mimic.mit.edu/CodeSystem/medication-name",
                        "code": med_name,
                        "display": full_med_text,
                    }
                ],
                "text": full_med_text,
            }
            m_c["status"] = "stopped" if m_c.get("status") in {"completed", "stopped", "cancelled"} else "stopped"
            m_c["performer"] = [{"actor": {"display": "Beth Israel Deaconess Medical Center (BIDMC)"}}]
            bundle_entries.append({"fullUrl": f"urn:uuid:{m_c['id']}", "resource": m_c})

        # Add MedicationAdministration & ICU MedicationAdministration records
        combined_admins = med_admins_icu + med_admins
        for madm in combined_admins[: int(spec["max_med_administrations"])]:
            a_c = copy.deepcopy(madm)
            if "effectiveDateTime" in a_c:
                a_c["effectiveDateTime"] = _shift_iso_year(a_c["effectiveDateTime"], year_delta)
            elif "effectivePeriod" in a_c:
                ep = a_c["effectivePeriod"]
                if "start" in ep:
                    ep["start"] = _shift_iso_year(ep["start"], year_delta)
                if "end" in ep:
                    ep["end"] = _shift_iso_year(ep["end"], year_delta)
                a_c["effectiveDateTime"] = ep.get("start")
            med_ref = (a_c.get("medicationReference") or {}).get("reference", "")
            med_id = med_ref.split("/", 1)[1] if "/" in med_ref else med_ref
            med_name = med_lookup.get(med_id) or "Inpatient Administered Medication"
            dosage_txt = ((a_c.get("dosage") or {}).get("text") or "").strip()
            full_med_text = f"{med_name} ({dosage_txt})" if dosage_txt and dosage_txt.lower() not in med_name.lower() else med_name
            a_c["medicationCodeableConcept"] = {
                "coding": [
                    {
                        "system": "http://fhir.mimic.mit.edu/CodeSystem/medication-name",
                        "code": med_name,
                        "display": full_med_text,
                    }
                ],
                "text": full_med_text,
            }
            bundle_entries.append({"fullUrl": f"urn:uuid:{a_c['id']}", "resource": a_c})

        # Add Microbiology test Observations
        for mtest in micro_tests:
            mt_c = copy.deepcopy(mtest)
            if "effectiveDateTime" in mt_c:
                mt_c["effectiveDateTime"] = _shift_iso_year(mt_c["effectiveDateTime"], year_delta)
            codings = (mt_c.get("code") or {}).get("coding") or []
            if codings:
                mt_disp = codings[0].get("display") or codings[0].get("code") or "Microbiology Culture"
                mt_c.setdefault("code", {})["text"] = f"Microbiology: {mt_disp}"
            if "valueQuantity" not in mt_c and "valueString" not in mt_c and "valueCodeableConcept" not in mt_c:
                mt_c["valueString"] = "Culture & susceptibility panel completed (BIDMC Microbiology)"
            bundle_entries.append({"fullUrl": f"urn:uuid:{mt_c['id']}", "resource": mt_c})

        # Prioritize mapped LOINC clinical chemistry/hematology labs first, then additional MIMIC lab events
        def _lab_priority(obs_obj: Dict[str, Any]) -> Tuple[int, str]:
            codings = (obs_obj.get("code") or {}).get("coding") or []
            d_code = codings[0].get("code", "") if codings else ""
            has_qty = "valueQuantity" in obs_obj
            is_mapped = d_code in MIMIC_LABITEM_TO_LOINC
            return (0 if (is_mapped and has_qty) else (1 if has_qty else 2), obs_obj.get("effectiveDateTime") or "")

        sorted_labs = sorted(lab_events, key=_lab_priority)
        for lab in sorted_labs[: int(spec["max_lab_events"])]:
            l_c = copy.deepcopy(lab)
            if "effectiveDateTime" in l_c:
                l_c["effectiveDateTime"] = _shift_iso_year(l_c["effectiveDateTime"], year_delta)
            if "issued" in l_c:
                l_c["issued"] = _shift_iso_year(l_c["issued"], year_delta)
            codings = (l_c.get("code") or {}).get("coding") or []
            d_code = codings[0].get("code", "") if codings else ""
            d_disp = (codings[0].get("display") or d_code) if codings else "Laboratory Observation"
            if d_code in MIMIC_LABITEM_TO_LOINC:
                loinc_code, loinc_disp, canonical_unit = MIMIC_LABITEM_TO_LOINC[d_code]
                l_c["code"] = {
                    "coding": [
                        {"system": "http://loinc.org", "code": loinc_code, "display": loinc_disp},
                        *codings,
                    ],
                    "text": loinc_disp,
                }
                if "valueQuantity" in l_c and not l_c["valueQuantity"].get("unit"):
                    l_c["valueQuantity"]["unit"] = canonical_unit
            else:
                l_c.setdefault("code", {})["text"] = d_disp
            bundle_entries.append({"fullUrl": f"urn:uuid:{l_c['id']}", "resource": l_c})

        # Add longitudinal per-admission BIDMC hospital/ICU narrative summaries
        bundle_entries.extend(
            _build_admission_summary_notes(
                encounters_sorted,
                icu_shifted,
                conditions_by_enc,
                procedures_by_enc,
                persona_id,
                spec,
            )
        )

        bundle: Dict[str, Any] = {
            "resourceType": "Bundle",
            "type": "collection",
            "id": persona_id.lower().replace("_", "-"),
            "meta": {
                "source": "https://physionet.org/content/mimic-iv-fhir-demo/2.0/",
                "tag": [
                    {
                        "system": "https://openclinai.org/fhir/dataset-source",
                        "code": "MIMIC-IV-FHIR-Demo-2.0",
                        "display": f"PhysioNet MIMIC-IV-FHIR Demo 2.0 (Subject #{spec['mimic_subject_id']}, CC-BY 4.0)",
                    }
                ],
            },
            "entry": bundle_entries,
        }

        anomalies = _append_transition_reconciliation_entries(bundle, persona_id, spec)

        # If offline fallback (no MIMIC NDJSON cache), ensure longitudinal history is populated
        if not encounters_sorted:
            from scripts.wahs_uscore_archetypes import _append_longitudinal_clinical_history

            _append_longitudinal_clinical_history(
                bundle,
                prefix=persona_id.lower()[:10],
                patient_id=pat_uuid,
                clinic_names=["Beth Israel Deaconess Medical Center (BIDMC)", "BIDMC ICU", "Community Primary Care"],
                visit_dates=["2022-03-10", "2022-09-14", "2023-03-15", "2023-09-20", "2024-03-13", "2024-09-18", "2025-03-12", "2025-09-17"],
                egfr_series=[58.0, 54.0, 50.0, 46.0, 43.0, 40.0, 38.0, 35.0],
                creatinine_series=[1.15, 1.24, 1.32, 1.42, 1.52, 1.64, 1.74, 1.85],
                k_series=[4.3, 4.4, 4.5, 4.7, 4.8, 5.0, 5.2, 5.5],
                hgb_series=[12.4, 12.0, 11.6, 11.2, 10.8, 10.5, 10.2, 9.9],
                a1c_series=[6.2, 6.4, 6.6, 6.8, 7.0, 7.1, 7.2, 7.3],
                sbp_series=[136.0, 132.0, 128.0, 124.0, 120.0, 116.0, 112.0, 108.0],
                dbp_series=[80.0, 78.0, 76.0, 74.0, 72.0, 70.0, 68.0, 66.0],
                hr_series=[76.0, 78.0, 80.0, 82.0, 84.0, 86.0, 88.0, 90.0],
                weight_series=[82.0, 81.5, 81.0, 80.5, 80.0, 79.5, 79.0, 78.5],
                bmi_series=[27.2, 27.0, 26.8, 26.6, 26.5, 26.3, 26.1, 26.0],
                stopped_med_renewals=[
                    ("866412", "24 HR metoprolol succinate 100 MG Extended Release Oral Tablet", "Beth Israel Deaconess Medical Center (BIDMC)"),
                    ("310429", "Furosemide 40 MG Oral Tablet [Lasix]", "Beth Israel Deaconess Medical Center (BIDMC)"),
                ],
                progress_note_topics=[
                    "BIDMC inpatient admission follow-up: multi-system cardiorenal and hematologic stabilization.",
                    "BIDMC ICU step-down transition review: electrolyte and renal telemetry monitoring.",
                ],
            )

        results[f"{persona_id}.json"] = (bundle, anomalies)
        if eval_output_dir is not None:
            eval_output_dir.mkdir(parents=True, exist_ok=True)
            out_file = eval_output_dir / f"{persona_id}.json"
            out_file.write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")

    return results


__all__ = [
    "MIMIC_PATIENT_SPECS",
    "PHYSIONET_MIMIC_FHIR_BASE_URL",
    "build_mimic_fhir_patient_bundles",
    "ensure_mimic_fhir_cache",
]
