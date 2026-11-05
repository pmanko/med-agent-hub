"""Synthetic US Core / IPS Multi-Clinic Transition Archetype Builders for WAHS+.

Defines three synthetic non-Synthea US Core / IPS clinical-summary transition patient bundles:
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
from typing import Any, Dict, List, Sequence, Tuple


def _append_longitudinal_clinical_history(
    bundle: Dict[str, Any],
    *,
    prefix: str,
    patient_id: str,
    clinic_names: Sequence[str],
    visit_dates: Sequence[str],
    egfr_series: Sequence[float],
    creatinine_series: Sequence[float],
    k_series: Sequence[float],
    hgb_series: Sequence[float],
    a1c_series: Sequence[float],
    sbp_series: Sequence[float],
    dbp_series: Sequence[float],
    hr_series: Sequence[float],
    weight_series: Sequence[float],
    bmi_series: Sequence[float],
    stopped_med_renewals: Sequence[Tuple[str, str, str]],
    progress_note_topics: Sequence[str],
) -> None:
    """Append deterministic 5-year longitudinal outpatient visits, serial lab/vital panels,
    historical stopped medication renewals, immunizations, progress notes, duplicate HIE notes,
    clinic equipment devices, and administrative Claims/Provenance to a US Core patient bundle."""
    entries = bundle.setdefault("entry", [])

    # 1. Lookup Organizations, Practitioners & Provenance (16 entries)
    for idx, cname in enumerate(clinic_names, start=1):
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-org-long-{idx}",
                "resource": {
                    "resourceType": "Organization",
                    "id": f"{prefix}-org-long-{idx}",
                    "name": cname,
                },
            }
        )
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-prac-long-{idx}",
                "resource": {
                    "resourceType": "Practitioner",
                    "id": f"{prefix}-prac-long-{idx}",
                    "name": [{"family": f"Clinician{idx}", "given": ["Attending"]}],
                },
            }
        )
    for idx in range(1, 9):
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-prov-long-{idx}",
                "resource": {
                    "resourceType": "Provenance",
                    "id": f"{prefix}-prov-long-{idx}",
                    "recorded": f"2024-0{idx}-15T12:00:00Z",
                    "agent": [{"who": {"display": clinic_names[idx % len(clinic_names)]}}],
                },
            }
        )

    # 2. Transient clinic room equipment Device entries (4 entries)
    room_devices = [
        ("Hospital bed, device", "91537007"),
        ("Examination table, device", "706172005"),
        ("Radiography system, device", "706173000"),
        ("Hospital bed, electric", "91537007"),
    ]
    for idx, (dev_txt, dev_code) in enumerate(room_devices, start=1):
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-room-dev-{idx}",
                "resource": {
                    "resourceType": "Device",
                    "id": f"{prefix}-room-dev-{idx}",
                    "status": "active",
                    "type": {
                        "coding": [{"system": "http://snomed.info/sct", "code": dev_code, "display": dev_txt}],
                        "text": dev_txt,
                    },
                },
            }
        )

    # 3. Longitudinal outpatient Encounters, Lab/Vital Panels & Claims across visit_dates
    for i, vdate in enumerate(visit_dates):
        idx = i + 1
        cname = clinic_names[i % len(clinic_names)]
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-enc-long-{idx}",
                "resource": {
                    "resourceType": "Encounter",
                    "id": f"{prefix}-enc-long-{idx}",
                    "status": "finished",
                    "class": {"code": "AMB"},
                    "type": [{"text": f"Longitudinal Outpatient Follow-Up Visit ({cname})"}],
                    "period": {"start": vdate, "end": vdate},
                    "serviceProvider": {"display": cname},
                },
            }
        )
        # Administrative Claim + EOB pair for each visit (non-clinical billing bloat)
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-claim-long-{idx}",
                "resource": {
                    "resourceType": "Claim",
                    "id": f"{prefix}-claim-long-{idx}",
                    "status": "active",
                    "use": "claim",
                    "patient": {"reference": f"urn:uuid:{patient_id}"},
                    "billablePeriod": {"start": vdate, "end": vdate},
                    "provider": {"display": cname},
                },
            }
        )
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-eob-long-{idx}",
                "resource": {
                    "resourceType": "ExplanationOfBenefit",
                    "id": f"{prefix}-eob-long-{idx}",
                    "status": "active",
                    "use": "claim",
                    "patient": {"reference": f"urn:uuid:{patient_id}"},
                    "created": vdate,
                },
            }
        )

        # Serial Lab Observations (known KB LOINC codes)
        egfr_val = egfr_series[i % len(egfr_series)]
        creat_val = creatinine_series[i % len(creatinine_series)]
        k_val = k_series[i % len(k_series)]
        hgb_val = hgb_series[i % len(hgb_series)]
        a1c_val = a1c_series[i % len(a1c_series)]
        sbp_val = sbp_series[i % len(sbp_series)]
        dbp_val = dbp_series[i % len(dbp_series)]
        hr_val = hr_series[i % len(hr_series)]
        wt_val = weight_series[i % len(weight_series)]
        bmi_val = bmi_series[i % len(bmi_series)]

        obs_specs = [
            ("egfr", "laboratory", "33914-3", "Glomerular filtration rate (eGFR)", egfr_val, "mL/min/1.73_m2"),
            ("creat-ser", "laboratory", "2160-0", "Creatinine [Mass/volume] in Serum or Plasma", creat_val, "mg/dL"),
            ("creat-bld", "laboratory", "38483-4", "Creatinine [Mass/volume] in Blood", round(creat_val + 0.02, 2), "mg/dL"),
            ("k-ser", "laboratory", "2823-3", "Potassium [Moles/volume] in Serum or Plasma", k_val, "mmol/L"),
            ("k-bld", "laboratory", "6298-4", "Potassium [Moles/volume] in Blood", round(k_val - 0.05, 2), "mmol/L"),
            ("hgb", "laboratory", "718-7", "Hemoglobin [Mass/volume] in Blood", hgb_val, "g/dL"),
            ("a1c", "laboratory", "4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood", a1c_val, "%"),
            ("mg", "laboratory", "19123-9", "Magnesium [Mass/volume] in Serum or Plasma", round(1.9 + (i % 3) * 0.1, 1), "mg/dL"),
            ("phos", "laboratory", "2777-1", "Phosphate [Mass/volume] in Serum or Plasma", round(3.4 + (i % 4) * 0.15, 2), "mg/dL"),
            ("tsh", "laboratory", "3016-3", "Thyrotropin [Units/volume] in Serum or Plasma", round(2.1 + (i % 3) * 0.2, 2), "mIU/L"),
            ("sbp", "vital-signs", "8480-6", "Systolic blood pressure", sbp_val, "mm[Hg]"),
            ("dbp", "vital-signs", "8462-4", "Diastolic blood pressure", dbp_val, "mm[Hg]"),
            ("hr", "vital-signs", "8867-4", "Heart rate", hr_val, "/min"),
            ("wt", "vital-signs", "29463-7", "Body Weight", wt_val, "kg"),
            ("bmi", "vital-signs", "39156-5", "Body mass index (BMI) [Ratio]", bmi_val, "kg/m2"),
        ]
        for short_key, cat_code, loinc_code, disp, val, unit in obs_specs:
            entries.append(
                {
                    "fullUrl": f"urn:uuid:{prefix}-obs-long-{short_key}-{idx}",
                    "resource": {
                        "resourceType": "Observation",
                        "id": f"{prefix}-obs-long-{short_key}-{idx}",
                        "status": "final",
                        "category": [{"coding": [{"code": cat_code}]}],
                        "code": {
                            "coding": [{"system": "http://loinc.org", "code": loinc_code, "display": disp}],
                            "text": disp,
                        },
                        "effectiveDateTime": f"{vdate}T09:00:00Z",
                        "valueQuantity": {"value": val, "unit": unit},
                        "performer": [{"display": cname}],
                    },
                }
            )

    # 4. Historical stopped MedicationRequest renewals across 2021-2025
    renewal_dates = [
        "2021-03-10", "2021-09-14", "2022-03-18", "2022-09-22",
        "2023-03-15", "2023-09-19", "2024-03-12", "2024-09-17",
    ]
    for m_idx, (rx_code, rx_disp, prescriber) in enumerate(stopped_med_renewals, start=1):
        for r_idx, r_date in enumerate(renewal_dates, start=1):
            entries.append(
                {
                    "fullUrl": f"urn:uuid:{prefix}-med-hist-{m_idx}-{r_idx}",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": f"{prefix}-med-hist-{m_idx}-{r_idx}",
                        "status": "stopped",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [
                                {
                                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                    "code": rx_code,
                                    "display": rx_disp,
                                }
                            ],
                            "text": rx_disp,
                        },
                        "authoredOn": r_date,
                        "performer": [{"actor": {"display": prescriber}}],
                    },
                }
            )

    # 5. Longitudinal routine Immunizations (spaced by >= 1 year)
    imm_specs = [
        ("flu-2021", "140", "Influenza, split virus, trivalent, preservative free", "2021-10-14"),
        ("flu-2022", "140", "Influenza, split virus, trivalent, preservative free", "2022-10-19"),
        ("flu-2023", "140", "Influenza, split virus, trivalent, preservative free", "2023-10-11"),
        ("flu-2024", "140", "Influenza, split virus, trivalent, preservative free", "2024-10-16"),
        ("covid-2021", "208", "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose", "2021-04-12"),
        ("covid-2023", "208", "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose", "2023-09-25"),
        ("tdap-2020", "115", "Tdap (Tetanus toxoid, reduced diphtheria toxoid, and acellular pertussis vaccine, adsorbed)", "2020-08-18"),
        ("shingrix-2022", "187", "Zoster vaccine recombinant, adjuvanted (Shingrix)", "2022-05-20"),
    ]
    for imm_key, cvx_code, cvx_disp, imm_date in imm_specs:
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-imm-long-{imm_key}",
                "resource": {
                    "resourceType": "Immunization",
                    "id": f"{prefix}-imm-long-{imm_key}",
                    "status": "completed",
                    "vaccineCode": {
                        "coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": cvx_code, "display": cvx_disp}],
                        "text": cvx_disp,
                    },
                    "occurrenceDateTime": imm_date,
                    "performer": [{"actor": {"display": clinic_names[0]}}],
                },
            }
        )

    # 6. Longitudinal outpatient progress notes + HIE byte-identical duplicate notes
    note_dates = ["2022-06-14", "2023-02-18", "2023-11-09", "2024-05-21", "2024-11-12", "2025-04-15"]
    for n_idx, (n_date, topic) in enumerate(zip(note_dates, progress_note_topics), start=1):
        note_body = (
            f"# Outpatient Subspecialty Progress Note ({n_date})\n"
            f"Clinic: {clinic_names[n_idx % len(clinic_names)]}\n"
            f"# Interval History & Longitudinal Assessment\n{topic}\n"
            f"Continue routine longitudinal laboratory monitoring and care coordination across clinics."
        )
        b64_payload = base64.b64encode(note_body.encode("utf-8")).decode("ascii")
        entries.append(
            {
                "fullUrl": f"urn:uuid:{prefix}-note-long-{n_idx}",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": f"{prefix}-note-long-{n_idx}",
                    "status": "current",
                    "type": {"text": f"Outpatient Longitudinal Progress Note ({n_date})"},
                    "date": f"{n_date}T14:00:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": b64_payload}}],
                },
            }
        )
        if n_idx <= 3:
            entries.append(
                {
                    "fullUrl": f"urn:uuid:{prefix}-note-long-dup-{n_idx}",
                    "resource": {
                        "resourceType": "DocumentReference",
                        "id": f"{prefix}-note-long-dup-{n_idx}",
                        "status": "current",
                        "type": {"text": f"HIE Syndicated Copy — Progress Note ({n_date})"},
                        "date": f"{n_date}T14:05:00+00:00",
                        "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": b64_payload}}],
                    },
                }
            )


def _append_marcus_high_volume_cardiooncology_cycles(bundle: Dict[str, Any]) -> None:
    """Append 56 longitudinal chemo-immunotherapy infusion & cardio-oncology surveillance cycles (2021-2025)
    so Marcus Vance's multi-clinic oncology/cardiorenal chart spans >85,000 lossless tokens (`W24.4`)."""
    entries = bundle.setdefault("entry", [])
    clinics = [
        "UW Thoracic Oncology Infusion Center",
        "Fred Hutch Cardio-Oncology & Arrhythmia Clinic",
        "Puget Sound Regional Nephrology & Primary Care",
    ]
    cycle_dates = [
        f"{yr}-{mo:02d}-{day:02d}"
        for yr in (2021, 2022, 2023, 2024, 2025)
        for mo in range(1, 13)
        for day in ((8,) if yr == 2021 and mo <= 4 else (8, 22) if yr in (2023, 2024) else (10,))
    ][:56]

    cycle_med_specs = [
        ("1740467", "2 ML Ondansetron 2 MG/ML Injection"),
        ("866412", "24 HR metoprolol succinate 100 MG Extended Release Oral Tablet"),
        ("310429", "Furosemide 40 MG Oral Tablet [Lasix]"),
        ("1656340", "sacubitril 49 MG / valsartan 51 MG Oral Tablet [Entresto]"),
    ]

    for idx, cdate in enumerate(cycle_dates, start=1):
        cname = clinics[idx % len(clinics)]
        egfr = round(max(36.5, 71.5 - idx * 0.62), 1)
        creat = round(min(1.88, 1.01 + idx * 0.015), 2)
        k_val = round(4.1 + (idx % 6) * 0.12, 2)
        hgb = round(max(10.1, 13.7 - (idx % 9) * 0.32 - idx * 0.03), 1)
        bnp = round(240.0 + idx * 52.0 + (idx % 4) * 85.0, 1)
        sbp = round(135.0 - (idx % 7) * 3.5, 1)
        dbp = round(81.0 - (idx % 6) * 2.0, 1)
        hr = round(74.0 + (idx % 8) * 3.5, 1)
        wt = round(87.8 - idx * 0.19, 1)

        entries.append(
            {
                "fullUrl": f"urn:uuid:marcus-enc-cycle-{idx}",
                "resource": {
                    "resourceType": "Encounter",
                    "id": f"marcus-enc-cycle-{idx}",
                    "status": "finished",
                    "class": {"code": "AMB"},
                    "type": [{"text": f"Cycle {idx} Thoracic Oncology Infusion & Cardio-Oncology Telemetry Visit ({cname})"}],
                    "period": {"start": f"{cdate}T08:00:00Z", "end": f"{cdate}T15:30:00Z"},
                    "serviceProvider": {"display": cname},
                },
            }
        )
        entries.append(
            {
                "fullUrl": f"urn:uuid:marcus-proc-cycle-{idx}",
                "resource": {
                    "resourceType": "Procedure",
                    "id": f"marcus-proc-cycle-{idx}",
                    "status": "completed",
                    "code": {
                        "text": (
                            f"Cycle {idx} Subcutaneous PowerPort access, serial 12-lead ECG & transthoracic strain echocardiography "
                            f"surveillance prior to thoracic immunotherapy administration"
                        )
                    },
                    "performedDateTime": f"{cdate}T09:15:00Z",
                },
            }
        )

        obs_cycle = [
            ("egfr", "laboratory", "33914-3", "Glomerular filtration rate (eGFR)", egfr, "mL/min/1.73_m2"),
            ("creat-ser", "laboratory", "2160-0", "Creatinine [Mass/volume] in Serum or Plasma", creat, "mg/dL"),
            ("creat-bld", "laboratory", "38483-4", "Creatinine [Mass/volume] in Blood", round(creat + 0.02, 2), "mg/dL"),
            ("k-ser", "laboratory", "2823-3", "Potassium [Moles/volume] in Serum or Plasma", k_val, "mmol/L"),
            ("k-bld", "laboratory", "6298-4", "Potassium [Moles/volume] in Blood", round(k_val - 0.04, 2), "mmol/L"),
            ("hgb", "laboratory", "718-7", "Hemoglobin [Mass/volume] in Blood", hgb, "g/dL"),
            ("bnp", "laboratory", "33762-6", "Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma", bnp, "pg/mL"),
            ("a1c", "laboratory", "4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood", round(5.8 + (idx % 5) * 0.1, 1), "%"),
            ("mg", "laboratory", "19123-9", "Magnesium [Mass/volume] in Serum or Plasma", round(1.8 + (idx % 4) * 0.1, 1), "mg/dL"),
            ("phos", "laboratory", "2777-1", "Phosphate [Mass/volume] in Serum or Plasma", round(3.3 + (idx % 5) * 0.12, 2), "mg/dL"),
            ("tsh", "laboratory", "3016-3", "Thyrotropin [Units/volume] in Serum or Plasma", round(2.2 + (idx % 6) * 0.35, 2), "mIU/L"),
            ("sbp", "vital-signs", "8480-6", "Systolic blood pressure", sbp, "mm[Hg]"),
            ("dbp", "vital-signs", "8462-4", "Diastolic blood pressure", dbp, "mm[Hg]"),
            ("hr", "vital-signs", "8867-4", "Heart rate", hr, "/min"),
            ("wt", "vital-signs", "29463-7", "Body Weight", wt, "kg"),
            ("bmi", "vital-signs", "39156-5", "Body mass index (BMI) [Ratio]", round(wt / (1.78 * 1.78), 1), "kg/m2"),
        ]
        for short_key, cat_code, loinc_code, disp, val, unit in obs_cycle:
            entries.append(
                {
                    "fullUrl": f"urn:uuid:marcus-obs-cycle-{short_key}-{idx}",
                    "resource": {
                        "resourceType": "Observation",
                        "id": f"marcus-obs-cycle-{short_key}-{idx}",
                        "status": "final",
                        "category": [{"coding": [{"code": cat_code}]}],
                        "code": {
                            "coding": [{"system": "http://loinc.org", "code": loinc_code, "display": disp}],
                            "text": disp,
                        },
                        "effectiveDateTime": f"{cdate}T08:30:00Z",
                        "valueQuantity": {"value": val, "unit": unit},
                        "performer": [{"display": cname}],
                    },
                }
            )

        for m_slot in range(2):
            rx_code, rx_disp = cycle_med_specs[(idx + m_slot) % len(cycle_med_specs)]
            entries.append(
                {
                    "fullUrl": f"urn:uuid:marcus-med-cycle-{idx}-{m_slot + 1}",
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": f"marcus-med-cycle-{idx}-{m_slot + 1}",
                        "status": "stopped",
                        "intent": "order",
                        "medicationCodeableConcept": {
                            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": rx_code, "display": rx_disp}],
                            "text": rx_disp,
                        },
                        "authoredOn": cdate,
                        "performer": [{"actor": {"display": cname}}],
                    },
                }
            )

        cycle_note_text = (
            f"# Thoracic Oncology & Cardio-Oncology Cycle {idx} Multidisciplinary Note ({cdate})\n"
            f"Facility: {cname}\n"
            f"# Interval Oncologic & Cardiorenal Telemetry Assessment\n"
            f"Patient Marcus Vance presented for Cycle {idx} thoracic chemo-immunotherapy and cardio-oncology surveillance on {cdate}. "
            f"Pre-infusion renal panel demonstrated eGFR {egfr} mL/min/1.73m2 (serum creatinine {creat} mg/dL, potassium {k_val} mmol/L), "
            f"hemoglobin {hgb} g/dL, and serial NT-proBNP {bnp} pg/mL. Vital signs recorded blood pressure {sbp}/{dbp} mmHg, heart rate {hr} bpm, "
            f"and weight {wt} kg. Subcutaneous chest wall PowerPort flushed briskly with normal saline without erythema or thrombosis. "
            f"Implantable cardiac loop recorder interrogation reviewed for subclinical atrial fibrillation burden and checkpoint-inhibitor "
            f"myocarditis screening; left ventricular global longitudinal strain and biomarker trajectory documented across UW Thoracic Oncology "
            f"and Fred Hutch Cardio-Oncology co-management."
        )
        cycle_b64 = base64.b64encode(cycle_note_text.encode("utf-8")).decode("ascii")
        entries.append(
            {
                "fullUrl": f"urn:uuid:marcus-note-cycle-{idx}",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": f"marcus-note-cycle-{idx}",
                    "status": "current",
                    "type": {"text": f"Cycle {idx} Thoracic Oncology & Cardio-Oncology Progress Note ({cdate})"},
                    "date": f"{cdate}T15:00:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": cycle_b64}}],
                },
            }
        )


def build_deidentified_real_clinical_summaries() -> Dict[str, Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """Pathway B: Three synthetic multi-domain US Core / IPS clinical-summary transition patient bundles
    (Elena Rostova — Cardiorenal HFrEF & Hyperkalemia; Arthur Pendelton — AFib + Mechanical Valve
    Dual-Anticoagulation & Upper GI Bleed; Margaret Chen — Geriatric Polypharmacy, Beers Criteria
    CNS Sedation & Falls), plus backward-compatible MIMIC-IV / SMART IPS alias fixture filenames."""
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

    common_visit_dates = [
        "2021-02-10", "2021-05-14", "2021-08-19", "2021-11-17",
        "2022-02-16", "2022-05-18", "2022-08-24", "2022-11-15",
        "2023-02-14", "2023-05-17", "2023-08-16", "2023-11-15",
        "2024-02-14", "2024-05-15", "2024-08-21", "2024-11-13",
        "2025-02-12", "2025-05-14", "2025-08-13", "2025-10-15",
    ]

    _append_longitudinal_clinical_history(
        elena_bundle,
        prefix="elena",
        patient_id="mimic-pat-1001",
        clinic_names=["UW Medicine Heart Institute", "Northwest Kidney Centers", "Community Primary Care"],
        visit_dates=common_visit_dates,
        egfr_series=[62.0, 60.5, 59.0, 58.0, 56.5, 55.0, 53.5, 52.0, 50.0, 48.5, 47.0, 45.5, 44.0, 43.0, 42.0, 41.0, 40.0, 39.5, 39.0, 38.0],
        creatinine_series=[1.02, 1.05, 1.08, 1.10, 1.14, 1.18, 1.22, 1.25, 1.29, 1.33, 1.36, 1.40, 1.44, 1.48, 1.52, 1.55, 1.58, 1.61, 1.64, 1.68],
        k_series=[4.1, 4.2, 4.2, 4.3, 4.3, 4.4, 4.4, 4.5, 4.5, 4.6, 4.6, 4.7, 4.7, 4.8, 4.8, 4.9, 4.9, 5.0, 5.1, 4.9],
        hgb_series=[12.8, 12.7, 12.6, 12.5, 12.4, 12.3, 12.2, 12.1, 12.0, 11.9, 11.8, 11.8, 11.7, 11.6, 11.5, 11.5, 11.4, 11.3, 11.2, 11.2],
        a1c_series=[5.8, 5.8, 5.9, 5.9, 6.0, 6.0, 6.1, 6.1, 6.1, 6.2, 6.2, 6.2, 6.3, 6.3, 6.3, 6.4, 6.4, 6.4, 6.4, 6.4],
        sbp_series=[138.0, 136.0, 134.0, 132.0, 130.0, 128.0, 126.0, 128.0, 124.0, 122.0, 120.0, 118.0, 116.0, 118.0, 114.0, 112.0, 110.0, 108.0, 106.0, 104.0],
        dbp_series=[84.0, 82.0, 80.0, 80.0, 78.0, 78.0, 76.0, 76.0, 74.0, 74.0, 72.0, 72.0, 70.0, 70.0, 68.0, 68.0, 66.0, 66.0, 64.0, 64.0],
        hr_series=[78.0, 76.0, 74.0, 72.0, 74.0, 76.0, 78.0, 80.0, 76.0, 74.0, 72.0, 74.0, 76.0, 78.0, 80.0, 82.0, 84.0, 86.0, 88.0, 86.0],
        weight_series=[71.0, 71.2, 71.5, 71.8, 72.0, 72.4, 72.8, 73.0, 73.5, 74.0, 74.2, 74.8, 75.2, 75.6, 76.0, 76.5, 77.2, 78.0, 79.1, 80.4],
        bmi_series=[26.8, 26.9, 27.0, 27.1, 27.2, 27.3, 27.5, 27.6, 27.8, 27.9, 28.0, 28.2, 28.4, 28.5, 28.7, 28.9, 29.1, 29.4, 29.9, 30.3],
        stopped_med_renewals=[
            ("866412", "24 HR metoprolol succinate 100 MG Extended Release Oral Tablet", "UW Medicine Heart Institute"),
            ("1656340", "sacubitril 49 MG / valsartan 51 MG Oral Tablet [Entresto]", "UW Medicine Heart Institute"),
            ("310798", "Hydrochlorothiazide 25 MG Oral Tablet", "Community Primary Care"),
            ("617311", "Lipitor 40 MG Oral Tablet [atorvastatin]", "Community Primary Care"),
        ],
        progress_note_topics=[
            "Cardiorenal clinic follow-up: compensated HFrEF (LVEF 35%), baseline CKD stage 2-3a, stable electrolytes on low-dose diuretic therapy.",
            "Nephrology longitudinal review: gradual eGFR decline from 58 to 48 mL/min/1.73m2; counselled to avoid OTC NSAIDs and monitor serum potassium.",
            "Device clinic interrogation: dual-chamber ICD functioning normally with 98% biventricular pacing and zero ventricular tachyarrhythmia episodes.",
            "Heart Failure clinic titration: transitioned antihypertensive regimen and reinforced 2-gram dietary sodium and fluid restriction.",
            "Nephrology & Cardiology co-management: eGFR 41 mL/min/1.73m2 (CKD stage 3B progression); added potassium-sparing mineralocorticoid antagonist with close lab checks.",
            "Pre-admission outpatient check: mild lower-extremity edema and 2 kg weight gain; potassium 4.9 mmol/L on spironolactone and oral potassium supplement.",
        ],
    )

    _append_longitudinal_clinical_history(
        arthur_bundle,
        prefix="arthur",
        patient_id="arthur-pat-2001",
        clinic_names=["Swedish Heart & Vascular Anticoagulation Clinic", "Eastside Primary Care", "Virginia Mason Gastroenterology"],
        visit_dates=common_visit_dates,
        egfr_series=[74.0, 73.5, 73.0, 72.5, 72.0, 71.5, 71.0, 70.5, 70.0, 69.5, 69.0, 68.5, 68.0, 67.5, 67.0, 66.5, 66.0, 65.5, 65.0, 64.5],
        creatinine_series=[0.98, 0.99, 1.00, 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.07, 1.08, 1.09, 1.10, 1.11, 1.12, 1.13, 1.14, 1.15, 1.16, 1.18],
        k_series=[4.2, 4.1, 4.3, 4.2, 4.1, 4.2, 4.3, 4.2, 4.1, 4.3, 4.2, 4.2, 4.1, 4.3, 4.2, 4.1, 4.2, 4.3, 4.2, 4.3],
        hgb_series=[14.2, 14.1, 14.0, 13.9, 13.9, 13.8, 13.8, 13.7, 13.7, 13.6, 13.6, 13.5, 13.5, 13.5, 13.4, 13.4, 13.3, 13.2, 13.1, 12.9],
        a1c_series=[5.6, 5.6, 5.7, 5.7, 5.6, 5.7, 5.7, 5.8, 5.7, 5.8, 5.8, 5.7, 5.8, 5.8, 5.9, 5.8, 5.9, 5.9, 5.8, 5.9],
        sbp_series=[132.0, 130.0, 128.0, 130.0, 126.0, 128.0, 130.0, 128.0, 126.0, 124.0, 128.0, 126.0, 124.0, 122.0, 124.0, 126.0, 122.0, 120.0, 118.0, 116.0],
        dbp_series=[78.0, 76.0, 76.0, 78.0, 74.0, 76.0, 76.0, 74.0, 74.0, 72.0, 74.0, 72.0, 72.0, 70.0, 72.0, 72.0, 70.0, 68.0, 68.0, 66.0],
        hr_series=[72.0, 70.0, 68.0, 72.0, 74.0, 70.0, 68.0, 72.0, 70.0, 74.0, 76.0, 72.0, 70.0, 68.0, 72.0, 74.0, 76.0, 78.0, 82.0, 88.0],
        weight_series=[84.0, 84.2, 84.0, 83.8, 83.5, 83.6, 83.4, 83.2, 83.0, 82.8, 82.9, 82.7, 82.5, 82.4, 82.2, 82.0, 81.8, 81.6, 81.5, 81.2],
        bmi_series=[27.1, 27.2, 27.1, 27.0, 26.9, 27.0, 26.9, 26.8, 26.8, 26.7, 26.7, 26.7, 26.6, 26.6, 26.5, 26.4, 26.4, 26.3, 26.3, 26.2],
        stopped_med_renewals=[
            ("855332", "Warfarin Sodium 5 MG Oral Tablet", "Swedish Heart & Vascular Anticoagulation Clinic"),
            ("866412", "24 HR metoprolol succinate 100 MG Extended Release Oral Tablet", "Swedish Heart & Vascular Anticoagulation Clinic"),
            ("197361", "Amlodipine 5 MG Oral Tablet [Norvasc]", "Eastside Primary Care"),
            ("312961", "Simvastatin 20 MG Oral Tablet", "Eastside Primary Care"),
        ],
        progress_note_topics=[
            "Anticoagulation clinic longitudinal check: St. Jude mechanical aortic valve in situ; INR therapeutic at 2.8 on Warfarin 5 MG daily.",
            "Cardiology valve & atrial fibrillation follow-up: rate-controlled atrial fibrillation; mechanical prosthetic valve gradients stable on transthoracic echo.",
            "Primary care annual wellness visit: hemoglobin 13.8 g/dL, renal function stable, counselled against NSAID use while on chronic Warfarin therapy.",
            "Anticoagulation clinic quarterly review: INR 2.6, zero bleeding or thromboembolic events; continued Warfarin Sodium 5 MG daily.",
            "Outpatient primary care visit for knee osteoarthritis: patient requested anti-inflammatory pain relief; reconciliation needed with cardiology anticoagulation plan.",
            "Pre-admission primary care visit: uncoordinated DOAC sample/prescription (Eliquis) and walk-in NSAID (Naproxen) started prior to acute GI bleed.",
        ],
    )

    _append_longitudinal_clinical_history(
        margaret_bundle,
        prefix="margaret",
        patient_id="smart-pat-2002",
        clinic_names=["Pacific Northwest Geriatrics", "Overlake Sleep & Behavioral Clinic", "Community Pharmacy Refill"],
        visit_dates=common_visit_dates,
        egfr_series=[68.0, 67.5, 67.0, 66.5, 66.0, 65.5, 65.0, 64.5, 64.0, 63.5, 63.0, 62.5, 62.0, 61.5, 61.0, 60.5, 60.0, 59.5, 59.0, 58.5],
        creatinine_series=[0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00, 1.01, 1.02, 1.03, 1.04, 1.05, 1.06, 1.08],
        k_series=[4.0, 4.1, 4.0, 4.2, 4.1, 4.0, 4.1, 4.2, 4.1, 4.0, 4.2, 4.1, 4.0, 4.1, 4.2, 4.1, 4.0, 4.1, 4.2, 4.1],
        hgb_series=[12.6, 12.5, 12.5, 12.4, 12.4, 12.3, 12.3, 12.2, 12.2, 12.1, 12.1, 12.0, 12.0, 11.9, 11.9, 11.8, 11.8, 11.8, 11.7, 11.7],
        a1c_series=[6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.8, 6.9, 7.1, 7.2, 7.3, 7.4, 7.3, 7.2, 7.1, 7.0, 6.9, 6.8, 6.8, 6.7],
        sbp_series=[134.0, 132.0, 130.0, 128.0, 126.0, 124.0, 122.0, 120.0, 118.0, 116.0, 114.0, 112.0, 110.0, 108.0, 106.0, 104.0, 102.0, 100.0, 98.0, 96.0],
        dbp_series=[76.0, 74.0, 74.0, 72.0, 72.0, 70.0, 70.0, 68.0, 68.0, 66.0, 66.0, 64.0, 64.0, 62.0, 62.0, 60.0, 60.0, 58.0, 58.0, 56.0],
        hr_series=[74.0, 72.0, 74.0, 72.0, 70.0, 72.0, 74.0, 72.0, 70.0, 72.0, 74.0, 76.0, 74.0, 76.0, 78.0, 76.0, 78.0, 80.0, 82.0, 84.0],
        weight_series=[62.0, 61.8, 61.5, 61.4, 61.2, 61.0, 60.8, 60.6, 60.5, 60.2, 60.0, 59.8, 59.5, 59.2, 59.0, 58.8, 58.5, 58.2, 58.0, 57.8],
        bmi_series=[24.2, 24.1, 24.0, 24.0, 23.9, 23.8, 23.7, 23.7, 23.6, 23.5, 23.4, 23.4, 23.2, 23.1, 23.0, 23.0, 22.9, 22.7, 22.7, 22.6],
        stopped_med_renewals=[
            ("860975", "Metformin hydrochloride 500 MG Oral Tablet [Glucophage]", "Pacific Northwest Geriatrics"),
            ("197361", "Amlodipine 5 MG Oral Tablet [Norvasc]", "Pacific Northwest Geriatrics"),
            ("313782", "Acetaminophen 325 MG Oral Tablet", "Pacific Northwest Geriatrics"),
            ("310798", "Hydrochlorothiazide 25 MG Oral Tablet", "Pacific Northwest Geriatrics"),
        ],
        progress_note_topics=[
            "Geriatric primary care baseline: prediabetes (HbA1c 6.2%), mild osteopenia, independent ambulation with cane.",
            "Metabolic progression note: HbA1c rose to 7.4%; initiated Metformin and updated problem list to Type 2 Diabetes Mellitus (prior Prediabetes entry remained active).",
            "Geriatric mobility & blood pressure review: patient noting morning lightheadedness upon standing; orthostatic vitals borderline.",
            "Sleep & behavioral clinic consult: insomnia and anxiety following social isolation; initiated nighttime sedative-hypnotic without cross-clinic reconciliation.",
            "Social work & confidential safety assessment: documented sensitive domestic safety and social isolation concerns requiring 42 CFR / WA MHMDA privacy protection.",
            "Pre-ED geriatric follow-up: HbA1c down to 6.7% on dual oral hypoglycemic therapy; increased daytime drowsiness and unsteady gait on rollator walker.",
        ],
    )

    return {
        "Elena_Rostova_Cardiorenal.json": (elena_bundle, elena_anomalies),
        "Arthur_Pendelton_Anticoag.json": (arthur_bundle, arthur_anomalies),
        "Margaret_Chen_Polypharmacy.json": (margaret_bundle, margaret_anomalies),
        "MIMIC_IV_Cardiorenal_Summary.json": (elena_bundle, elena_anomalies),
        "SMART_IPS_Transition_Summary.json": (margaret_bundle, margaret_anomalies),
    }


def build_extended_longitudinal_demo_archetypes() -> Dict[str, Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """Build two additional longitudinal multi-clinic patient archetypes (`W22.1`) to expand the interactive
    WAHS+ experimentation cohort across maternal-fetal/endocrine and thoracic-oncology/cardio-oncology care."""
    common_visit_dates = [
        "2021-03-10", "2021-06-14", "2021-09-16", "2021-12-15",
        "2022-03-16", "2022-06-15", "2022-09-21", "2022-12-14",
        "2023-03-15", "2023-06-14", "2023-09-20", "2023-12-13",
        "2024-03-13", "2024-06-12", "2024-09-18", "2024-12-11",
        "2025-03-12", "2025-06-18", "2025-09-17", "2025-11-19",
    ]

    # Persona 7: Sofia Alvarez — Postpartum Preeclampsia, Cardiometabolic & Renal Transition
    sofia_note = (
        "# Chief Complaint\nPostpartum severe hypertension, acute kidney injury & glycemic transition follow-up\n"
        "# Assessment and Plan\nHold Ibuprofen immediately due to postpartum preeclampsia blood pressure surge "
        "(166/104 mmHg) and acute kidney injury (eGFR 46.0 mL/min/1.73m2). Continue single-agent Amlodipine 5 MG daily "
        "and reconcile duplicate Norvasc 5 MG prescription from maternal-fetal clinic. Transition glycemic problem list "
        "from Prediabetes to established Type 2 Diabetes Mellitus (HbA1c 7.2%) on Metformin 500 MG BID."
    )
    sofia_note_b64 = base64.b64encode(sofia_note.encode("utf-8")).decode("ascii")
    sofia_bundle: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "id": "sofia-alvarez-maternal-endocrine",
        "entry": [
            {
                "fullUrl": "urn:uuid:sofia-pat-3001",
                "resource": {
                    "resourceType": "Patient",
                    "id": "sofia-pat-3001",
                    "name": [{"use": "official", "family": "Alvarez", "given": ["Sofia"]}],
                    "gender": "female",
                    "birthDate": "1991-06-14",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-enc-postpartum",
                "resource": {
                    "resourceType": "Encounter",
                    "id": "sofia-enc-postpartum",
                    "status": "finished",
                    "class": {"code": "IMP"},
                    "type": [{"text": "High-Risk Obstetrical & Maternal-Fetal Medicine Admission (Postpartum Preeclampsia & AKI)"}],
                    "period": {"start": "2026-01-22", "end": "2026-01-26"},
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-allergy-latex",
                "resource": {
                    "resourceType": "AllergyIntolerance",
                    "id": "sofia-allergy-latex",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Latex surgical gloves (contact urticaria & bronchospasm)"},
                    "criticality": "high",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-cond-htn",
                "resource": {
                    "resourceType": "Condition",
                    "id": "sofia-cond-htn",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "59621000", "display": "Essential hypertension (disorder)"}],
                        "text": "Essential hypertension (disorder)",
                    },
                    "onsetDateTime": "2024-11-10",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-cond-predm",
                "resource": {
                    "resourceType": "Condition",
                    "id": "sofia-cond-predm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Prediabetes (finding)"},
                    "onsetDateTime": "2022-05-14",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-cond-t2dm",
                "resource": {
                    "resourceType": "Condition",
                    "id": "sofia-cond-t2dm",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Diabetes mellitus type 2 (disorder)"}],
                        "text": "Diabetes mellitus type 2 (disorder)",
                    },
                    "onsetDateTime": "2025-10-18",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-cond-ckd2",
                "resource": {
                    "resourceType": "Condition",
                    "id": "sofia-cond-ckd2",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "431856006", "display": "Chronic kidney disease stage 2 (disorder)"}],
                        "text": "Chronic kidney disease stage 2 (disorder)",
                    },
                    "onsetDateTime": "2023-08-12",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-cond-ckd3a",
                "resource": {
                    "resourceType": "Condition",
                    "id": "sofia-cond-ckd3a",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "700378005", "display": "Chronic kidney disease stage 3A (disorder)"}],
                        "text": "Chronic kidney disease stage 3A (disorder)",
                    },
                    "onsetDateTime": "2026-01-23",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-cond-sdoh-stress",
                "resource": {
                    "resourceType": "Condition",
                    "id": "sofia-cond-sdoh-stress",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "160968000", "display": "Reports of violence in the environment (finding)"}],
                        "text": "Reports of violence in the environment (finding)",
                    },
                    "onsetDateTime": "2025-07-09",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-proc-echo",
                "resource": {
                    "resourceType": "Procedure",
                    "id": "sofia-proc-echo",
                    "status": "completed",
                    "code": {"text": "Postpartum maternal transthoracic echocardiography & renal Doppler ultrasound"},
                    "performedDateTime": "2026-01-23",
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-med-amlodipine",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "sofia-med-amlodipine",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "197361", "display": "Amlodipine 5 MG Oral Tablet"}],
                        "text": "Amlodipine 5 MG Oral Tablet",
                    },
                    "authoredOn": "2026-01-25",
                    "performer": [{"actor": {"display": "UW Maternal-Fetal Medicine Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-med-norvasc",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "sofia-med-norvasc",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {"text": "Norvasc 5 MG Oral Tablet"},
                    "authoredOn": "2025-12-18",
                    "performer": [{"actor": {"display": "Neighborhood OB/GYN Associates"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-med-ibuprofen",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "sofia-med-ibuprofen",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "310965", "display": "Ibuprofen 200 MG Oral Tablet"}],
                        "text": "Ibuprofen 200 MG Oral Tablet",
                    },
                    "authoredOn": "2026-01-19",
                    "performer": [{"actor": {"display": "Postpartum Ward Discharge Order"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-med-metformin",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "sofia-med-metformin",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "860975", "display": "Metformin hydrochloride 500 MG Oral Tablet [Glucophage]"}],
                        "text": "Metformin hydrochloride 500 MG Oral Tablet [Glucophage]",
                    },
                    "authoredOn": "2026-01-26",
                    "performer": [{"actor": {"display": "UW Endocrine & Metabolic Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-obs-egfr-acute",
                "resource": {
                    "resourceType": "Observation",
                    "id": "sofia-obs-egfr-acute",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate (eGFR)"}]},
                    "effectiveDateTime": "2026-01-23T06:00:00Z",
                    "valueQuantity": {"value": 46.0, "unit": "mL/min/1.73_m2"},
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-obs-sbp-acute",
                "resource": {
                    "resourceType": "Observation",
                    "id": "sofia-obs-sbp-acute",
                    "status": "final",
                    "category": [{"coding": [{"code": "vital-signs"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6", "display": "Systolic blood pressure"}]},
                    "effectiveDateTime": "2026-01-22T21:00:00Z",
                    "valueQuantity": {"value": 166.0, "unit": "mm[Hg]"},
                },
            },
            {
                "fullUrl": "urn:uuid:sofia-note-discharge",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "sofia-note-discharge",
                    "status": "current",
                    "type": {"text": "Maternal-Fetal Medicine Postpartum Discharge Summary"},
                    "date": "2026-01-26T15:00:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": sofia_note_b64}}],
                },
            },
        ],
    }
    _append_longitudinal_clinical_history(
        sofia_bundle,
        prefix="sofia",
        patient_id="sofia-pat-3001",
        clinic_names=["UW Maternal-Fetal Medicine Clinic", "UW Endocrine & Metabolic Clinic", "Neighborhood OB/GYN Associates"],
        visit_dates=common_visit_dates,
        egfr_series=[88.0, 86.0, 85.0, 84.0, 82.0, 80.0, 78.0, 76.0, 74.0, 72.0, 70.0, 68.0, 66.0, 64.0, 62.0, 60.0, 58.0, 56.0, 54.0, 52.0],
        creatinine_series=[0.74, 0.76, 0.78, 0.80, 0.82, 0.85, 0.88, 0.90, 0.94, 0.98, 1.02, 1.06, 1.10, 1.14, 1.18, 1.22, 1.26, 1.30, 1.35, 1.42],
        k_series=[4.0, 4.1, 4.0, 4.1, 4.2, 4.1, 4.0, 4.2, 4.1, 4.2, 4.3, 4.2, 4.3, 4.4, 4.3, 4.4, 4.5, 4.4, 4.5, 4.6],
        hgb_series=[12.9, 12.8, 12.6, 12.5, 12.4, 12.2, 12.0, 11.9, 11.8, 11.6, 11.5, 11.4, 11.3, 11.2, 11.1, 11.0, 10.9, 10.8, 10.7, 10.6],
        a1c_series=[5.7, 5.8, 5.9, 6.0, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 7.0, 7.1, 7.2, 7.3, 7.2, 7.2, 7.2],
        sbp_series=[118.0, 120.0, 122.0, 124.0, 126.0, 128.0, 130.0, 132.0, 134.0, 136.0, 138.0, 140.0, 142.0, 144.0, 146.0, 148.0, 150.0, 154.0, 158.0, 162.0],
        dbp_series=[74.0, 76.0, 76.0, 78.0, 78.0, 80.0, 82.0, 82.0, 84.0, 86.0, 86.0, 88.0, 88.0, 90.0, 92.0, 92.0, 94.0, 96.0, 98.0, 100.0],
        hr_series=[76.0, 78.0, 76.0, 80.0, 82.0, 80.0, 84.0, 82.0, 86.0, 84.0, 88.0, 86.0, 88.0, 90.0, 88.0, 92.0, 90.0, 94.0, 92.0, 96.0],
        weight_series=[66.0, 66.5, 67.0, 67.8, 68.5, 69.2, 70.0, 70.8, 71.5, 72.4, 73.2, 74.0, 75.0, 76.2, 77.5, 78.8, 80.0, 81.2, 79.0, 76.5],
        bmi_series=[25.1, 25.3, 25.5, 25.8, 26.1, 26.3, 26.6, 26.9, 27.2, 27.5, 27.8, 28.1, 28.5, 29.0, 29.5, 30.0, 30.4, 30.9, 30.1, 29.1],
        stopped_med_renewals=[
            ("313782", "Acetaminophen 325 MG Oral Tablet", "Neighborhood OB/GYN Associates"),
            ("106892", "insulin isophane, human 70 UNT/ML / insulin, regular, human 30 UNT/ML Injectable Suspension [Humulin]", "UW Endocrine & Metabolic Clinic"),
            ("314153", "Pantoprazole 40 MG Delayed Release Oral Tablet [Protonix]", "Neighborhood OB/GYN Associates"),
        ],
        progress_note_topics=[
            "Pre-conception metabolic & endocrine consultation: baseline Prediabetes (HbA1c 6.0%) and borderline blood pressure; counselled on home glucose monitoring.",
            "Prenatal maternal-fetal medicine visit: rising fasting and postprandial blood glucose with HbA1c 7.1%; transitioned to insulin and Metformin management.",
            "Second-trimester renal & hypertension check: mild proteinuria and eGFR 64 mL/min/1.73m2; home blood pressure cuff logs averaging 142/88 mmHg.",
            "Third-trimester high-risk OB surveillance: worsening hypertension (154/96 mmHg) and declining eGFR; initiated Amlodipine 5 MG daily.",
            "Postpartum readmission note: severe postpartum preeclampsia (BP 166/104 mmHg) exacerbated by outpatient Ibuprofen use and duplicate brand Norvasc + generic Amlodipine bottles.",
            "Post-discharge maternal-fetal & endocrine transition: hold all NSAIDs (Ibuprofen), consolidate to single Amlodipine 5 MG pill, and retire outdated Prediabetes problem entry.",
        ],
    )
    sofia_anomalies = [
        {
            "anomaly_id": "sofia-amlodipine-norvasc-dup",
            "category": "cross_source_brand_generic_medication_duplicate",
            "severity": "high",
            "description": "Concurrent active Amlodipine 5 MG and brand Norvasc 5 MG across Maternal-Fetal Medicine and OB/GYN clinics.",
            "expected_resource_uuids": ["sofia-med-amlodipine", "sofia-med-norvasc"],
        },
        {
            "anomaly_id": "sofia-predm-t2dm-conflict",
            "category": "mutually_exclusive_condition_stages",
            "severity": "high",
            "description": "Concurrent active Prediabetes and Type 2 Diabetes Mellitus alongside CKD Stage 2 vs Stage 3A.",
            "expected_resource_uuids": ["sofia-cond-predm", "sofia-cond-t2dm", "sofia-cond-ckd2", "sofia-cond-ckd3a"],
        },
        {
            "anomaly_id": "sofia-note-hold-ibuprofen",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": "Postpartum discharge summary instructs to hold Ibuprofen immediately due to preeclampsia & AKI while structured order remains active.",
            "expected_resource_uuids": ["sofia-med-ibuprofen", "sofia-note-discharge"],
        },
    ]

    # Persona 8: Marcus Vance — Cardio-Oncology, Checkpoint Inhibitor Myocarditis & Anticoagulation
    marcus_note = (
        "# Chief Complaint\nImmune checkpoint inhibitor cardiotoxicity, atrial fibrillation & renal-anticoagulation review\n"
        "# Assessment and Plan\nStop Naproxen and hold lisinopril immediately due to acute renal decline "
        "(eGFR 36.0 mL/min/1.73m2) and high bleeding risk on Eliquis 5 MG BID. Reconcile duplicate Lipitor 40 MG "
        "and Simvastatin 20 MG statin prescriptions across oncology and primary care. Continue Entresto 49/51 MG BID "
        "and Metoprolol Succinate 100 MG daily for myocarditis-related HFrEF recovery."
    )
    marcus_note_b64 = base64.b64encode(marcus_note.encode("utf-8")).decode("ascii")
    marcus_bundle: Dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "id": "marcus-vance-oncology-immunotherapy",
        "entry": [
            {
                "fullUrl": "urn:uuid:marcus-pat-4001",
                "resource": {
                    "resourceType": "Patient",
                    "id": "marcus-vance-4001",
                    "name": [{"use": "official", "family": "Vance", "given": ["Marcus"]}],
                    "gender": "male",
                    "birthDate": "1961-09-22",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-enc-cardiooncology",
                "resource": {
                    "resourceType": "Encounter",
                    "id": "marcus-enc-cardiooncology",
                    "status": "finished",
                    "class": {"code": "IMP"},
                    "type": [{"text": "Inpatient Cardio-Oncology & Thoracic Oncology Consult Admission"}],
                    "period": {"start": "2026-02-02", "end": "2026-02-06"},
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-allergy-plat",
                "resource": {
                    "resourceType": "AllergyIntolerance",
                    "id": "marcus-allergy-plat",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "Carboplatin / platinum chemotherapy agents (severe hypersensitivity infusion reaction)"},
                    "criticality": "high",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-cond-hfref",
                "resource": {
                    "resourceType": "Condition",
                    "id": "marcus-cond-hfref",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "441481004", "display": "Chronic systolic heart failure (disorder)"}],
                        "text": "Chronic systolic heart failure (disorder)",
                    },
                    "onsetDateTime": "2025-11-18",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-cond-afib",
                "resource": {
                    "resourceType": "Condition",
                    "id": "marcus-cond-afib",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "49436004", "display": "Atrial fibrillation (disorder)"}],
                        "text": "Atrial fibrillation (disorder)",
                    },
                    "onsetDateTime": "2025-12-04",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-cond-ckd2",
                "resource": {
                    "resourceType": "Condition",
                    "id": "marcus-cond-ckd2",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "431856006", "display": "Chronic kidney disease stage 2 (disorder)"}],
                        "text": "Chronic kidney disease stage 2 (disorder)",
                    },
                    "onsetDateTime": "2022-04-10",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-cond-ckd3b",
                "resource": {
                    "resourceType": "Condition",
                    "id": "marcus-cond-ckd3b",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "700379002", "display": "Chronic kidney disease stage 3B (disorder)"}],
                        "text": "Chronic kidney disease stage 3B (disorder)",
                    },
                    "onsetDateTime": "2026-02-02",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-cond-sdoh-alc",
                "resource": {
                    "resourceType": "Condition",
                    "id": "marcus-cond-sdoh-alc",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "228366006", "display": "Unhealthy alcohol drinking behavior (finding)"}],
                        "text": "Unhealthy alcohol drinking behavior (finding)",
                    },
                    "onsetDateTime": "2024-08-15",
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-dev-port",
                "resource": {
                    "resourceType": "Device",
                    "id": "marcus-dev-port",
                    "status": "active",
                    "type": {"text": "Subcutaneous chest wall植入式 venous access port (PowerPort) & implantable cardiac loop recorder"},
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-med-eliquis",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "marcus-med-eliquis",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "1364430", "display": "apixaban 5 MG Oral Tablet [Eliquis]"}],
                        "text": "apixaban 5 MG Oral Tablet [Eliquis]",
                    },
                    "authoredOn": "2025-12-05",
                    "performer": [{"actor": {"display": "Fred Hutch Cardio-Oncology Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-med-naproxen",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "marcus-med-naproxen",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "849574", "display": "Naproxen sodium 220 MG Oral Tablet"}],
                        "text": "Naproxen sodium 220 MG Oral Tablet",
                    },
                    "authoredOn": "2026-01-14",
                    "performer": [{"actor": {"display": "Community Urgent Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-med-lisinopril",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "marcus-med-lisinopril",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {"text": "lisinopril 10 MG Oral Tablet"},
                    "authoredOn": "2025-07-19",
                    "performer": [{"actor": {"display": "Puget Sound Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-med-lipitor",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "marcus-med-lipitor",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "617311", "display": "Lipitor 40 MG Oral Tablet [atorvastatin]"}],
                        "text": "Lipitor 40 MG Oral Tablet [atorvastatin]",
                    },
                    "authoredOn": "2025-11-20",
                    "performer": [{"actor": {"display": "Fred Hutch Cardio-Oncology Clinic"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-med-simvastatin",
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "marcus-med-simvastatin",
                    "status": "active",
                    "intent": "order",
                    "medicationCodeableConcept": {
                        "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "312961", "display": "Simvastatin 20 MG Oral Tablet"}],
                        "text": "Simvastatin 20 MG Oral Tablet",
                    },
                    "authoredOn": "2025-03-10",
                    "performer": [{"actor": {"display": "Puget Sound Primary Care"}}],
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-obs-egfr-acute",
                "resource": {
                    "resourceType": "Observation",
                    "id": "marcus-obs-egfr-acute",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33914-3", "display": "Glomerular filtration rate (eGFR)"}]},
                    "effectiveDateTime": "2026-02-02T07:00:00Z",
                    "valueQuantity": {"value": 36.0, "unit": "mL/min/1.73_m2"},
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-obs-bnp-acute",
                "resource": {
                    "resourceType": "Observation",
                    "id": "marcus-obs-bnp-acute",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"system": "http://loinc.org", "code": "33762-6", "display": "Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma"}]},
                    "effectiveDateTime": "2026-02-02T07:00:00Z",
                    "valueQuantity": {"value": 3420.0, "unit": "pg/mL"},
                },
            },
            {
                "fullUrl": "urn:uuid:marcus-note-discharge",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "marcus-note-discharge",
                    "status": "current",
                    "type": {"text": "Cardio-Oncology Inpatient Consult & Discharge Summary"},
                    "date": "2026-02-06T16:00:00+00:00",
                    "content": [{"attachment": {"contentType": "text/plain; charset=utf-8", "data": marcus_note_b64}}],
                },
            },
        ],
    }
    _append_longitudinal_clinical_history(
        marcus_bundle,
        prefix="marcus",
        patient_id="marcus-vance-4001",
        clinic_names=["Fred Hutch Cardio-Oncology Clinic", "UW Thoracic Oncology Center", "Puget Sound Primary Care"],
        visit_dates=common_visit_dates,
        egfr_series=[72.0, 70.0, 68.0, 66.0, 64.0, 62.0, 60.0, 58.0, 56.0, 54.0, 52.0, 50.0, 48.0, 46.0, 45.0, 44.0, 42.0, 40.0, 39.0, 38.0],
        creatinine_series=[1.02, 1.05, 1.08, 1.12, 1.15, 1.18, 1.22, 1.26, 1.30, 1.35, 1.40, 1.44, 1.48, 1.54, 1.58, 1.62, 1.68, 1.74, 1.80, 1.86],
        k_series=[4.2, 4.3, 4.2, 4.4, 4.3, 4.4, 4.5, 4.4, 4.5, 4.6, 4.5, 4.6, 4.7, 4.6, 4.7, 4.8, 4.7, 4.8, 4.9, 4.8],
        hgb_series=[13.8, 13.6, 13.4, 13.1, 12.8, 12.5, 12.2, 12.0, 11.8, 11.5, 11.3, 11.1, 10.9, 10.8, 10.6, 10.5, 10.4, 10.3, 10.2, 10.1],
        a1c_series=[5.8, 5.8, 5.9, 5.9, 6.0, 6.0, 6.1, 6.1, 6.2, 6.2, 6.3, 6.3, 6.2, 6.2, 6.1, 6.1, 6.2, 6.2, 6.1, 6.1],
        sbp_series=[136.0, 134.0, 132.0, 130.0, 128.0, 126.0, 124.0, 122.0, 120.0, 118.0, 116.0, 114.0, 112.0, 110.0, 108.0, 106.0, 104.0, 102.0, 100.0, 98.0],
        dbp_series=[82.0, 80.0, 78.0, 78.0, 76.0, 76.0, 74.0, 74.0, 72.0, 72.0, 70.0, 70.0, 68.0, 68.0, 66.0, 66.0, 64.0, 64.0, 62.0, 60.0],
        hr_series=[74.0, 76.0, 78.0, 80.0, 82.0, 84.0, 86.0, 88.0, 90.0, 92.0, 94.0, 96.0, 98.0, 102.0, 106.0, 110.0, 104.0, 96.0, 90.0, 84.0],
        weight_series=[88.0, 87.5, 87.0, 86.2, 85.5, 84.8, 84.0, 83.2, 82.5, 81.8, 81.0, 80.4, 79.8, 79.2, 78.6, 78.0, 77.5, 77.0, 76.8, 76.5],
        bmi_series=[27.8, 27.6, 27.5, 27.2, 27.0, 26.8, 26.5, 26.3, 26.1, 25.8, 25.6, 25.4, 25.2, 25.0, 24.8, 24.6, 24.5, 24.3, 24.3, 24.2],
        stopped_med_renewals=[
            ("1740467", "2 ML Ondansetron 2 MG/ML Injection", "UW Thoracic Oncology Center"),
            ("866412", "24 HR metoprolol succinate 100 MG Extended Release Oral Tablet", "Fred Hutch Cardio-Oncology Clinic"),
            ("310429", "Furosemide 40 MG Oral Tablet [Lasix]", "Fred Hutch Cardio-Oncology Clinic"),
        ],
        progress_note_topics=[
            "Thoracic oncology baseline: Stage IIIA non-small cell lung cancer; subcutaneous venous access port placed; baseline renal function CKD Stage 2.",
            "Oncology cycle 4 chemo-immunotherapy review: mild treatment-related anemia (Hemoglobin 11.5 g/dL) and cisplatin-associated eGFR decline to 52 mL/min/1.73m2.",
            "Cardio-oncology surveillance: new-onset paroxysmal atrial fibrillation and rising NT-proBNP after checkpoint inhibitor maintenance; initiated Eliquis 5 MG BID.",
            "Thyroid & metabolic immunotherapy check: TSH monitoring and lipid management; Lipitor 40 MG started at cardio-oncology while primary care Simvastatin 20 MG remained active.",
            "Urgent care musculoskeletal visit: prescribed OTC-strength Naproxen 220 MG for chest-wall arthralgia despite active Eliquis anticoagulation and CKD Stage 3B.",
            "Cardio-oncology inpatient stabilization: immune-mediated myocarditis / HFrEF flare with eGFR 36 mL/min; instructed to stop Naproxen, hold Lisinopril, and stop duplicate Simvastatin.",
        ],
    )
    _append_marcus_high_volume_cardiooncology_cycles(marcus_bundle)
    marcus_anomalies = [
        {
            "anomaly_id": "marcus-statin-dup",
            "category": "cross_source_therapeutic_class_duplicate",
            "severity": "high",
            "description": "Concurrent active Lipitor 40 MG [atorvastatin] and Simvastatin 20 MG across cardio-oncology and primary care.",
            "expected_resource_uuids": ["marcus-med-lipitor", "marcus-med-simvastatin"],
        },
        {
            "anomaly_id": "marcus-anticoag-nsaid-hazard",
            "category": "high_risk_drug_bleeding_interaction",
            "severity": "high",
            "description": "Concurrent Eliquis 5 MG [apixaban] and Naproxen 220 MG with CKD Stage 3B progression.",
            "expected_resource_uuids": ["marcus-med-eliquis", "marcus-med-naproxen"],
        },
        {
            "anomaly_id": "marcus-note-stop-naproxen-lisinopril",
            "category": "note_vs_structured_medication_contradiction",
            "severity": "high",
            "description": "Cardio-oncology discharge note instructs to stop Naproxen and hold Lisinopril while outpatient orders remain active.",
            "expected_resource_uuids": ["marcus-med-naproxen", "marcus-med-lisinopril", "marcus-note-discharge"],
        },
    ]

    return {
        "Sofia_Alvarez_Maternal_Endocrine.json": (sofia_bundle, sofia_anomalies),
        "Marcus_Vance_Oncology_Immunotherapy.json": (marcus_bundle, marcus_anomalies),
    }


__all__ = [
    "build_deidentified_real_clinical_summaries",
    "build_extended_longitudinal_demo_archetypes",
]
