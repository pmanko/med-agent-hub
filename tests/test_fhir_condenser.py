"""Unit tests for the FHIR R4 5-Tier Progressive Chart Condenser and ContextSource."""

from __future__ import annotations

import asyncio
import base64
from typing import Any, Dict, List

from server.context_sources import (
    ContextBudget,
    ContextRequest,
    SourceRegistry,
    select_context,
)
from server.fhir import (
    FhirBundleContextSource,
    ResilientTokenCounter,
    build_tier_items,
    condense_bundle_to_budget,
    estimate_tokens_local,
    extract_bundle,
)
from server.temporal import _date_output_failures, build_temporal_facts


def _make_synthetic_wahs_bundle() -> Dict[str, Any]:
    """Build a representative multi-source US Core / IPS FHIR R4 Bundle with:
    - Patient demographics
    - AllergyIntolerance (Penicillin)
    - Mutually exclusive active Conditions (CKD stages 2 & 3, Prediabetes + Diabetic nephropathy)
    - Sensitive SDOH Condition (Has a criminal record) + routine SDOH (Full-time employment)
    - 15 repeated resolved Conditions (Medication review due)
    - Active Medications + inpatient MedicationAdministration referencing a Medication resource
    - 20 repeated historical MedicationRequest renewals (Simvastatin)
    - Duplicate/anomalous Immunizations (2 live zoster doses, 3 HPV doses started at age 11)
    - 10 routine annual Influenza Immunizations
    - Duplicate Base64 clinical note across DocumentReference and DiagnosticReport.presentedForm
    - Multi-analyte DiagnosticReport lab panels across multiple years (eGFR, Creatinine, HbA1c)
    - Multi-component Blood Pressure Observations
    - Clinic room hardware Device (Dental x-ray system) vs patient-carried Device (CPAP appliance)
    - Non-clinical Claim / ExplanationOfBenefit / Provenance resources
    """
    note_md = (
        "# Chief Complaint\nShortness of breath and leg swelling.\n\n"
        "# History of Present Illness\nMaurice is a 53 year-old male with long boilerplate history.\n\n"
        "# Allergies\nPenicillin.\n\n"
        "# Assessment and Plan\nAcute decompensated heart failure and CKD stage 3; adjust diuretics.\n"
    )
    note_b64 = base64.b64encode(note_md.encode("utf-8")).decode("ascii")

    entries: List[Dict[str, Any]] = [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "pat-1",
                "name": [{"use": "official", "given": ["Alex"], "family": "Rivera"}],
                "gender": "male",
                "birthDate": "1985-04-10",
            }
        },
        {
            "resource": {
                "resourceType": "Organization",
                "id": "org-uw",
                "name": "UW Medical Center",
            }
        },
        {
            "resource": {
                "resourceType": "Medication",
                "id": "med-cefazolin",
                "code": {
                    "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": "309090", "display": "cefazolin 2000 MG Injection"}]
                },
            }
        },
        {
            "resource": {
                "resourceType": "AllergyIntolerance",
                "id": "allergy-pen",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "verificationStatus": {"coding": [{"code": "confirmed"}]},
                "criticality": "high",
                "category": ["medication"],
                "code": {"coding": [{"code": "7980", "display": "Penicillin"}]},
                "recordedDate": "2010-05-01T10:00:00Z",
                "reaction": [{"severity": "severe", "manifestation": [{"coding": [{"display": "Anaphylaxis"}]}]}],
            }
        },
        # Conflicting active CKD stages + Prediabetes alongside Diabetic kidney disease + Sensitive SDOH
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-ckd2",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {"coding": [{"code": "431856006", "display": "Chronic kidney disease stage 2 (disorder)"}]},
                "onsetDateTime": "2020-01-15T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-ckd3",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {"coding": [{"code": "433144002", "display": "Chronic kidney disease stage 3 (disorder)"}]},
                "onsetDateTime": "2023-06-20T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-predm",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {"coding": [{"code": "714628002", "display": "Prediabetes (finding)"}]},
                "onsetDateTime": "2015-03-10T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-dmkidney",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {"coding": [{"code": "127013003", "display": "Disorder of kidney due to diabetes mellitus (disorder)"}]},
                "onsetDateTime": "2020-01-15T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-sens-crim",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {"coding": [{"code": "266948004", "display": "Has a criminal record (finding)"}]},
                "onsetDateTime": "2018-02-01T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-emp",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {"coding": [{"code": "160903007", "display": "Full-time employment (finding)"}]},
                "onsetDateTime": "2025-01-10T00:00:00Z",
            }
        },
        # Active MedicationRequest + Inpatient MedicationAdministration
        {
            "resource": {
                "resourceType": "MedicationRequest",
                "id": "medreq-lisinopril",
                "status": "active",
                "authoredOn": "2025-11-01T09:00:00Z",
                "medicationCodeableConcept": {"coding": [{"code": "314076", "display": "lisinopril 10 MG Oral Tablet"}]},
            }
        },
        {
            "resource": {
                "resourceType": "MedicationAdministration",
                "id": "medadm-cefazolin",
                "status": "completed",
                "effectiveDateTime": "2025-10-15T14:00:00Z",
                "medicationReference": {"reference": "urn:uuid:med-cefazolin"},
            }
        },
        # HPV 3 doses started at age 11 (born 1985-04-10 -> 1996 is age 11)
        {
            "resource": {
                "resourceType": "Immunization",
                "id": "imm-hpv-1",
                "status": "completed",
                "vaccineCode": {"coding": [{"code": "165", "display": "HPV, 9-valent"}]},
                "occurrenceDateTime": "1996-06-01T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Immunization",
                "id": "imm-hpv-2",
                "status": "completed",
                "vaccineCode": {"coding": [{"code": "165", "display": "HPV, 9-valent"}]},
                "occurrenceDateTime": "1996-12-01T00:00:00Z",
            }
        },
        {
            "resource": {
                "resourceType": "Immunization",
                "id": "imm-hpv-3",
                "status": "completed",
                "vaccineCode": {"coding": [{"code": "165", "display": "HPV, 9-valent"}]},
                "occurrenceDateTime": "1997-06-01T00:00:00Z",
            }
        },
        # Duplicate COVID-19 doses 5 days apart across two sources
        {
            "resource": {
                "resourceType": "Immunization",
                "id": "imm-covid-a",
                "status": "completed",
                "vaccineCode": {"coding": [{"code": "208", "display": "COVID-19, mRNA, LNP-S, PF"}]},
                "occurrenceDateTime": "2024-10-01T00:00:00Z",
                "performer": [{"actor": {"display": "Walgreens Pharmacy"}}],
            }
        },
        {
            "resource": {
                "resourceType": "Immunization",
                "id": "imm-covid-b",
                "status": "completed",
                "vaccineCode": {"coding": [{"code": "208", "display": "COVID-19, mRNA, LNP-S, PF"}]},
                "occurrenceDateTime": "2024-10-05T00:00:00Z",
                "performer": [{"actor": {"display": "UW Medical Center"}}],
            }
        },
        # Duplicate Base64 note in DocumentReference AND DiagnosticReport.presentedForm
        {
            "resource": {
                "resourceType": "DocumentReference",
                "id": "docref-1",
                "date": "2025-11-15T12:00:00Z",
                "type": {"coding": [{"display": "History and physical note"}]},
                "content": [{"attachment": {"contentType": "text/plain", "data": note_b64}}],
            }
        },
        {
            "resource": {
                "resourceType": "DiagnosticReport",
                "id": "diagrep-note-dup",
                "effectiveDateTime": "2025-11-15T12:00:00Z",
                "code": {"coding": [{"display": "History and physical note"}]},
                "presentedForm": [{"contentType": "text/plain", "data": note_b64}],
            }
        },
        # Clinic room equipment Device (should be excluded) vs Patient CPAP Device (should be kept)
        {
            "resource": {
                "resourceType": "Device",
                "id": "dev-xray-room",
                "status": "active",
                "type": {"coding": [{"display": "Dental x-ray system (physical object)"}]},
            }
        },
        {
            "resource": {
                "resourceType": "Device",
                "id": "dev-cpap-1",
                "status": "active",
                "manufactureDate": "2024-02-01T00:00:00Z",
                "type": {"coding": [{"code": "706180005", "display": "Appliance for sleep apnea (physical object)"}]},
            }
        },
        # Non-clinical Claim & Provenance
        {
            "resource": {
                "resourceType": "Claim",
                "id": "claim-1",
                "status": "active",
            }
        },
        {
            "resource": {
                "resourceType": "Provenance",
                "id": "prov-1",
                "target": [{"reference": "urn:uuid:cond-ckd3"}, {"reference": "urn:uuid:medreq-lisinopril"}],
                "agent": [{"who": {"reference": "urn:uuid:org-uw", "display": "UW Medical Center"}}],
            }
        },
    ]

    # Add 15 repeated resolved Conditions ("Medication review due")
    for i in range(15):
        year = 2010 + i
        entries.append(
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": f"cond-medrev-{i}",
                    "clinicalStatus": {"coding": [{"code": "resolved"}]},
                    "code": {"coding": [{"code": "314529007", "display": "Medication review due (situation)"}]},
                    "onsetDateTime": f"{year}-03-01T00:00:00Z",
                    "abatementDateTime": f"{year}-03-01T01:00:00Z",
                }
            }
        )

    # Add 20 repeated stopped Simvastatin renewals
    for i in range(20):
        year = 2005 + i
        entries.append(
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": f"med-simva-{i}",
                    "status": "stopped",
                    "authoredOn": f"{year}-06-15T00:00:00Z",
                    "medicationCodeableConcept": {
                        "coding": [{"code": "312961", "display": "Simvastatin 20 MG Oral Tablet"}]
                    },
                }
            }
        )

    # Add 6 years of BMP DiagnosticReport lab panels (eGFR declining from 88 to 46, Creatinine rising from 1.0 to 1.8)
    for i in range(6):
        year = 2020 + i
        egfr_val = 88.0 - i * 8.4
        creat_val = round(1.0 + i * 0.16, 2)
        egfr_id = f"obs-egfr-{i}"
        creat_id = f"obs-creat-{i}"
        bp_id = f"obs-bp-{i}"
        entries.extend(
            [
                {
                    "resource": {
                        "resourceType": "Observation",
                        "id": egfr_id,
                        "status": "final",
                        "category": [{"coding": [{"code": "laboratory"}]}],
                        "code": {"coding": [{"code": "33914-3", "display": "Glomerular filtration rate"}]},
                        "effectiveDateTime": f"{year}-08-10T00:00:00Z",
                        "valueQuantity": {"value": egfr_val, "unit": "mL/min"},
                    }
                },
                {
                    "resource": {
                        "resourceType": "Observation",
                        "id": creat_id,
                        "status": "final",
                        "category": [{"coding": [{"code": "laboratory"}]}],
                        "code": {"coding": [{"code": "2160-0", "display": "Creatinine [Mass/volume] in Blood"}]},
                        "effectiveDateTime": f"{year}-08-10T00:00:00Z",
                        "valueQuantity": {"value": creat_val, "unit": "mg/dL"},
                    }
                },
                {
                    "resource": {
                        "resourceType": "DiagnosticReport",
                        "id": f"panel-bmp-{i}",
                        "status": "final",
                        "code": {"coding": [{"code": "51990-0", "display": "Basic metabolic panel - Blood"}]},
                        "effectiveDateTime": f"{year}-08-10T00:00:00Z",
                        "result": [
                            {"reference": f"urn:uuid:{egfr_id}"},
                            {"reference": f"urn:uuid:{creat_id}"},
                        ],
                    }
                },
                {
                    "resource": {
                        "resourceType": "Observation",
                        "id": bp_id,
                        "status": "final",
                        "category": [{"coding": [{"code": "vital-signs"}]}],
                        "code": {"coding": [{"code": "85354-9", "display": "Blood pressure panel"}]},
                        "effectiveDateTime": f"{year}-08-10T00:00:00Z",
                        "component": [
                            {
                                "code": {"coding": [{"code": "8480-6", "display": "Systolic Blood Pressure"}]},
                                "valueQuantity": {"value": 122 + i * 3, "unit": "mm[Hg]"},
                            },
                            {
                                "code": {"coding": [{"code": "8462-4", "display": "Diastolic Blood Pressure"}]},
                                "valueQuantity": {"value": 78 + i * 2, "unit": "mm[Hg]"},
                            },
                        ],
                    }
                },
            ]
        )

    return {"resourceType": "Bundle", "type": "collection", "entry": entries}


def test_extract_bundle_normalizes_notes_panels_components_and_conflicts():
    bundle = _make_synthetic_wahs_bundle()
    extracted = extract_bundle(bundle)

    # 1. Base64 note duplicated in DocumentReference and DiagnosticReport is merged into 1 note
    assert extracted.deduplicated_note_count == 1
    note_items = [x for x in extracted.items if x.section == "clinical_note"]
    assert len(note_items) == 1
    assert set(note_items[0].member_uuids) == {"docref-1", "diagrep-note-dup"}
    assert "Shortness of breath" in note_items[0].text
    assert "Acute decompensated heart failure" in note_items[0].text
    # Repeated boilerplate section ("long boilerplate history") is stripped
    assert "long boilerplate history" not in note_items[0].text

    # 2. Clinic room equipment (Dental x-ray system) is excluded while patient CPAP is retained
    assert extracted.excluded_resource_counts.get("Device(clinic_equipment)") == 1
    assert extracted.excluded_resource_counts.get("Claim") == 1
    assert extracted.excluded_resource_counts.get("Provenance") == 1
    devices = [x for x in extracted.items if x.section == "device"]
    assert [d.resource_uuid for d in devices] == ["dev-cpap-1"]

    # 3. MedicationAdministration resolves Medication reference (cefazolin 2000 MG Injection)
    meds = {x.resource_uuid: x for x in extracted.items if x.section in {"med_active", "med_historical"}}
    assert "cefazolin 2000 MG Injection" in meds["medadm-cefazolin"].text
    assert "inpatient/administered" in meds["medadm-cefazolin"].text

    # 4. Stage conflicts and ACIP immunization anomalies are pre-annotated
    conds = {x.resource_uuid: x for x in extracted.items if x.section == "condition_active"}
    assert "stage_conflict: CKD stage 2, stage 3 simultaneously marked ACTIVE" in (
        conds["cond-ckd2"].conflict_note or ""
    )
    assert "stage_conflict: Prediabetes marked ACTIVE alongside established diabetes" in (
        conds["cond-predm"].conflict_note or ""
    )
    assert conds["cond-sens-crim"].sensitivity == "SDOH/PRIVACY-SENSITIVE"

    imms = {x.resource_uuid: x for x in extracted.items if x.section == "immunization"}
    assert "acip_schedule_note: 3rd HPV dose" in (imms["imm-hpv-3"].conflict_note or "")
    assert "duplicate_candidate: COVID-19 recorded 4d apart" in (imms["imm-covid-a"].conflict_note or "")
    assert "duplicate_candidate: COVID-19 recorded 4d apart" in (imms["imm-covid-b"].conflict_note or "")


def test_sharing_postures_keep_or_redact_privacy_sensitive_sdoh():
    bundle = _make_synthetic_wahs_bundle()

    self_res = condense_bundle_to_budget(bundle, target_tokens=8192, sharing_posture="patient_self")
    assert "Has a criminal record" in self_res.chart_text
    assert "[sensitivity: SDOH/PRIVACY-SENSITIVE]" in self_res.chart_text
    assert self_res.redacted_for_privacy == []

    ext_res = condense_bundle_to_budget(bundle, target_tokens=8192, sharing_posture="external_share")
    assert "Has a criminal record" not in ext_res.chart_text
    assert len(ext_res.redacted_for_privacy) == 1
    assert ext_res.redacted_for_privacy[0]["resourceUuid"] == "cond-sens-crim"
    assert "privacy_redacted=1" in ext_res.chart_text


def test_progressive_tiers_fit_any_token_budget_and_preserve_safety_core_uuids():
    bundle = _make_synthetic_wahs_bundle()
    required_safety_uuids = {
        "pat-1",
        "allergy-pen",
        "cond-ckd2",
        "cond-ckd3",
        "cond-predm",
        "cond-dmkidney",
        "cond-sens-crim",
        "cond-emp",
        "medreq-lisinopril",
        "imm-hpv-3",
        "imm-covid-a",
        "imm-covid-b",
        "dev-cpap-1",
    }

    for budget in (16384, 8192, 4096, 2048, 1024, 512):
        res = condense_bundle_to_budget(bundle, target_tokens=budget)
        assert res.actual_tokens <= budget, f"Budget {budget} overflowed with {res.actual_tokens}"
        assert "Completeness Manifest:" in res.chart_text

        # Collect all primary + memberUuids across mappings
        present_uuids = set()
        for m in res.mappings:
            if m.get("resourceUuid"):
                present_uuids.add(m["resourceUuid"])
            for member in m.get("memberUuids") or []:
                present_uuids.add(member)

        missing = required_safety_uuids - present_uuids
        assert not missing, f"Budget {budget} lost safety-critical UUIDs: {missing}"

        # Verify temporal.py compatibility: all ISO dates in chart_text are in date_ledger
        facts = build_temporal_facts(res.chart_text, None)
        assert facts["reference_date"] == "2025-11-15" or facts["reference_date"] == "2025-11-01"
        # Every date mentioned in any line of chart_text must be allowed by date_ledger
        failures = _date_output_failures(res.chart_text, facts)
        assert failures == [], f"Budget {budget} produced temporal date_ledger failures: {failures}"


def test_tier_3_and_tier_4_trajectories_support_temporal_trend_verification():
    bundle = _make_synthetic_wahs_bundle()
    extracted = extract_bundle(bundle)
    items, _, _ = build_tier_items(extracted, 3)
    res = condense_bundle_to_budget(extracted, target_tokens=2048)

    facts = build_temporal_facts(res.chart_text, None)
    series_by_concept = {s["concept"]: s for s in facts["numeric_series"]}
    assert "Glomerular filtration rate" in series_by_concept
    egfr = series_by_concept["Glomerular filtration rate"]
    assert egfr["trend_supported"] is True
    assert egfr["direction"] == "down"
    assert egfr["window"]["start"] == "2020-08-10"
    assert egfr["window"]["end"] == "2025-08-10"


def test_fhir_bundle_context_source_and_resilient_token_counter():
    bundle = _make_synthetic_wahs_bundle()
    source = FhirBundleContextSource(
        bundles_by_patient={"alex-rivera": bundle},
        target_tokens=4096,
    )
    registry = SourceRegistry([source])
    counter = ResilientTokenCounter(router_counter=None)

    async def _run():
        req = ContextRequest(
            messages=[{"role": "user", "content": "What are the patient's kidney function trends and active medications?"}],
            patient="alex-rivera",
            source="fhir",
            question="What are the patient's kidney function trends and active medications?",
        )
        ledger = await registry.build_ledger(req)
        assert "fhir" in ledger.source_names
        assert ledger.source_metadata["fhir"]["completeness_manifest"]["total_bundle_entries"] == len(bundle["entry"])

        view = await select_context(
            ledger,
            question=req.question,
            model="gemma-4-31b",
            budget=ContextBudget(context_window=4096, reserved_output_tokens=1024),
            counter=counter,
        )
        assert view.input_tokens <= 3072
        rendered = view.render()
        assert "Penicillin" in rendered
        assert "lisinopril 10 MG Oral Tablet" in rendered
        mappings = view.mappings()
        assert any(m.get("memberUuids") for m in mappings)

    asyncio.run(_run())
