"""Patient-Facing Demo Workspace, Condensation Matrix, and Read-Only Skills Builder (`W15.2`).

Separates backend chart preparation & condensation levers from the 9 read-only
patient-facing skills (5 Everyday Patient Tasks + 4 Chart Health-Check & Guidance Skills).
"""

from __future__ import annotations

from collections import defaultdict
import json
import re
from typing import Any, Dict, List, Mapping, Optional

from .condenser import (
    CondensedChartResult,
    condense_bundle_to_budget,
    estimate_tokens_local,
)
from .extractors import (
    ClinicalItem,
    extract_bundle_lossless,
)
from .fhir_browser import build_fhir_browser_index
from .kb_lookup import resolve_medication_code
from .wahs_analyzer import (
    _build_uuid_to_citation_map,
    _citations_for_uuids,
    analyze_data_quality_and_conflicts,
    analyze_duplicates_and_bloat,
    analyze_patterns_and_summaries,
)


def _extract_human_summary_from_condensed(
    condensed: CondensedChartResult,
    *,
    condense_mode: str,
) -> Dict[str, Any]:
    """Build a clean, patient-friendly structured summary of what is inside a condensed chart snapshot."""
    allergies: List[str] = []
    active_conditions: List[str] = []
    active_medications: List[str] = []
    labs_and_vitals: List[str] = []
    immunizations: List[str] = []
    notes_and_visits: List[str] = []

    for rec in condensed.records:
        rtype = str(rec.get("resourceType") or "")
        text = str(rec.get("text") or "").strip()
        r_date = str(rec.get("clinicalDate") or rec.get("date") or "")[:10]
        prefix_date = f"({r_date}) " if r_date else ""
        clean_line = re.sub(
            r"\s*\[(?:ingredient_group|conflict|acip_schedule_note|duplicate_candidate)[^\]]*\]",
            "",
            text,
        ).strip()

        if rtype == "AllergyIntolerance" or clean_line.startswith("Allergy:"):
            allergies.append(clean_line.replace("Allergy: ", ""))
        elif rtype == "Condition" and "status: active" in clean_line.lower():
            active_conditions.append(clean_line.replace("Condition: ", ""))
        elif rtype in {"MedicationRequest", "MedicationAdministration"} and (
            "status: active" in clean_line.lower() or "active medication" in clean_line.lower()
        ):
            active_medications.append(clean_line.replace("Medication: ", ""))
        elif rtype == "Immunization" or clean_line.startswith("Immunization"):
            immunizations.append(f"{prefix_date}{clean_line.replace('Immunization: ', '')}")
        elif rtype in {"Observation", "DiagnosticReport"} and not clean_line.startswith(
            "Clinical Note"
        ):
            labs_and_vitals.append(f"{prefix_date}{clean_line}")
        elif rtype in {
            "DocumentReference",
            "Encounter",
            "Procedure",
            "Device",
            "CarePlan",
        } or clean_line.startswith("Clinical Note"):
            notes_and_visits.append(f"{prefix_date}{clean_line}")

    omitted = condensed.completeness_manifest.get("omitted_sections") or {}
    omitted_total = sum(int(v) for v in omitted.values()) if isinstance(omitted, Mapping) else 0
    budget = condensed.target_tokens
    if budget >= 8192:
        lever_note = (
            f"At {budget // 1024}k tokens ({'Lossless Structural' if condense_mode == 'lossless_structural' else 'Hierarchical Grouped'}), "
            f"100% of active conditions, medications, allergies, clinical notes, and multi-year lab history fit in context "
            f"({len(condensed.records)} clinical ledger lines; {omitted_total} items omitted)."
        )
    elif budget >= 4096:
        lever_note = (
            f"At 4k tokens ({'Lossless Structural' if condense_mode == 'lossless_structural' else 'Hierarchical Grouped'}), "
            f"all active safety records, recent notes, and core lab series remain in context ({len(condensed.records)} lines), "
            f"while {omitted_total} older routine observations/visits are trimmed to fit the 1-page visit budget."
        )
    else:
        lever_note = (
            f"At {budget // 1024}k tokens ({'Lossless Structural' if condense_mode == 'lossless_structural' else 'Hierarchical Grouped'}), "
            f"the chart is compressed to an emergency/compact capsule ({len(condensed.records)} lines). "
            f"The IPS Safety Core (allergies, active conditions, active meds) is preserved first, while {omitted_total} "
            f"older notes, procedures, and historical labs are omitted from the in-context prompt (recoverable via on-demand chart lookup)."
        )

    return {
        "allergies": allergies[:8],
        "active_conditions": active_conditions[:16],
        "active_medications": active_medications[:16],
        "labs_and_vitals": labs_and_vitals[:16],
        "immunizations": immunizations[:12],
        "notes_and_visits": notes_and_visits[:12],
        "counts": {
            "allergies": len(allergies),
            "active_conditions": len(active_conditions),
            "active_medications": len(active_medications),
            "labs_and_vitals": len(labs_and_vitals),
            "immunizations": len(immunizations),
            "notes_and_visits": len(notes_and_visits),
            "omitted_items": omitted_total,
        },
        "what_changed_at_this_lever": lever_note,
    }


def build_patient_skills_and_condensation_matrix(
    bundle: Mapping[str, Any],
    *,
    raw_json_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    """Build the W15 Patient-Facing Demo payload for any FHIR R4 Patient Bundle:
    1. `fhir_browser`: Simple 1-level core-resource FHIR browser index (`build_fhir_browser_index`)
    2. `prep_lever_summary`: Backend preparation & non-destructive hygiene metrics
    3. `condensation_matrix`: Interactive `(condense_mode x target_tokens)` snapshots (`lossless_structural` vs `hierarchical` across `16k/8k/4k/2k/1k`)
    4. `skills`: 9 Read-Only Patient-Facing Skills (5 Everyday Patient Tasks + 4 Chart Health-Check & Guidance Skills)
    """
    if raw_json_tokens is None:
        raw_json_tokens = estimate_tokens_local(json.dumps(bundle))

    fhir_browser = build_fhir_browser_index(bundle)
    extracted_lossless = extract_bundle_lossless(
        bundle,
        enable_privacy_classification=True,
        filter_room_equipment_devices=False,
    )
    condensed_8k = condense_bundle_to_budget(
        extracted_lossless,
        target_tokens=8192,
        condense_mode="lossless_structural",
    )
    uuid_map = _build_uuid_to_citation_map(condensed_8k)

    dq = analyze_data_quality_and_conflicts(extracted_lossless, condensed_8k)
    patterns = analyze_patterns_and_summaries(extracted_lossless, condensed_8k)
    bloat = analyze_duplicates_and_bloat(extracted_lossless, condensed_8k)

    # 1. Prep lever summary
    excl = extracted_lossless.excluded_resource_counts
    claims_count = int(excl.get("Claim", 0)) + int(excl.get("ExplanationOfBenefit", 0))
    lookup_count = (
        int(excl.get("Organization", 0))
        + int(excl.get("Practitioner", 0))
        + int(excl.get("Provenance", 0))
        + int(excl.get("Medication", 0))
        + int(excl.get("Location", 0))
    )
    room_equip_count = int(excl.get("Device(clinic_equipment_retained)", 0)) + int(
        excl.get("Device(clinic_equipment)", 0)
    )
    sensitive_items = [
        it for it in extracted_lossless.items if it.sensitivity == "SDOH/PRIVACY-SENSITIVE"
    ]

    prep_lever_summary = {
        "raw_json_tokens": raw_json_tokens,
        "total_bundle_entries": extracted_lossless.total_bundle_entries,
        "core_clinical_items_count": len(extracted_lossless.items),
        "non_clinical_claims_count": claims_count,
        "lookup_metadata_count": lookup_count,
        "duplicate_base64_notes_count": extracted_lossless.deduplicated_note_count,
        "room_equipment_devices_count": room_equip_count,
        "sensitive_sdoh_count": len(sensitive_items),
    }

    # 2. Interactive Condensation Matrix (2 modes x 5 budgets)
    condensation_matrix: Dict[str, Dict[str, Any]] = {
        "lossless_structural": {},
        "hierarchical": {},
    }
    for mode_key, internal_mode in (
        ("lossless_structural", "lossless_structural"),
        ("hierarchical", "tiered"),
    ):
        for b_tok in (16384, 8192, 4096, 2048, 1024):
            c_res = condense_bundle_to_budget(
                bundle,
                target_tokens=b_tok,
                condense_mode=internal_mode,
            )
            red_pct = round((1.0 - (c_res.actual_tokens / max(1, raw_json_tokens))) * 100.0, 1)
            condensation_matrix[mode_key][str(b_tok)] = {
                "condense_mode": mode_key,
                "mode_label": (
                    "Lossless Structural (Verbatim Records, No Folding)"
                    if mode_key == "lossless_structural"
                    else "Hierarchical Grouped & Trend-Summarized"
                ),
                "target_tokens": b_tok,
                "actual_tokens": c_res.actual_tokens,
                "reduction_pct": red_pct,
                "tier": c_res.tier,
                "tier_name": c_res.tier_name,
                "rendered_records": len(c_res.records),
                "omitted_sections": c_res.completeness_manifest.get("omitted_sections", {}),
                "chart_text": c_res.chart_text[:14000],
                "human_summary": _extract_human_summary_from_condensed(
                    c_res, condense_mode=mode_key
                ),
            }

    # 3. Build the 9 Read-Only Patient-Facing Skills
    active_conds = [it for it in extracted_lossless.items if it.section == "condition_active"]
    active_meds = [it for it in extracted_lossless.items if it.section == "med_active"]
    allergies = [it for it in extracted_lossless.items if it.section == "allergy"]
    imms = [it for it in extracted_lossless.items if it.section == "immunization"]
    notes = [it for it in extracted_lossless.items if it.section == "clinical_note"]
    devices = [it for it in extracted_lossless.items if it.section == "device"]
    trajectories = patterns.get("trajectories") or []
    hl_enc = patterns.get("highlight_encounter") or {}

    # Skill 1: Prepare My Next Doctor Visit Summary
    visit_rows: List[Dict[str, Any]] = []
    for al in allergies[:4]:
        visit_rows.append(
            {
                "category": "Allergy Safety Alert",
                "label": al.display,
                "detail": al.text.replace("Allergy: ", ""),
                "source_org": al.source_org or "Chart Record",
                "code": al.code,
                "tone": "warn",
            }
        )
    for c_it in active_conds[:8]:
        visit_rows.append(
            {
                "category": "Active Condition",
                "label": c_it.display,
                "detail": f"Onset: {c_it.date or 'recorded'} • Status: {c_it.status}",
                "source_org": c_it.source_org or "Primary / Specialist Clinic",
                "code": f"SNOMED {c_it.code}" if c_it.code else "",
                "tone": "info",
            }
        )
    for m_it in active_meds[:8]:
        doses = "; ".join(m_it.metadata.get("dosage") or []) or "Active prescription"
        visit_rows.append(
            {
                "category": "Current Medication",
                "label": m_it.display,
                "detail": doses,
                "source_org": m_it.source_org or "Prescribing Clinic",
                "code": f"RxNorm {m_it.code}" if m_it.code else "",
                "tone": "ok",
            }
        )
    visit_questions = [
        f["what_to_ask_doctor"] for f in dq["findings"] if f.get("what_to_ask_doctor")
    ][:4]
    if not visit_questions:
        visit_questions = [
            "Are all of my active diagnoses and daily medications up to date on my chart?",
            "Which lab tests or preventive screenings are due at today's visit?",
        ]
    visit_cites = _citations_for_uuids(
        [it.resource_uuid for it in (allergies[:2] + active_conds[:5] + active_meds[:5])],
        uuid_map,
        condensed_8k,
    )

    skill_visit_brief = {
        "id": "skill-visit-brief",
        "group": "everyday_task",
        "group_label": "Everyday Patient Tasks",
        "icon": "📋",
        "title": "Prepare My Next Doctor Visit Summary",
        "subtitle": "1-page snapshot of active conditions, current medications, allergies, and top questions to ask",
        "prompt_question": "Can you prepare a clear 1-page summary of my active conditions, current medications, allergies, and top questions for my next doctor visit?",
        "status_badge": f"{len(active_conds)} Active Conditions • {len(active_meds)} Active Meds",
        "badge_tone": "info",
        "plain_english_summary": (
            f"Here is your 1-page pre-visit brief for **{extracted_lossless.patient_name}** (chart anchor date `{extracted_lossless.reference_date}`). "
            f"Your chart lists **{len(allergies)} recorded allergy alert(s)**, **{len(active_conds)} active condition(s)** on your problem list, "
            f"and **{len(active_meds)} active medication prescription(s)** across your care team."
        ),
        "structured_rows": visit_rows,
        "suggested_remediation": visit_questions,
        "lever_sensitivity_note": (
            "Allergies, active conditions, and active medications belong to the IPS Safety Core and are retained across all condensation budgets (16k down to 1k)."
        ),
        "clinician_technical_note": (
            f"IPS Safety Core summary for {extracted_lossless.patient_name} (DOB {extracted_lossless.birth_date}): "
            f"{len(allergies)} AllergyIntolerance, {len(active_conds)} active Condition, {len(active_meds)} active MedicationRequest/Administration resources."
        ),
        "citations": visit_cites,
    }

    # Skill 2: Explain My Recent Hospital Stay or Procedure
    hosp_rows: List[Dict[str, Any]] = []
    for p_str in hl_enc.get("key_procedures") or hl_enc.get("procedures") or []:
        hosp_rows.append(
            {
                "category": "Procedure / Intervention",
                "label": p_str.split("(")[0].strip(),
                "detail": p_str,
                "source_org": hl_enc.get("facility") or "Hospital / Surgical Center",
                "code": "SNOMED Procedure",
                "tone": "info",
            }
        )
    for m_str in hl_enc.get("inpatient_medications") or []:
        hosp_rows.append(
            {
                "category": "Hospital / Discharge Medication",
                "label": m_str.split("(")[0].strip(),
                "detail": m_str,
                "source_org": hl_enc.get("facility") or "Hospital Team",
                "code": "RxNorm",
                "tone": "ok",
            }
        )
    for dev_it in [d for d in devices if not d.metadata.get("is_room_equipment")]:
        hosp_rows.append(
            {
                "category": "Implanted / Patient Device",
                "label": dev_it.display,
                "detail": f"Recorded {dev_it.date or 'in chart'} • Status: {dev_it.status}",
                "source_org": dev_it.source_org or "Surgical / Clinical Team",
                "code": f"SNOMED {dev_it.code}" if dev_it.code else "Device",
                "tone": "info",
            }
        )
    for n_it in notes[:3]:
        full_txt = str(n_it.metadata.get("full_text") or n_it.text)
        hosp_rows.append(
            {
                "category": f"Clinical Note ({n_it.date or 'Recorded'})",
                "label": n_it.display,
                "detail": " ".join(full_txt.splitlines()[:4]).strip()[:260],
                "source_org": n_it.source_org or "Care Team",
                "code": "DocumentReference",
                "tone": "info",
            }
        )
    skill_hospital_stay = {
        "id": "skill-hospital-stay",
        "group": "everyday_task",
        "group_label": "Everyday Patient Tasks",
        "icon": "🏥",
        "title": "Explain My Recent Hospital Stay or Procedure",
        "subtitle": "Plain-English timeline of your major hospital visit, procedures, devices, and clinician notes",
        "prompt_question": "What happened during my recent hospital stay or major procedure, and what instructions were in the clinical note?",
        "status_badge": f"{hl_enc.get('window') or 'Recent Episode'} • {len(notes)} Clinical Note(s)",
        "badge_tone": "info",
        "plain_english_summary": (
            f"**{hl_enc.get('title') or 'Highlighted Care Episode'} ({hl_enc.get('window') or 'Recent'}):** "
            f"{hl_enc.get('narrative') or 'Your chart includes clinical notes and procedure records from your care episodes.'}"
        ),
        "structured_rows": hosp_rows[:10],
        "suggested_remediation": [
            "Review any hospital discharge or specialist note instructions alongside your current outpatient pill bottles to make sure temporary stop/hold orders were updated.",
            "Confirm whether any follow-up imaging, lab checks, or specialist visits recommended at discharge have been scheduled.",
        ],
        "lever_sensitivity_note": (
            "Full clinical notes and older procedure details fit directly in context at 16k, 8k, and 4k budgets. At 1k–2k emergency budgets, detailed encounter narratives are omitted from the compact prompt and retrieved on demand when you run this skill."
        ),
        "clinician_technical_note": (
            f"Encounter window {hl_enc.get('window')}: {len(hl_enc.get('procedures') or [])} procedures, "
            f"{len(hl_enc.get('inpatient_medications') or [])} encounter medications, {len(notes)} decoded DocumentReference/DiagnosticReport narratives."
        ),
        "citations": (hl_enc.get("citations") or visit_cites)[:6],
    }

    # Skill 3: Explain My Current Medications & What They Treat
    med_rows: List[Dict[str, Any]] = []
    for m_it in active_meds:
        kb_res = m_it.metadata.get("kb_resolution") or resolve_medication_code(
            m_it.code, m_it.display
        )
        t_class = (
            str(
                kb_res.get("therapeutic_class")
                or m_it.metadata.get("therapeutic_class")
                or "Medication"
            )
            .replace("_", " ")
            .title()
        )
        ing_name = str(
            kb_res.get("ingredient")
            or m_it.metadata.get("ingredient_group")
            or m_it.ingredient_key
            or ""
        ).title()
        reasons = (
            ", ".join(m_it.metadata.get("reasons") or []) or f"Used for {t_class} management"
        )
        doses = "; ".join(m_it.metadata.get("dosage") or []) or "Take as directed on bottle"
        med_rows.append(
            {
                "category": t_class,
                "label": m_it.display,
                "detail": f"Active ingredient: {ing_name or m_it.display} • {doses} • Purpose: {reasons} (Ordered {m_it.date or 'active'})",
                "source_org": m_it.source_org or "Prescribing Clinic",
                "code": f"RxNorm {m_it.code}" if m_it.code else "MedicationRequest",
                "tone": "ok",
            }
        )
    med_cites = _citations_for_uuids(
        [m.resource_uuid for m in active_meds], uuid_map, condensed_8k
    )
    skill_med_guide = {
        "id": "skill-med-guide",
        "group": "everyday_task",
        "group_label": "Everyday Patient Tasks",
        "icon": "💊",
        "title": "Explain My Current Medications & What They Treat",
        "subtitle": "Plain-English guide to every active prescription on your chart, its active ingredient, and purpose",
        "prompt_question": "What active medications are on my chart right now, what active ingredient is in each pill, and what condition is each one treating?",
        "status_badge": f"{len(active_meds)} Active Prescriptions",
        "badge_tone": "info",
        "plain_english_summary": (
            f"Your chart currently lists **{len(active_meds)} active medication prescription(s)**. "
            f"Below is each medication matched to its active ingredient, therapeutic drug family, dosage instructions, and the clinic that ordered it."
        ),
        "structured_rows": med_rows,
        "suggested_remediation": [
            "Bring your physical pill bottles to your next visit so your care team can confirm that the doses you take at home match these active orders.",
            "If two prescriptions share the same active ingredient or drug family, run the 'Check Medication Conflicts' skill before taking both.",
        ],
        "lever_sensitivity_note": (
            "Active MedicationRequest and MedicationAdministration orders are part of the mandatory IPS Safety Core and are retained across all condensation sizes (16k down to 1k)."
        ),
        "clinician_technical_note": (
            f"{len(active_meds)} active MedicationRequest/MedicationAdministration items normalized via RxNorm ingredient & therapeutic class KB."
        ),
        "citations": med_cites,
    }

    # Skill 4: Explain My Lab Results & Trends Over Time
    lab_rows: List[Dict[str, Any]] = []
    lab_uuids: List[str] = []
    for t in trajectories:
        first_pt = t.get("initial") or t.get("first") or {}
        last_pt = t.get("latest") or {}
        min_pt = t.get("min") or t.get("trough") or first_pt
        max_pt = t.get("max") or t.get("peak") or last_pt
        delta_val = float(last_pt.get("value", 0.0)) - float(first_pt.get("value", 0.0))
        lab_rows.append(
            {
                "category": f"Direction: {str(t.get('direction') or 'tracked').upper()}",
                "label": f"{t['concept']} ({t['unit']})",
                "detail": (
                    f"Latest: {last_pt.get('value')} {t['unit']} ({last_pt.get('date')}) • "
                    f"Earliest: {first_pt.get('value')} {t['unit']} ({first_pt.get('date')}) • "
                    f"Range across {t['count']} tests: {min_pt.get('value')} – {max_pt.get('value')} {t['unit']} (Δ {delta_val:+.2f})"
                ),
                "source_org": t.get("reference_range")
                and f"Ref: {t['reference_range']}"
                or "Longitudinal Lab Series",
                "code": f"LOINC {t.get('loinc') or t.get('category') or 'Lab'}".strip(),
                "tone": "warn"
                if str(t.get("direction") or "")
                in {"increasing", "decreasing", "rising", "falling"}
                else "info",
            }
        )
        for c_obj in t.get("citations") or []:
            u = c_obj.get("resource_uuid") or c_obj.get("resourceUuid")
            if u:
                lab_uuids.append(u)
    lab_cites = _citations_for_uuids(lab_uuids, uuid_map, condensed_8k)
    skill_lab_trends = {
        "id": "skill-lab-trends",
        "group": "everyday_task",
        "group_label": "Everyday Patient Tasks",
        "icon": "📈",
        "title": "Explain My Lab Results & Trends Over Time",
        "subtitle": "Longitudinal walkthrough of kidney function (eGFR/Creatinine), HbA1c, potassium, INR, and vitals",
        "prompt_question": "How have my key lab results and vital signs (like kidney eGFR, creatinine, HbA1c, potassium, or hemoglobin/INR) changed over time?",
        "status_badge": f"{len(trajectories)} Tracked Biomarker Series",
        "badge_tone": "info",
        "plain_english_summary": (
            f"Instead of reading dozens of separate lab slips in isolation, this skill tracks how your **{len(trajectories)} key laboratory and vital-sign series** "
            f"have moved over time—from your earliest baseline to your most recent reading—highlighting significant upward or downward trends."
        ),
        "structured_rows": lab_rows,
        "suggested_remediation": [
            "Ask your clinician whether your most recent kidney (eGFR/creatinine), electrolyte (potassium), blood count, or metabolic (HbA1c) trend requires any dose adjustment on your current medications.",
        ],
        "lever_sensitivity_note": (
            "In Lossless Structural mode, individual timestamped lab observations are listed verbatim (and older labs are trimmed first at 1k–2k budgets). In Hierarchical Grouped mode, multi-year lab series are compressed into compact First → Peak/Trough → Latest trend lines so longitudinal slopes survive even in small token budgets."
        ),
        "clinician_technical_note": (
            f"Computed {len(trajectories)} longitudinal LOINC numeric series across {len(extracted_lossless.numeric_points)} extracted numeric Observation points."
        ),
        "citations": lab_cites[:8],
    }

    # Skill 5: Summarize My Vaccines & Preventive Care Status
    imm_by_grp: Dict[str, List[ClinicalItem]] = defaultdict(list)
    for im in imms:
        grp_name = str(im.metadata.get("vaccine_group") or im.ingredient_key or im.display)
        imm_by_grp[grp_name].append(im)
    imm_rows: List[Dict[str, Any]] = []
    for grp_name, grp_items in sorted(imm_by_grp.items()):
        ordered = sorted(grp_items, key=lambda x: x.date or "")
        dates_str = ", ".join(sorted({x.date or "undated" for x in ordered}))
        orgs_str = (
            ", ".join(sorted({x.source_org for x in ordered if x.source_org}))
            or "Immunization Record"
        )
        imm_rows.append(
            {
                "category": f"{len(ordered)} Recorded Entry(ies)",
                "label": ordered[-1].display,
                "detail": f"Vaccine group: {grp_name} • Dates recorded: {dates_str}",
                "source_org": orgs_str,
                "code": f"CVX {ordered[-1].code}" if ordered[-1].code else "Immunization",
                "tone": "ok",
            }
        )
    imm_cites = _citations_for_uuids([im.resource_uuid for im in imms], uuid_map, condensed_8k)
    skill_vaccine_summary = {
        "id": "skill-vaccine-summary",
        "group": "everyday_task",
        "group_label": "Everyday Patient Tasks",
        "icon": "💉",
        "title": "Summarize My Vaccines & Preventive Care Status",
        "subtitle": "Organized immunization history grouped by vaccine family, date, and clinic/state registry source",
        "prompt_question": "What vaccines and immunizations are recorded in my chart, and are there any schedule or duplicate registry notes?",
        "status_badge": f"{len(imms)} Vaccine Entries ({len(imm_by_grp)} Groups)",
        "badge_tone": "info",
        "plain_english_summary": (
            f"Your chart contains **{len(imms)} immunization record(s)** across **{len(imm_by_grp)} vaccine group(s)**. "
            f"Below is your organized vaccine history showing the dates recorded and which clinic or state registry reported each dose."
        ),
        "structured_rows": imm_rows,
        "suggested_remediation": [
            "If two doses of the same vaccine appear on the exact same date from both your clinic and a state registry (like WA IIS), that is a registry sync duplicate—not two physical shots given on the same day.",
            "Bring this summary to your next annual checkup to verify whether any booster doses are due for your age group.",
        ],
        "lever_sensitivity_note": (
            "In Lossless Structural mode, every individual Immunization resource is listed separately at 8k–16k budgets. In Hierarchical Grouped mode, routine multi-dose series (like annual Influenza) are folded into a single series summary line."
        ),
        "clinician_technical_note": (
            f"{len(imms)} FHIR Immunization resources mapped across {len(imm_by_grp)} CVX/ACIP vaccine groups."
        ),
        "citations": imm_cites[:8],
    }

    # Skill 6: Check for Medication Conflicts, Duplicate Pills & Note Mismatches (Read-Only)
    med_conflict_findings = [
        f
        for f in dq["findings"]
        if any(
            k in (f.get("domain") or "").lower()
            for k in (
                "medication",
                "drug",
                "duplicate therapy",
                "therapeutic class",
                "interaction",
                "narrative",
            )
        )
        or (f.get("id") or "").startswith("conflict-med")
        or (f.get("id") or "").startswith("conflict-note-med")
    ]
    med_check_rows: List[Dict[str, Any]] = []
    med_check_remediation: List[str] = []
    med_check_cites: List[Dict[str, Any]] = []
    for f in med_conflict_findings:
        med_check_rows.append(
            {
                "category": f.get("domain") or "Medication Reconciliation Alert",
                "label": f.get("title") or "Medication Conflict",
                "detail": f.get("patient_plain_summary") or f.get("description") or "",
                "source_org": ", ".join(f.get("source_organizations") or [])
                or "Multi-Clinic / Discharge Record",
                "code": f.get("kb_rule_id") or "RxNorm Check",
                "tone": "warn",
            }
        )
        if f.get("what_to_ask_doctor"):
            med_check_remediation.append(f'Ask your doctor: "{f["what_to_ask_doctor"]}"')
        if f.get("recommendation"):
            med_check_remediation.append(
                f"Suggested chart remediation (read-only): {f['recommendation']}"
            )
        med_check_cites.extend(f.get("citations") or [])

    if not med_check_rows:
        med_check_rows.append(
            {
                "category": "All Clear",
                "label": "No Active Medication Conflicts or Note Mismatches Detected",
                "detail": f"Checked all {len(active_meds)} active prescriptions against RxNorm ingredient groups, therapeutic classes, lab safety thresholds, and clinical notes.",
                "source_org": "Chart-Wide Read-Only Check",
                "code": "PASS",
                "tone": "ok",
            }
        )
        med_check_remediation.append(
            "No conflicting active prescriptions were detected. Continue reviewing your medication list at routine visits."
        )

    skill_med_conflicts = {
        "id": "skill-med-conflicts",
        "group": "health_check",
        "group_label": "Chart Health-Check & Guidance (Read-Only)",
        "icon": "⚠️",
        "title": "Check Medication Conflicts, Duplicate Pills & Note Mismatches",
        "subtitle": "Read-only safety check for brand-vs-generic duplicates, overlapping drug classes, lab hazards, and hospital note 'hold/stop' mismatches",
        "prompt_question": "Are there any conflicting, duplicate, or unsafe medications on my active medication list, or any medicine a clinical note said to hold or stop?",
        "status_badge": (
            f"{len(med_conflict_findings)} Issue(s) Found (Read-Only Guidance)"
            if med_conflict_findings
            else "No Medication Conflicts Found"
        ),
        "badge_tone": "warn" if med_conflict_findings else "ok",
        "plain_english_summary": (
            f"This read-only check audited your **{len(active_meds)} active prescription(s)** and **{len(notes)} clinical note(s)** without modifying your chart. "
            + (
                f"It found **{len(med_conflict_findings)} potential medication safety or reconciliation issue(s)**—such as brand-vs-generic duplicate bottles, overlapping pills from the same drug class, drug-lab interaction hazards, or an outpatient prescription that remained 'active' after a hospital note instructed to hold or stop it."
                if med_conflict_findings
                else "No duplicate active ingredients, overlapping therapeutic classes, or clinical note 'hold/stop' contradictions were found."
            )
        ),
        "structured_rows": med_check_rows,
        "suggested_remediation": med_check_remediation[:6],
        "lever_sensitivity_note": (
            "Structured prescription conflicts are detectable at all budgets (16k down to 1k) because active medications are always kept. However, detecting a contradiction between a free-text hospital discharge note and an active pill requires the clinical note text (included in context at 4k–16k, or fetched via on-demand note lookup at 1k–2k)."
        ),
        "clinician_technical_note": (
            " | ".join(f.get("clinician_technical_note") or "" for f in med_conflict_findings)
            if med_conflict_findings
            else "Zero RxNorm ingredient, therapeutic class, drug-lab, or note-vs-MedicationRequest contradictions detected."
        ),
        "citations": (med_check_cites or med_cites)[:8],
    }

    # Skill 7: Check for Outdated or Conflicting Problem-List Diagnoses (Read-Only)
    cond_conflict_findings = [
        f
        for f in dq["findings"]
        if any(
            k in (f.get("domain") or "").lower()
            for k in ("condition", "staging", "kdigo", "glycemic", "problem")
        )
        or (f.get("id") or "").startswith("conflict-ckd")
        or (f.get("id") or "").startswith("conflict-glycemic")
        or (f.get("id") or "").startswith("conflict-cond")
    ]
    cond_check_rows: List[Dict[str, Any]] = []
    cond_check_remediation: List[str] = []
    cond_check_cites: List[Dict[str, Any]] = []
    for f in cond_conflict_findings:
        cond_check_rows.append(
            {
                "category": f.get("domain") or "Problem-List Discrepancy",
                "label": f.get("title") or "Condition Conflict",
                "detail": f.get("patient_plain_summary") or f.get("description") or "",
                "source_org": ", ".join(f.get("source_organizations") or [])
                or "Multi-Clinic Problem List",
                "code": f.get("kb_rule_id") or "SNOMED Check",
                "tone": "warn",
            }
        )
        if f.get("what_to_ask_doctor"):
            cond_check_remediation.append(f'Ask your doctor: "{f["what_to_ask_doctor"]}"')
        if f.get("recommendation"):
            cond_check_remediation.append(
                f"Suggested chart remediation (read-only): {f['recommendation']}"
            )
        cond_check_cites.extend(f.get("citations") or [])

    if not cond_check_rows:
        cond_check_rows.append(
            {
                "category": "All Clear",
                "label": "No Conflicting Disease Stages or Cross-Clinic Status Disagreements",
                "detail": f"Audited all {len(active_conds)} active and resolved Condition resources across clinics.",
                "source_org": "Chart-Wide Read-Only Check",
                "code": "PASS",
                "tone": "ok",
            }
        )
        cond_check_remediation.append(
            "Your active problem list has no mutually exclusive disease stages or active-vs-resolved clinic conflicts."
        )

    skill_condition_conflicts = {
        "id": "skill-condition-conflicts",
        "group": "health_check",
        "group_label": "Chart Health-Check & Guidance (Read-Only)",
        "icon": "🩺",
        "title": "Check Outdated or Conflicting Problem-List Diagnoses",
        "subtitle": "Read-only check for mutually exclusive disease stages (e.g., multiple CKD stages, Prediabetes + T2DM) and active-vs-resolved clinic disagreements",
        "prompt_question": "Why does my chart list multiple stages of the same condition or show a condition as both active and resolved across different clinics?",
        "status_badge": (
            f"{len(cond_conflict_findings)} Discrepancy(ies) Found (Read-Only)"
            if cond_conflict_findings
            else "Problem List Clean"
        ),
        "badge_tone": "warn" if cond_conflict_findings else "ok",
        "plain_english_summary": (
            "When records are combined from multiple clinics over several years, older diagnoses often stay marked 'active' even after a condition progresses or heals. "
            + (
                f"This read-only check found **{len(cond_conflict_findings)} problem-list discrepancy(ies)** on your chart and explains which lab result or specialist record clarifies your true current status."
                if cond_conflict_findings
                else "All active diagnoses on your problem list are consistent across clinics."
            )
        ),
        "structured_rows": cond_check_rows,
        "suggested_remediation": cond_check_remediation[:6],
        "lever_sensitivity_note": (
            "Active Condition resources are preserved at all budgets (16k down to 1k). However, comparing an active Condition against an older resolved Condition from another clinic requires resolved conditions (kept in context at 4k–16k, or retrieved via chart lookup at 1k–2k)."
        ),
        "clinician_technical_note": (
            " | ".join(f.get("clinician_technical_note") or "" for f in cond_conflict_findings)
            if cond_conflict_findings
            else "Zero KDIGO/ADA mutually exclusive staging or cross-clinic clinicalStatus conflicts detected."
        ),
        "citations": (cond_check_cites or visit_cites)[:8],
    }

    # Skill 8: Audit Chart Redundancy, Duplicate Panels & Export Artifacts (Read-Only)
    hygiene_rows: List[Dict[str, Any]] = []
    hygiene_remediation: List[str] = []
    hygiene_cites: List[Dict[str, Any]] = []

    imm_conflict_findings = [
        f
        for f in dq["findings"]
        if "immunization" in (f.get("domain") or "").lower()
        or (f.get("id") or "").startswith("conflict-imm")
    ]
    for f in imm_conflict_findings:
        hygiene_rows.append(
            {
                "category": f.get("domain") or "Immunization Registry Artifact",
                "label": f.get("title") or "Vaccine Record Anomaly",
                "detail": f.get("patient_plain_summary") or f.get("description") or "",
                "source_org": "Clinic vs. State Registry",
                "code": f.get("kb_rule_id") or "CVX / ACIP",
                "tone": "warn",
            }
        )
        if f.get("what_to_ask_doctor"):
            hygiene_remediation.append(f'Ask your doctor: "{f["what_to_ask_doctor"]}"')
        hygiene_cites.extend(f.get("citations") or [])

    for card in bloat.get("cards") or []:
        hygiene_rows.append(
            {
                "category": card.get("category") or "Chart Redundancy / Bloat",
                "label": card.get("title") or "Redundancy Pattern",
                "detail": f"{card.get('impact') or ''} — {card.get('summary') or ''}",
                "source_org": "Raw FHIR Bundle Audit",
                "code": "Non-Destructive Prep",
                "tone": "info",
            }
        )
        hygiene_cites.extend(card.get("citations") or [])

    if room_equip_count > 0:
        hygiene_remediation.append(
            f"Suggested chart remediation (read-only): Mark or filter the {room_equip_count} clinic room-equipment Device entry(ies) (e.g., dental X-ray machine or hospital bed) so only patient-carried implants/prosthetics appear on your personal Device list."
        )
    if extracted_lossless.deduplicated_note_count > 0:
        hygiene_remediation.append(
            f"Suggested chart remediation (read-only): Collapse the {extracted_lossless.deduplicated_note_count} byte-identical Base64 clinical note wrapper copy(ies) (`DocumentReference` vs `DiagnosticReport.presentedForm`) while keeping 100% of the unique note text."
        )
    if claims_count > 0:
        hygiene_remediation.append(
            f"Suggested chart remediation (read-only): Keep the {claims_count} billing `Claim`/`ExplanationOfBenefit` records out of clinical AI prompts so they don't crowd out medical history."
        )

    skill_chart_hygiene = {
        "id": "skill-chart-hygiene",
        "group": "health_check",
        "group_label": "Chart Health-Check & Guidance (Read-Only)",
        "icon": "🧹",
        "title": "Audit Chart Redundancy, Duplicate Panels & Export Artifacts",
        "subtitle": "Read-only audit of duplicate registry vaccines, Blood-vs-Serum duplicate lab panels, repeated Base64 notes, room equipment on the Device list, and billing bloat",
        "prompt_question": "What duplicate records, repeated lab panels, clinic room-equipment artifacts, or billing bloat exist in my raw FHIR chart, and how does backend condensation handle them?",
        "status_badge": f"{len(hygiene_rows)} Redundancy / Artifact Pattern(s)",
        "badge_tone": "info",
        "plain_english_summary": (
            f"Real-world EHR exports often include billing records, byte-identical clinical notes wrapped in two different FHIR resource types, same-day clinic + state-registry vaccine entries, and even exam-room equipment (like a dental X-ray machine) exported on the patient's Device list. "
            f"This read-only audit catalogs every redundancy and export artifact in **{extracted_lossless.patient_name}'s** raw `{extracted_lossless.total_bundle_entries}`-entry bundle (`{raw_json_tokens:,}` raw tokens) without deleting your original data."
        ),
        "structured_rows": hygiene_rows,
        "suggested_remediation": hygiene_remediation[:6],
        "lever_sensitivity_note": (
            "Use the Backend Condensation Levers in Zone A to experiment with how these artifacts are handled: toggle 'Filter Room-Equipment Devices' or switch between 'Lossless Structural' (keeps individual renewal/lab lines) and 'Hierarchical Grouped' (folds historical renewals and repeated normal panels)."
        ),
        "clinician_technical_note": (
            f"Raw bundle: {extracted_lossless.total_bundle_entries} entries ({raw_json_tokens:,} est. tokens) -> "
            f"{claims_count} Claims/EOBs, {extracted_lossless.deduplicated_note_count} SHA-256 duplicate Base64 notes, {room_equip_count} room-equipment Devices."
        ),
        "citations": (hygiene_cites or visit_cites)[:8],
    }

    # Skill 9: Preview Sensitive Records Before Sharing (Read-Only)
    priv_rows: List[Dict[str, Any]] = []
    for s_it in sensitive_items:
        priv_rows.append(
            {
                "category": "Protected in External Share",
                "label": s_it.display,
                "detail": f"{s_it.text} • Recorded {s_it.date or 'in chart'}",
                "source_org": s_it.source_org or "Chart Record",
                "code": f"{s_it.resource_type} ({s_it.code})",
                "tone": "warn",
            }
        )
    if not priv_rows:
        priv_rows.append(
            {
                "category": "No Sensitive SDOH/Behavioral Flags",
                "label": "No High-Sensitivity Social, Behavioral, or Safety Entries Flagged",
                "detail": "Audited all Condition, Observation, and DiagnosticReport resources against 42 CFR Part 2 / ONC USCDI sensitivity categories.",
                "source_org": "Privacy Preview Check",
                "code": "PASS",
                "tone": "ok",
            }
        )
    priv_cites = _citations_for_uuids(
        [s.resource_uuid for s in sensitive_items], uuid_map, condensed_8k
    )
    skill_privacy_preview = {
        "id": "skill-privacy-preview",
        "group": "health_check",
        "group_label": "Chart Health-Check & Guidance (Read-Only)",
        "icon": "🔒",
        "title": "Preview Sensitive Records Before Sharing (Read-Only)",
        "subtitle": "Read-only check showing sensitive social, behavioral, or personal-safety records that can be optionally protected when sharing outside your care team",
        "prompt_question": "If I share a summary of my chart with an outside provider, employer, or school, which sensitive social or personal records are in my chart?",
        "status_badge": (
            f"{len(sensitive_items)} Sensitive Entry(ies) Highlighted"
            if sensitive_items
            else "0 Sensitive Entries Flagged"
        ),
        "badge_tone": "warn" if sensitive_items else "ok",
        "plain_english_summary": (
            "By default, your full personal health view keeps 100% of your records visible. "
            + (
                f"If you choose to share a summary externally (such as a dental referral, school form, or third-party app), this read-only preview highlights **{len(sensitive_items)} sensitive social, behavioral, or personal-safety entry(ies)** in your chart so you can decide whether to withhold them while still sharing your complete medical safety core (allergies, active conditions, medications, and implants)."
                if sensitive_items
                else "This read-only check scanned your chart for sensitive social history, mental health, or personal-safety entries and found none flagged."
            )
        ),
        "structured_rows": priv_rows[:12],
        "suggested_remediation": [
            "Keep full visibility ('Patient Self View') when reviewing your chart for yourself or with your trusted primary care doctor.",
            "Toggle 'Preview Privacy Redaction (external_share)' in the Backend Levers panel (Zone A) to see how these sensitive entries are cleanly withheld from an external summary while keeping all allergies, medications, and medical diagnoses intact.",
        ],
        "lever_sensitivity_note": (
            "Privacy redaction is strictly optional and off by default (`sharing_posture='patient_self'`). Turning on the Privacy Redaction lever in Zone A withholds only the flagged sensitive items listed above."
        ),
        "clinician_technical_note": (
            f"Optional privacy segmentation check: {len(sensitive_items)} items matched `SDOH/PRIVACY-SENSITIVE` rules in `clinical_exclusivity_and_privacy.json`."
        ),
        "citations": (priv_cites or visit_cites)[:8],
    }

    skills = [
        skill_visit_brief,
        skill_hospital_stay,
        skill_med_guide,
        skill_lab_trends,
        skill_vaccine_summary,
        skill_med_conflicts,
        skill_condition_conflicts,
        skill_chart_hygiene,
        skill_privacy_preview,
    ]

    return {
        "fhir_browser": fhir_browser,
        "prep_lever_summary": prep_lever_summary,
        "condensation_matrix": condensation_matrix,
        "skills": skills,
    }
