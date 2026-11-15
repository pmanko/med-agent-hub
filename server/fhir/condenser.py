"""FHIR chart condensation compatibility adapter over `wahs_core.condenser`.

Delegates canonical 2-stage monotonic condensation (`tiered` / `hierarchical`),
`CondensedChartResult`, `estimate_tokens_local`, `items_to_querystore_records`,
and shared folding/trajectory primitives directly to `wahs_core.condenser`, while
preserving Hub-only research helpers (`build_tier_items` and
`condense_mode="lossless_structural"`) for legacy ablation and workbench suites.
"""

from __future__ import annotations

from datetime import date, timedelta
import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from wahs_core.chart_serializer import render_chart
from wahs_core.condenser import (
    CondensedChartResult,
    _build_completeness_manifest,
    _build_lab_trajectories,
    _date_sort_key,
    _excerpt_notes,
    _fold_devices,
    _fold_encounters,
    _fold_immunizations,
    _fold_lab_panels_by_type,
    _fold_procedures,
    _fold_resolved_conditions,
    _fold_social_history,
    _fold_stopped_medications,
    _group_vitals_by_date,
    _int_date,
    _merge_uuids,
    _span_phrase,
    condense_bundle_to_budget as _core_condense_bundle_to_budget,
    estimate_tokens_local,
    items_to_querystore_records,
)

from .extractors import ClinicalItem, ExtractedChart, extract_bundle
from .normalizers import classify_sensitivity


def _cutoff_iso(reference_date: Optional[str], days: int) -> str:
    if not reference_date:
        return "0000-00-00"
    try:
        ref = date.fromisoformat(reference_date[:10])
        return (ref - timedelta(days=days)).isoformat()
    except ValueError:
        return "0000-00-00"


def _compact_item_for_micro_tier(item: ClinicalItem) -> ClinicalItem:
    """Shorten verbose SNOMED/RxNorm/dosage boilerplate when building Tier 4 (<2k tokens) while
    preserving all clinical facts, dates, conflict tags, and UUID provenance."""
    txt = item.text
    txt = re.sub(r",\s*(?:SNOMED|RxNorm|CVX):\s*[A-Za-z0-9._-]+", "", txt)
    txt = re.sub(r",\s*type:\s*(?:prescribed|inpatient/administered)", "", txt)
    txt = re.sub(r",\s*verification:\s*confirmed", "", txt)
    return ClinicalItem(
        resource_type=item.resource_type,
        resource_uuid=item.resource_uuid,
        date=item.date,
        clinical_date=item.clinical_date,
        date_kind=item.date_kind,
        section=item.section,
        code=item.code,
        display=item.display,
        text=txt,
        mandatory=item.mandatory,
        status=item.status,
        end_date=item.end_date,
        source_org=item.source_org,
        sensitivity=item.sensitivity,
        conflict_note=item.conflict_note,
        ingredient_key=item.ingredient_key,
        is_routine=item.is_routine,
        member_uuids=item.member_uuids,
        metadata=item.metadata,
    )


def build_tier_items(
    extracted: ExtractedChart,
    tier: int,
    *,
    mode: str = "full",
    question: str = "",
    sharing_posture: str = "patient_self",
) -> Tuple[List[ClinicalItem], List[Dict[str, Any]], Dict[str, Any]]:
    """Construct the ordered list of `ClinicalItem`s for `tier` in `{0, 1, 2, 3, 4}`."""
    if tier not in {0, 1, 2, 3, 4}:
        raise ValueError(f"Unsupported condensation tier: {tier}")

    redacted: List[Dict[str, Any]] = []
    redacted_uuids: Set[str] = set()
    source_items: List[ClinicalItem] = []
    for it in extracted.items:
        eff_sens = it.sensitivity or (
            classify_sensitivity(it.code, f"{it.display} {it.text}")
            if sharing_posture == "external_share"
            else None
        )
        if sharing_posture == "external_share" and eff_sens == "SDOH/PRIVACY-SENSITIVE":
            redacted.append(
                {
                    "resourceType": it.resource_type,
                    "resourceUuid": it.resource_uuid,
                    "date": it.date,
                    "display": "Redacted privacy-sensitive SDOH record",
                    "sensitivity": eff_sens,
                }
            )
            if it.resource_uuid:
                redacted_uuids.add(it.resource_uuid)
            for m_uuid in it.member_uuids:
                if m_uuid:
                    redacted_uuids.add(m_uuid)
            continue
        if mode == "reconcile" and it.section == "clinical_note":
            continue
        source_items.append(it)

    allowed_numeric_points = [
        pt
        for pt in extracted.numeric_points
        if pt.resource_uuid not in redacted_uuids
        and not (
            sharing_posture == "external_share"
            and classify_sensitivity(pt.code, pt.concept, [pt.category]) == "SDOH/PRIVACY-SENSITIVE"
        )
    ]

    patient_items = [x for x in source_items if x.section == "patient"]
    allergies = [x for x in source_items if x.section == "allergy"]
    cond_active = [x for x in source_items if x.section == "condition_active"]
    cond_resolved = [x for x in source_items if x.section == "condition_resolved"]
    med_active = [x for x in source_items if x.section == "med_active"]
    med_hist = [x for x in source_items if x.section == "med_historical"]
    imms = [x for x in source_items if x.section == "immunization"]
    devices = [x for x in source_items if x.section == "device"]
    careplans_active = [x for x in source_items if x.section == "careplan_active"]
    careplans_hist = [x for x in source_items if x.section == "careplan_historical"]
    procs_major = [x for x in source_items if x.section == "procedure_major"]
    procs_routine = [x for x in source_items if x.section == "procedure_routine"]
    enc_acuity = [x for x in source_items if x.section == "encounter_acuity"]
    enc_routine = [x for x in source_items if x.section == "encounter_routine"]
    lab_panels = [x for x in source_items if x.section == "lab_panel"]
    survey_panels = [x for x in source_items if x.section == "survey_panel"]
    vitals = [x for x in source_items if x.section == "vital_sign"]
    labs_standalone = sorted(
        [x for x in source_items if x.section == "lab_observation"],
        key=lambda x: _date_sort_key(x.date),
        reverse=True,
    )
    social_obs = [x for x in source_items if x.section == "social_history"]
    survey_obs = sorted(
        [x for x in source_items if x.section == "survey_observation"],
        key=lambda x: _date_sort_key(x.date),
        reverse=True,
    )
    other_obs = [x for x in source_items if x.section == "observation"]
    imaging = sorted(
        [x for x in source_items if x.section == "imaging"],
        key=lambda x: _date_sort_key(x.date),
        reverse=True,
    )
    notes = sorted(
        [x for x in source_items if x.section == "clinical_note"],
        key=lambda x: _date_sort_key(x.date),
        reverse=True,
    )
    excerpted_notes = _excerpt_notes(notes, max_chars=400)

    tier_names = {
        0: "Full Normalized Ledger",
        1: "Panel-Grouped & Renewal-Folded Ledger",
        2: "IPS Core + 24m Horizon Window",
        3: "IPS Core + Longitudinal Analyte Trajectories",
        4: "IPS Micro-Capsule",
    }

    if tier == 0:
        core = patient_items + allergies + cond_active + med_active + _fold_devices(devices)
        rest = (
            imms
            + procs_major
            + enc_acuity
            + careplans_active
            + lab_panels
            + labs_standalone
            + vitals
            + social_obs
            + other_obs
            + notes
            + imaging
            + cond_resolved
            + med_hist
            + survey_panels
            + survey_obs
            + procs_routine
            + careplans_hist
            + enc_routine
        )
        rest.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        body_items = core + rest

    elif tier == 1:
        folded_devices = _fold_devices(devices)
        folded_med_hist = _fold_stopped_medications(med_hist)
        folded_cond_res = _fold_resolved_conditions(cond_resolved)
        folded_imms = _fold_immunizations(imms)
        folded_procs_major = _fold_procedures(procs_major, section="procedure_major")
        folded_procs_routine = _fold_procedures(procs_routine, section="procedure_routine")
        folded_enc_acuity = _fold_encounters(enc_acuity, keep_latest_individual=5)
        folded_enc_routine = _fold_encounters(enc_routine, keep_latest_individual=5)
        grouped_vitals = _group_vitals_by_date(vitals)[:16]
        folded_social = _fold_social_history(social_obs)
        folded_labs = _fold_lab_panels_by_type(lab_panels, keep_latest_per_type=3)
        folded_surveys = _fold_lab_panels_by_type(survey_panels, keep_latest_per_type=2)
        trajectories = _build_lab_trajectories(allowed_numeric_points, max_analytes=24)

        core = patient_items + allergies + cond_active + med_active + folded_devices
        rest = (
            folded_imms
            + folded_procs_major
            + folded_enc_acuity
            + careplans_active
            + folded_cond_res
            + folded_med_hist
            + trajectories
            + folded_labs
            + labs_standalone[:24]
            + grouped_vitals
            + folded_social
            + other_obs
            + excerpted_notes[:20]
            + imaging
            + folded_surveys
            + survey_obs[:16]
            + folded_procs_routine
            + careplans_hist
            + folded_enc_routine
        )
        rest.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        body_items = core + rest

    elif tier == 2:
        cutoff_24m = _cutoff_iso(extracted.reference_date, 730)

        folded_devices = _fold_devices(devices)
        folded_med_hist = _fold_stopped_medications(med_hist)
        folded_cond_res = _fold_resolved_conditions(cond_resolved)
        folded_imms = _fold_immunizations(imms)
        folded_procs_major = _fold_procedures(procs_major, section="procedure_major")
        folded_procs_routine = _fold_procedures(procs_routine, section="procedure_routine")
        folded_enc_acuity = _fold_encounters(enc_acuity, keep_latest_individual=3)
        folded_enc_routine = _fold_encounters(enc_routine, keep_latest_individual=2)
        grouped_vitals = _group_vitals_by_date(vitals)[:6]
        folded_social = _fold_social_history(social_obs)
        folded_labs = _fold_lab_panels_by_type(lab_panels, keep_latest_per_type=1)
        folded_surveys = _fold_lab_panels_by_type(survey_panels, keep_latest_per_type=1)
        trajectories = _build_lab_trajectories(allowed_numeric_points, max_analytes=24)

        recent_notes = sorted(
            [n for n in excerpted_notes if _date_sort_key(n.date) >= cutoff_24m],
            key=lambda x: _date_sort_key(x.date),
            reverse=True,
        )[:6]
        if not recent_notes and excerpted_notes:
            recent_notes = excerpted_notes[:3]

        core = patient_items + allergies + cond_active + med_active + folded_devices
        rest = (
            folded_imms
            + folded_procs_major
            + folded_enc_acuity
            + careplans_active
            + folded_cond_res
            + folded_med_hist
            + trajectories
            + folded_labs
            + labs_standalone[:10]
            + grouped_vitals
            + folded_social
            + other_obs
            + folded_surveys
            + survey_obs[:8]
            + recent_notes
            + imaging[:6]
            + folded_procs_routine[:8]
            + folded_enc_routine
        )
        rest.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        body_items = core + rest

    elif tier == 3:
        folded_devices = _fold_devices(devices)
        folded_med_hist = _fold_stopped_medications(med_hist)
        folded_cond_res = _fold_resolved_conditions(cond_resolved)
        folded_imms = _fold_immunizations(imms)
        folded_procs_major = _fold_procedures(procs_major, section="procedure_major")
        folded_enc_acuity = _fold_encounters(enc_acuity, keep_latest_individual=2)
        folded_social = _fold_social_history(social_obs)
        trajectories = _build_lab_trajectories(allowed_numeric_points, max_analytes=22)

        med_cond_res = [c for c in folded_cond_res if not c.is_routine]
        sdoh_cond_res = [c for c in folded_cond_res if c.is_routine]
        if sdoh_cond_res:
            l_date, span_str = _span_phrase([c.date for c in sdoh_cond_res])
            names = ", ".join(c.display for c in sdoh_cond_res[:6])
            med_cond_res.append(
                ClinicalItem(
                    resource_type="Condition",
                    resource_uuid=sdoh_cond_res[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="condition_resolved",
                    code="sdoh-summary",
                    display="Resolved Social/Administrative Findings",
                    text=f"Condition History (Social/Admin): {names} ({span_str})",
                    mandatory=False,
                    is_routine=True,
                    member_uuids=_merge_uuids(sdoh_cond_res),
                )
            )

        anomalous_notes_t3 = [n for n in excerpted_notes if n.conflict_note]
        other_notes_t3 = [n for n in excerpted_notes if not n.conflict_note]
        core = patient_items + allergies + cond_active + med_active + folded_devices + anomalous_notes_t3
        rest = (
            folded_imms
            + folded_procs_major[:10]
            + folded_enc_acuity
            + careplans_active
            + trajectories
            + med_cond_res
            + folded_med_hist[:12]
            + folded_social[:4]
            + other_notes_t3[:1]
        )
        rest.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        body_items = core + rest

    else:
        # Tier 4: IPS Micro-Capsule (<= 1k - 2k token budgets)
        folded_devices = [_compact_item_for_micro_tier(x) for x in _fold_devices(devices)]
        folded_imms = _fold_immunizations(imms)
        anomalous_imms = [_compact_item_for_micro_tier(x) for x in folded_imms if x.conflict_note]
        routine_imms = [x for x in folded_imms if not x.conflict_note]
        imm_summary: List[ClinicalItem] = []
        if routine_imms:
            l_date, span_str = _span_phrase([x.date for x in routine_imms])
            v_names = ", ".join(dict.fromkeys(x.display for x in routine_imms[:6]))
            imm_summary.append(
                ClinicalItem(
                    resource_type="Immunization",
                    resource_uuid=routine_imms[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="immunization",
                    code="routine-immunizations",
                    display="Routine Immunizations",
                    text=f"Immunization Summary: {len(routine_imms)} routine vaccine series ({v_names}; {span_str})",
                    mandatory=False,
                    member_uuids=_merge_uuids(routine_imms),
                )
            )

        compact_allergies = [_compact_item_for_micro_tier(x) for x in allergies]
        compact_cond_active = [
            _compact_item_for_micro_tier(x)
            for x in cond_active
            if x.sensitivity != "SDOH-SOCIAL"
        ]
        anomalous_cond_res = [
            _compact_item_for_micro_tier(x)
            for x in cond_resolved
            if x.conflict_note
        ]
        sdoh_active = [x for x in cond_active if x.sensitivity == "SDOH-SOCIAL"]
        if sdoh_active:
            l_date, _ = _span_phrase([x.date for x in sdoh_active])
            s_names = ", ".join(f"{x.display} ({x.date})" for x in sdoh_active)
            compact_cond_active.append(
                ClinicalItem(
                    resource_type="Condition",
                    resource_uuid=sdoh_active[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="condition_active",
                    code="active-social-status",
                    display="Active Social/Admin Findings",
                    text=f"Condition (status: active, social/admin): {s_names}",
                    mandatory=True,
                    member_uuids=_merge_uuids(sdoh_active),
                )
            )
        compact_med_active = [_compact_item_for_micro_tier(x) for x in med_active]

        folded_med_hist = _fold_stopped_medications(med_hist)
        inpatient_meds = [m for m in med_hist if m.resource_type == "MedicationAdministration"]
        med_hist_summary: List[ClinicalItem] = []
        if inpatient_meds:
            l_date, span_str = _span_phrase([m.date for m in inpatient_meds])
            m_names = ", ".join(dict.fromkeys(m.display for m in inpatient_meds[:6]))
            med_hist_summary.append(
                ClinicalItem(
                    resource_type="MedicationAdministration",
                    resource_uuid=inpatient_meds[-1].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="med_historical",
                    code="inpatient-meds",
                    display="Inpatient Medications",
                    text=f"Inpatient/Perioperative Medications ({span_str}): {m_names}",
                    mandatory=False,
                    member_uuids=_merge_uuids(inpatient_meds),
                )
            )
        if folded_med_hist:
            l_date, span_str = _span_phrase([m.date for m in folded_med_hist])
            m_names = ", ".join(dict.fromkeys(m.display for m in folded_med_hist[:8]))
            med_hist_summary.append(
                ClinicalItem(
                    resource_type=folded_med_hist[0].resource_type,
                    resource_uuid=folded_med_hist[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="med_historical",
                    code="stopped-meds",
                    display="Historical Medications",
                    text=f"Stopped/Historical Medications ({span_str}): {m_names}",
                    mandatory=False,
                    member_uuids=_merge_uuids(folded_med_hist),
                )
            )

        trajectories = _build_lab_trajectories(allowed_numeric_points, max_analytes=12)
        folded_procs_major = _fold_procedures(procs_major, section="procedure_major")
        folded_enc_acuity = _fold_encounters(enc_acuity, keep_latest_individual=1)

        anomalous_notes_t4 = [_compact_item_for_micro_tier(n) for n in excerpted_notes if n.conflict_note]
        core = (
            patient_items
            + compact_allergies
            + compact_cond_active
            + anomalous_cond_res
            + compact_med_active
            + anomalous_imms
            + folded_devices
            + anomalous_notes_t4
        )
        rest = (
            trajectories
            + med_hist_summary
            + imm_summary
            + folded_procs_major[:5]
            + folded_enc_acuity[:3]
        )
        rest.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        body_items = core + rest

    manifest_item, manifest_dict = _build_completeness_manifest(
        extracted, tier, tier_names[tier], body_items, len(redacted)
    )
    if body_items and body_items[0].section == "patient":
        final_items = [body_items[0], manifest_item] + body_items[1:]
    else:
        final_items = [manifest_item] + body_items

    return final_items, redacted, manifest_dict


def _strip_prebaked_tags(item: ClinicalItem) -> ClinicalItem:
    """Remove pre-baked `[conflict_note: ...]` / `[ingredient_group: ...]` / `[sensitivity: ...]`
    annotations from `ClinicalItem.text` and reset `conflict_note=None` for `lossless_structural` mode.
    """
    cleaned_text = re.sub(
        r"\s*\[(?:conflict_note:|ingredient_group:|acip_schedule_note:|duplicate_immunization_note:|note_medication_contradiction:|sensitivity:)[^\]]*\]",
        "",
        item.text or "",
    )
    is_core_mandatory = item.section in {
        "patient",
        "allergy",
        "condition_active",
        "med_active",
    } or (item.section == "device" and item.status == "active")
    return ClinicalItem(
        resource_type=item.resource_type,
        resource_uuid=item.resource_uuid,
        date=item.date,
        clinical_date=item.clinical_date,
        date_kind=item.date_kind,
        section=item.section,
        code=item.code,
        display=item.display,
        text=cleaned_text,
        mandatory=is_core_mandatory,
        status=item.status,
        end_date=item.end_date,
        source_org=item.source_org,
        sensitivity=None,
        conflict_note=None,
        ingredient_key=item.ingredient_key,
        is_routine=item.is_routine,
        member_uuids=item.member_uuids if item.section == "clinical_note" else (),
        metadata=item.metadata,
    )


def _condense_lossless_structural(
    extracted: ExtractedChart,
    *,
    target_tokens: int,
    question: str,
    sharing_posture: str,
    token_counter: Callable[[str], int],
) -> CondensedChartResult:
    """Non-destructive structural condensation (`W12`):
    - Strips FHIR JSON boilerplate without mutating clinical content or pre-annotating `[conflict_note]` tags.
    - Preserves active conditions, active medications, allergies, and active patient devices verbatim
      (1 line per FHIR resource; no folding by ingredient or clinical display).
    - Orders optional records (lab trajectories, recent clinical notes, immunizations, resolved conditions,
      historical medications, procedures, encounters) chronologically and packs them up to `target_tokens`
      while recording exact overflow counts per section in `[Completeness Manifest]`.
    """
    redacted: List[Dict[str, Any]] = []
    redacted_uuids: Set[str] = set()
    clean_items: List[ClinicalItem] = []
    for raw_it in extracted.items:
        eff_sens = raw_it.sensitivity or (
            classify_sensitivity(raw_it.code, f"{raw_it.display} {raw_it.text}")
            if sharing_posture == "external_share"
            else None
        )
        if sharing_posture == "external_share" and eff_sens == "SDOH/PRIVACY-SENSITIVE":
            redacted.append(
                {
                    "resourceType": raw_it.resource_type,
                    "resourceUuid": raw_it.resource_uuid,
                    "date": raw_it.date,
                    "display": "Redacted privacy-sensitive SDOH record",
                    "sensitivity": eff_sens,
                }
            )
            if raw_it.resource_uuid:
                redacted_uuids.add(raw_it.resource_uuid)
            for m_uuid in raw_it.member_uuids:
                if m_uuid:
                    redacted_uuids.add(m_uuid)
            continue
        clean_items.append(_strip_prebaked_tags(raw_it))

    allowed_numeric_points = [
        pt
        for pt in extracted.numeric_points
        if pt.resource_uuid not in redacted_uuids
        and not (
            sharing_posture == "external_share"
            and classify_sensitivity(pt.code, pt.concept, [pt.category]) == "SDOH/PRIVACY-SENSITIVE"
        )
    ]
    trajectories = _build_lab_trajectories(allowed_numeric_points, max_analytes=24)

    mandatory_items = [it for it in clean_items if it.mandatory]
    optional_raw = [it for it in clean_items if not it.mandatory] + trajectories

    q_terms = {
        tok.lower()
        for tok in re.findall(r"[A-Za-z0-9]+", question or "")
        if len(tok) >= 3
    }
    section_rank = {
        "lab_trajectory": 1,
        "immunization": 2,
        "condition_resolved": 3,
        "encounter_acuity": 4,
        "procedure_major": 5,
        "clinical_note": 6,
        "med_historical": 7,
        "vital_sign": 8,
        "careplan_active": 9,
        "lab_panel": 10,
        "lab_observation": 11,
        "social_history": 12,
        "imaging": 13,
        "survey_panel": 14,
        "procedure_routine": 15,
        "encounter_routine": 16,
    }
    if target_tokens >= 192000:
        scale = 16.0
    elif target_tokens >= 96000:
        scale = 8.0
    elif target_tokens >= 48000:
        scale = 4.0
    elif target_tokens >= 24000:
        scale = 2.5
    elif target_tokens >= 12000:
        scale = 1.6
    elif target_tokens >= 6000:
        scale = 1.0
    elif target_tokens >= 3000:
        scale = 0.45
    else:
        scale = 0.20
    primary_window_caps = {
        "lab_trajectory": max(6, int(24 * scale)),
        "immunization": max(3, int(24 * scale)),
        "condition_resolved": max(3, int(25 * scale)),
        "encounter_acuity": max(2, int(10 * scale)),
        "procedure_major": max(3, int(18 * scale)),
        "clinical_note": max(2, int(8 * scale)),
        "med_historical": max(4, int(35 * scale)),
        "vital_sign": max(4, int(12 * scale)),
        "lab_panel": max(2, int(10 * scale)),
    }
    by_sec_ordered: Dict[str, List[ClinicalItem]] = {}
    for it in optional_raw:
        by_sec_ordered.setdefault(it.section, []).append(it)
    within_sec_rank: Dict[int, int] = {}
    for sec_name, sec_items in by_sec_ordered.items():
        if sec_name == "med_historical":
            sec_items.sort(
                key=lambda x: (
                    1 if x.metadata.get("setting") == "inpatient/administered" else 0,
                    _date_sort_key(x.date),
                ),
                reverse=True,
            )
        elif sec_name in {"procedure_major", "encounter_acuity"}:
            sec_items.sort(
                key=lambda x: (
                    1
                    if any(
                        w in x.display.lower()
                        for w in ("bypass", "cabg", "graft", "stent", "valve", "intensive care", "icu", "surgery")
                    )
                    else 0,
                    _date_sort_key(x.date),
                ),
                reverse=True,
            )
        else:
            sec_items.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        for idx_in_sec, x in enumerate(sec_items):
            within_sec_rank[id(x)] = idx_in_sec

    ranked_optional = sorted(
        optional_raw,
        key=lambda it: (
            -sum(1 for t in q_terms if t in f"{it.display} {it.text}".lower()),
            0 if within_sec_rank.get(id(it), 999) < primary_window_caps.get(it.section, 4) else 1,
            section_rank.get(it.section, 20),
            within_sec_rank.get(id(it), 999),
            -_int_date(it.date),
        ),
    )

    def render_structural(
        cand_body: Sequence[ClinicalItem],
        _legacy_omitted_by_sec: Dict[str, int],
    ) -> Tuple[List[ClinicalItem], Dict[str, Any], str, List[Dict[str, Any]], int]:
        _, probe_manifest = _build_completeness_manifest(
            extracted, 0, "Lossless Structural Ledger", cand_body, len(redacted)
        )
        omitted_by_sec = dict(probe_manifest.get("omitted_by_section") or {})
        tier_num = 0 if not omitted_by_sec else (1 if target_tokens >= 8192 else (2 if target_tokens >= 4096 else 3))
        tier_label = (
            "Lossless Structural Ledger"
            if not omitted_by_sec
            else f"Lossless Structural Window ({target_tokens}t budget)"
        )
        manifest_item, manifest_dict = _build_completeness_manifest(
            extracted, tier_num, tier_label, cand_body, len(redacted)
        )
        if omitted_by_sec:
            omitted_str = ", ".join(f"{k}={v}" for k, v in sorted(omitted_by_sec.items()))
            manifest_item = ClinicalItem(
                resource_type=manifest_item.resource_type,
                resource_uuid=manifest_item.resource_uuid,
                date=manifest_item.date,
                clinical_date=manifest_item.clinical_date,
                date_kind=manifest_item.date_kind,
                section="manifest",
                code=manifest_item.code,
                display=manifest_item.display,
                text=f"{manifest_item.text} | budget_overflow_omitted: {omitted_str}",
                mandatory=True,
                status="current",
            )
            manifest_dict["omitted_section_counts"] = dict(omitted_by_sec)
        else:
            manifest_dict["omitted_section_counts"] = {}
        manifest_dict["condense_mode"] = "lossless_structural"

        if cand_body and cand_body[0].section == "patient":
            ordered = [cand_body[0], manifest_item] + list(cand_body[1:])
        else:
            ordered = [manifest_item] + list(cand_body)
        recs = items_to_querystore_records(ordered)
        c_text, c_maps = render_chart(recs)
        toks = token_counter(c_text)
        return ordered, manifest_dict, c_text, c_maps, toks

    # Reserve at least 30% of budget for top trajectories/notes when mandatory items are large
    mand_ceiling = int(target_tokens * 0.70) if target_tokens <= 4096 else target_tokens
    kept_mandatory: List[ClinicalItem] = []
    omitted_counts: Dict[str, int] = {}
    running_est = 95  # manifest + header reserve
    for m_it in mandatory_items:
        est_it = max(8, token_counter(m_it.text) + 6)
        if running_est + est_it <= mand_ceiling or m_it.section in {"patient", "allergy"}:
            kept_mandatory.append(m_it)
            running_est += est_it
        else:
            omitted_counts[m_it.section] = omitted_counts.get(m_it.section, 0) + 1

    accepted_optional: List[ClinicalItem] = []
    soft_ceiling = max(128, target_tokens - 90)
    for opt in ranked_optional:
        est_opt = max(8, token_counter(opt.text) + 6)
        if running_est + est_opt <= soft_ceiling:
            accepted_optional.append(opt)
            running_est += est_opt
        else:
            omitted_counts[opt.section] = omitted_counts.get(opt.section, 0) + 1

    final_items, manifest_dict, chart_text, mappings, actual_toks = render_structural(
        kept_mandatory + sorted(accepted_optional, key=lambda x: _date_sort_key(x.date), reverse=True),
        omitted_counts,
    )
    while actual_toks > target_tokens and accepted_optional:
        dropped = accepted_optional.pop()
        omitted_counts[dropped.section] = omitted_counts.get(dropped.section, 0) + 1
        final_items, manifest_dict, chart_text, mappings, actual_toks = render_structural(
            kept_mandatory + sorted(accepted_optional, key=lambda x: _date_sort_key(x.date), reverse=True),
            omitted_counts,
        )
    records = items_to_querystore_records(final_items)
    return CondensedChartResult(
        tier=manifest_dict["tier"],
        tier_name=manifest_dict["tier_name"],
        target_tokens=target_tokens,
        actual_tokens=actual_toks,
        chart_text=chart_text,
        records=records,
        mappings=mappings,
        completeness_manifest=manifest_dict,
        redacted_for_privacy=redacted,
        compression_stats={
            "condense_mode": "lossless_structural",
            "bundle_entries": extracted.total_bundle_entries,
            "extracted_clinical_items": len(extracted.items),
            "rendered_records": len(records),
            "target_tokens": target_tokens,
            "actual_tokens": actual_toks,
            "budget_utilization": round(actual_toks / target_tokens, 3),
        },
    )


def condense_bundle_to_budget(
    bundle: Mapping[str, Any] | ExtractedChart,
    *,
    target_tokens: int = 8192,
    mode: str = "full",
    condense_mode: str = "tiered",
    question: str = "",
    sharing_posture: str = "patient_self",
    filter_room_equipment_devices: Optional[bool] = None,
    token_counter: Optional[Callable[[str], int]] = None,
) -> CondensedChartResult:
    """Condense a FHIR R4 Bundle (or `ExtractedChart`) into `target_tokens`.

    Delegates canonical `tiered` / `hierarchical` condensation (including the 2-stage
    monotonic structural pipeline and temporal digest support in `wahs_core`) directly to
    `wahs_core.condenser.condense_bundle_to_budget`, while routing Hub-only
    `condense_mode="lossless_structural"` through the local lossless adapter.
    """
    if not isinstance(target_tokens, int) or isinstance(target_tokens, bool) or target_tokens <= 0:
        raise ValueError("target_tokens must be a positive integer")
    if mode not in {"full", "reconcile"}:
        raise ValueError(f"Unsupported condensation mode: {mode!r}")
    if sharing_posture not in {"patient_self", "external_share"}:
        raise ValueError(f"Unsupported sharing_posture: {sharing_posture!r}")

    if condense_mode == "lossless_structural":
        counter = token_counter or estimate_tokens_local
        filter_equip = False if filter_room_equipment_devices is None else filter_room_equipment_devices
        extracted = (
            bundle
            if isinstance(bundle, ExtractedChart)
            else extract_bundle(
                bundle,
                include_conflict_annotations=False,
                enable_privacy_classification=(sharing_posture == "external_share"),
                preserve_intraday_timestamps=True,
                preserve_all_note_sections=True,
                filter_room_equipment_devices=filter_equip,
            )
        )
        return _condense_lossless_structural(
            extracted,
            target_tokens=target_tokens,
            question=question,
            sharing_posture=sharing_posture,
            token_counter=counter,
        )

    return _core_condense_bundle_to_budget(
        bundle,
        target_tokens=target_tokens,
        mode=mode,
        condense_mode=condense_mode,
        question=question,
        sharing_posture=sharing_posture,
        filter_room_equipment_devices=filter_room_equipment_devices,
        token_counter=token_counter,
    )


__all__ = [
    "CondensedChartResult",
    "build_tier_items",
    "condense_bundle_to_budget",
    "estimate_tokens_local",
    "items_to_querystore_records",
]
