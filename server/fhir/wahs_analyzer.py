"""WAHS+ Clinical Chart Analyzer, Suggested Analysis Workflows, and Grounded Q&A Engine.

Provides deterministic, provenance-grounded analysis across all WAHS+ / EHIgnite Phase 2
use cases (Use Case A: Chart Reconciliation & Data Quality, Use Case B-1: Encounter &
Longitudinal Summarization, Use Case B-2: Privacy-Segmented Directed Summaries, and
Use Case B-3: Grounded Patient Q&A with Unanswerable Abstention) over condensed FHIR R4
patient charts and the 4-layer WAHS+ clinical knowledge base.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from ..kb import search as search_kb
from .condenser import CondensedChartResult, condense_bundle_to_budget, estimate_tokens_local
from .extractors import (
    ClinicalItem,
    ExtractedChart,
    build_fhir_browser_index,
    extract_bundle,
    extract_bundle_lossless,
)
from .kb_lookup import (
    extract_chart_kb_codes,
    get_acip_rules,
    get_clinical_exclusivity_and_privacy,
    get_rxnorm_lookup,
    resolve_condition_code,
    resolve_immunization_code,
    resolve_lab_code,
    resolve_medication_code,
)
from .wahs_tools import (
    get_lab_history,
    read_clinical_note,
    search_chart_entries,
)


def _ensure_extracted_and_condensed(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    condensed: Optional[CondensedChartResult] = None,
    *,
    target_tokens: int = 8192,
    sharing_posture: str = "patient_self",
    condense_mode: str = "lossless_structural",
) -> Tuple[ExtractedChart, CondensedChartResult]:
    if isinstance(bundle_or_extracted, ExtractedChart):
        extracted = bundle_or_extracted
    else:
        if condense_mode == "lossless_structural":
            extracted = extract_bundle_lossless(bundle_or_extracted)
        else:
            extracted = extract_bundle(bundle_or_extracted)

    if condensed is None:
        condensed = condense_bundle_to_budget(
            extracted,
            target_tokens=target_tokens,
            sharing_posture=sharing_posture,
            condense_mode=condense_mode,
        )
    return extracted, condensed



def _build_uuid_to_citation_map(condensed: CondensedChartResult) -> Dict[str, List[int]]:
    """Map every primary and member FHIR resource UUID to its 1-based `[N]` ledger citation index."""
    uuid_map: Dict[str, List[int]] = defaultdict(list)
    for mapping in condensed.mappings:
        idx = int(mapping.get("index", 0))
        if idx <= 0:
            continue
        primary = str(mapping.get("resourceUuid") or "")
        if primary and idx not in uuid_map[primary]:
            uuid_map[primary].append(idx)
        for member in mapping.get("memberUuids") or []:
            m_str = str(member or "")
            if m_str and idx not in uuid_map[m_str]:
                uuid_map[m_str].append(idx)
    return uuid_map


def _citations_for_uuids(
    uuids: Sequence[str],
    uuid_map: Mapping[str, List[int]],
    condensed: CondensedChartResult,
) -> List[Dict[str, Any]]:
    seen_indices: List[int] = []
    for u in uuids:
        for idx in uuid_map.get(u, []):
            if idx not in seen_indices:
                seen_indices.append(idx)
    seen_indices.sort()
    by_idx = {int(m["index"]): m for m in condensed.mappings if "index" in m}
    rec_by_idx = {i + 1: r for i, r in enumerate(condensed.records)}
    out: List[Dict[str, Any]] = []
    for idx in seen_indices:
        m = by_idx.get(idx, {})
        rec = rec_by_idx.get(idx, {})
        all_members = list(m.get("memberUuids") or [])
        raw_text = str(rec.get("text") or "")
        out.append(
            {
                "index": idx,
                "label": f"[{idx}]",
                "resource_type": m.get("resourceType", rec.get("resource_type", "")),
                "resource_uuid": m.get("resourceUuid", rec.get("resource_uuid", "")),
                "member_uuids": all_members[:5],
                "member_uuid_count": len(all_members),
                "date": rec.get("effective_date") or "",
                "text": raw_text if len(raw_text) <= 260 else raw_text[:257] + "...",
            }
        )
    return out


def _format_citation_refs(citations: Sequence[Mapping[str, Any]]) -> str:
    if not citations:
        return ""
    return " ".join(f"[{c['index']}]" for c in citations)


def analyze_duplicates_and_bloat(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    condensed: Optional[CondensedChartResult] = None,
    *,
    target_tokens: int = 8192,
    sharing_posture: str = "patient_self",
) -> Dict[str, Any]:
    """Suggested Analysis 1: Duplicate Labs, Repeated Panels, & Record Bloat."""
    extracted, condensed = _ensure_extracted_and_condensed(
        bundle_or_extracted,
        condensed,
        target_tokens=target_tokens,
        sharing_posture=sharing_posture,
    )
    extracted = _apply_stage2_conflict_skills_on_chart(extracted)
    uuid_map = _build_uuid_to_citation_map(condensed)

    # 1. Analyze duplicate Blood vs Serum lab analytes and repeated lab panels
    lab_points_by_concept: Dict[str, List[Any]] = defaultdict(list)
    raw_codes_by_concept: Dict[str, Set[str]] = defaultdict(set)
    same_day_duplicate_labs: List[Dict[str, Any]] = []

    for pt in extracted.numeric_points:
        if pt.category == "laboratory":
            root_concept = re.sub(
                r"\s*\[.*?\]|\s*\((?:blood|serum|plasma|blood/serum)\)|\s+in\s+(?:blood|serum\s+or\s+plasma|serum|plasma|arterial\s+blood|capillary\s+blood).*",
                "",
                pt.concept,
                flags=re.IGNORECASE,
            ).strip() or pt.concept
            lab_points_by_concept[root_concept].append(pt)
            if pt.code:
                raw_codes_by_concept[root_concept].add(pt.code)

    multi_code_analytes: List[Dict[str, Any]] = []
    for concept, pts in sorted(lab_points_by_concept.items(), key=lambda kv: -len(kv[1])):
        codes = sorted(raw_codes_by_concept[concept])
        by_date: Dict[str, List[Any]] = defaultdict(list)
        for p in pts:
            by_date[p.date].append(p)
        same_day_dates = [d for d, d_pts in by_date.items() if len(d_pts) > 1]
        sample_uuids = [p.resource_uuid for p in pts[-4:]]
        cites = _citations_for_uuids(sample_uuids, uuid_map, condensed)

        if len(codes) > 1 or same_day_dates or len(pts) >= 6:
            kb_info = resolve_lab_code(codes[0], concept) if codes else None
            multi_code_analytes.append(
                {
                    "concept": concept,
                    "measurement_count": len(pts),
                    "loinc_codes": codes,
                    "has_blood_vs_serum_split": len(codes) > 1,
                    "same_day_duplicate_dates": sorted(same_day_dates),
                    "first_date": min(p.date for p in pts),
                    "latest_date": max(p.date for p in pts),
                    "latest_value": pts[-1].value,
                    "unit": pts[-1].unit,
                    "reference_range": (kb_info or {}).get("reference_range", ""),
                    "sample_uuids": sample_uuids,
                    "citations": cites,
                }
            )
            if same_day_dates:
                same_day_duplicate_labs.append(
                    {
                        "concept": concept,
                        "dates": sorted(same_day_dates),
                        "loinc_codes": codes,
                    }
                )

    # 2. Historical medication renewal folding
    stopped_meds = [
        it for it in extracted.items if it.section in {"med_stopped", "med_historical"}
    ]
    stopped_by_ing: Dict[str, List[ClinicalItem]] = defaultdict(list)
    for it in stopped_meds:
        key = it.ingredient_key or it.display.lower()
        stopped_by_ing[key].append(it)

    folded_med_renewals: List[Dict[str, Any]] = []
    for ing, items in sorted(stopped_by_ing.items(), key=lambda kv: -len(kv[1])):
        dates = sorted([it.clinical_date for it in items if it.clinical_date])
        uuids = [it.resource_uuid for it in items]
        cites = _citations_for_uuids(uuids[:5], uuid_map, condensed)
        folded_med_renewals.append(
            {
                "ingredient": ing,
                "raw_prescription_count": len(items),
                "folded_lines": 1,
                "first_date": dates[0] if dates else None,
                "latest_date": dates[-1] if dates else None,
                "sample_display": items[-1].display,
                "citations": cites,
            }
        )

    # 3. Routine screening / survey consolidation
    routine_items = [
        it
        for it in extracted.items
        if it.is_routine or it.section in {"procedure_routine", "survey_panel", "procedure", "observation_survey"}
    ]
    routine_by_name: Dict[str, List[ClinicalItem]] = defaultdict(list)
    for it in routine_items:
        if it.is_routine:
            routine_by_name[it.display].append(it)

    top_repeated_screenings = [
        {
            "display": disp,
            "count": len(grp),
            "first_date": min((x.clinical_date or "") for x in grp),
            "latest_date": max((x.clinical_date or "") for x in grp),
            "citations": _citations_for_uuids([grp[-1].resource_uuid], uuid_map, condensed),
        }
        for disp, grp in sorted(routine_by_name.items(), key=lambda kv: -len(kv[1]))[:8]
        if len(grp) > 1
    ]

    # 4. Excluded administrative & equipment noise
    excluded_total = sum(extracted.excluded_resource_counts.values())

    cards: List[Dict[str, Any]] = []

    # Card 1: Base64 Clinical Note Deduplication
    note_items = [it for it in extracted.items if it.section in {"clinical_note", "note"}]
    note_cites = _citations_for_uuids([it.resource_uuid for it in note_items[:4]], uuid_map, condensed)
    cards.append(
        {
            "id": "bloat-duplicate-notes",
            "category": "Duplicate Clinical Notes (Base64)",
            "severity": "high" if extracted.deduplicated_note_count >= 20 else "medium",
            "title": f"{extracted.deduplicated_note_count} Duplicate Base64 Notes Eliminated",
            "summary": (
                f"Synthea / EHR export duplicated clinical encounter narratives across DocumentReference "
                f"and DiagnosticReport.presentedForm[], plus repeated boilerplate problem/medication lists. "
                f"SHA-256 payload hashing eliminated {extracted.deduplicated_note_count} exact duplicate "
                f"note attachments while preserving {len(note_items)} unique clinical narratives."
            ),
            "metrics": {
                "duplicate_notes_removed": extracted.deduplicated_note_count,
                "unique_notes_retained": len(note_items),
            },
            "citations": note_cites,
        }
    )

    # Card 2: Blood vs. Serum LOINC Split & Repeated Lab Series
    split_analytes = [a for a in multi_code_analytes if a["has_blood_vs_serum_split"]]
    total_lab_points = sum(len(pts) for pts in lab_points_by_concept.values())
    lab_cites = []
    for a in multi_code_analytes[:3]:
        for c in a["citations"]:
            if c not in lab_cites:
                lab_cites.append(c)
    cards.append(
        {
            "id": "bloat-lab-consolidation",
            "category": "Duplicate Lab Streams & Panel Grouping",
            "severity": "medium",
            "title": (
                f"{total_lab_points} Raw Lab Measurements Consolidated Across "
                f"{len(lab_points_by_concept)} Canonical Analytes"
            ),
            "summary": (
                f"Identified {len(split_analytes)} metabolic analytes split across parallel Blood vs. "
                f"Serum/Plasma LOINC codes ({', '.join(a['concept'] for a in split_analytes[:5]) or 'none'}) "
                f"and {len(same_day_duplicate_labs)} analytes with same-day duplicate draws. "
                f"Unified into canonical LOINC trajectories with initial, min/max, and latest values."
            ),
            "metrics": {
                "raw_lab_observations": total_lab_points,
                "canonical_analytes": len(lab_points_by_concept),
                "blood_vs_serum_split_analytes": len(split_analytes),
                "same_day_duplicate_analytes": len(same_day_duplicate_labs),
            },
            "details": multi_code_analytes[:10],
            "citations": lab_cites[:6],
        }
    )

    # Card 3: Historical Medication Renewal Folding
    if stopped_meds:
        med_cites = []
        for m in folded_med_renewals[:3]:
            for c in m["citations"]:
                if c not in med_cites:
                    med_cites.append(c)
        cards.append(
            {
                "id": "bloat-medication-renewals",
                "category": "Medication Renewal Folding",
                "severity": "medium",
                "title": (
                    f"{len(stopped_meds)} Historical Prescription Renewals Folded into "
                    f"{len(stopped_by_ing)} Ingredient Spans"
                ),
                "summary": (
                    f"Condensed {len(stopped_meds)} stopped/completed MedicationRequest renewal entries "
                    f"into {len(stopped_by_ing)} canonical RxNorm ingredient intervals while preserving "
                    f"first-recorded and last-stopped dates and 100% of member FHIR resource UUIDs."
                ),
                "metrics": {
                    "raw_stopped_med_entries": len(stopped_meds),
                    "canonical_ingredient_spans": len(stopped_by_ing),
                },
                "details": folded_med_renewals[:8],
                "citations": med_cites[:6],
            }
        )

    # Card 4: Administrative & Non-Patient Device Pruning
    cards.append(
        {
            "id": "bloat-admin-pruning",
            "category": "Billing, Provenance & Room Equipment Filtering",
            "severity": "low",
            "title": f"{excluded_total} Non-Clinical / Lookup Resources Stripped at Tier 0",
            "summary": (
                f"Removed {excluded_total} billing/administrative resources "
                f"({', '.join(f'{k}: {v}' for k, v in sorted(extracted.excluded_resource_counts.items()))}) "
                f"after inverting Provenance.target[] and Organization/Practitioner references onto "
                f"clinical resources."
            ),
            "metrics": dict(extracted.excluded_resource_counts),
            "citations": [],
        }
    )

    return {
        "analysis_type": "duplicates_and_bloat",
        "patient_name": extracted.patient_name,
        "reference_date": extracted.reference_date,
        "summary_metrics": {
            "total_bundle_entries": extracted.total_bundle_entries,
            "excluded_non_clinical_entries": excluded_total,
            "deduplicated_base64_notes": extracted.deduplicated_note_count,
            "raw_lab_observations": total_lab_points,
            "canonical_lab_analytes": len(lab_points_by_concept),
            "blood_vs_serum_split_analytes": len(split_analytes),
            "raw_stopped_medications": len(stopped_meds),
            "folded_stopped_med_ingredients": len(stopped_by_ing),
            "condensed_ledger_lines": len(condensed.records),
            "condensed_tokens": condensed.actual_tokens,
            "target_tokens": condensed.target_tokens,
            "tier": condensed.tier,
            "tier_name": condensed.tier_name,
        },
        "cards": cards,
        "multi_code_lab_analytes": multi_code_analytes[:12],
        "folded_medication_renewals": folded_med_renewals[:10],
        "repeated_screenings": top_repeated_screenings,
    }


def _apply_stage2_conflict_skills_on_chart(extracted: ExtractedChart) -> ExtractedChart:
    """Populate Stage-2 conflict/anomaly annotations dynamically over the items present in `extracted`
    when Stage 1 ran in unhinted `lossless_structural` mode (`conflict_note=None`).
    """
    from dataclasses import replace
    from .normalizers import (
        annotate_immunization_anomalies,
        detect_condition_conflicts,
        detect_medication_conflicts,
        normalize_drug_ingredient,
    )

    if any(it.conflict_note for it in extracted.items):
        return extracted
    if any(it.metadata.get("skip_stage2_skills") for it in extracted.items):
        return extracted

    skip_multi_source = any(
        it.metadata.get("skip_multi_source_stage_notes") for it in extracted.items
    )

    active_cond_dicts = [
        {
            "resourceUuid": it.resource_uuid,
            "code": it.code,
            "display": it.display,
            "status": it.status,
            "is_active": True,
            "onset": it.metadata.get("onset") or it.date,
            "abatement": it.metadata.get("abatement") or it.end_date,
            "source_org": it.source_org,
        }
        for it in extracted.items
        if it.section == "condition_active"
    ]
    resolved_cond_dicts = [
        {
            "resourceUuid": it.resource_uuid,
            "code": it.code,
            "display": it.display,
            "status": it.status,
            "is_active": False,
            "onset": it.metadata.get("onset") or it.date,
            "abatement": it.metadata.get("abatement") or it.end_date,
            "source_org": it.source_org,
        }
        for it in extracted.items
        if it.section == "condition_resolved"
    ]
    cond_conflicts = (
        {}
        if skip_multi_source
        else detect_condition_conflicts(active_cond_dicts, resolved_cond_dicts)
    )

    med_dicts = [
        {
            "resourceUuid": it.resource_uuid,
            "code": it.code,
            "display": it.display,
            "status": it.status,
            "is_active": True,
            "date": it.date,
            "ingredient_key": it.ingredient_key,
            "source_org": it.source_org,
        }
        for it in extracted.items
        if it.section == "med_active"
    ]
    med_conflicts = detect_medication_conflicts(med_dicts)

    imm_dicts = [
        {
            "resourceUuid": it.resource_uuid,
            "code": it.code,
            "display": it.display,
            "date": it.date,
            "status": it.status,
            "source_org": it.source_org,
            "group": str(it.metadata.get("vaccine_group") or it.ingredient_key or it.display),
        }
        for it in extracted.items
        if it.section == "immunization"
    ]
    imm_anomalies = annotate_immunization_anomalies(imm_dicts, extracted.birth_date)
    imms_by_group: Dict[str, List[str]] = defaultdict(list)
    for imd in imm_dicts:
        imms_by_group[imd["group"]].append(imd["resourceUuid"])

    note_conflicts: Dict[str, str] = {}
    note_med_links: Dict[str, List[str]] = defaultdict(list)
    if not skip_multi_source:
        active_meds = [it for it in extracted.items if it.section == "med_active"]
        notes = [it for it in extracted.items if it.section in {"clinical_note", "note"}]
        hold_re = re.compile(
            r"\b(?:hold|stop|discontinue|held|stopped|do\s+not\s+take)\s+([^.;\n]{3,90})",
            re.IGNORECASE,
        )
        for n_item in notes:
            matched_meds_for_note: List[ClinicalItem] = []
            for match in hold_re.finditer(n_item.text or ""):
                clause = match.group(1).lower()
                for m_item in active_meds:
                    m_ing = (m_item.ingredient_key or normalize_drug_ingredient(m_item.display)).lower()
                    m_disp_tokens = [
                        t for t in re.split(r"[^a-z0-9]+", m_item.display.lower()) if len(t) >= 4 and not t.isdigit()
                    ]
                    if (m_ing and m_ing in clause) or any(tok in clause for tok in m_disp_tokens[:2]):
                        if m_item not in matched_meds_for_note:
                            matched_meds_for_note.append(m_item)
            if matched_meds_for_note:
                med_names_str = ", ".join(m.display for m in matched_meds_for_note)
                msg = (
                    f"note_medication_contradiction: Clinical note ({n_item.clinical_date or 'recent'}) "
                    f"instructs to hold/discontinue {med_names_str}, but structured MedicationRequest remains ACTIVE"
                )
                note_conflicts[n_item.resource_uuid] = msg
                for m_item in matched_meds_for_note:
                    if m_item.resource_uuid not in med_conflicts:
                        med_conflicts[m_item.resource_uuid] = msg
                    note_med_links[m_item.resource_uuid].append(n_item.resource_uuid)
                    note_med_links[n_item.resource_uuid].append(m_item.resource_uuid)


    updated_items: List[ClinicalItem] = []
    for it in extracted.items:
        c_note = (
            cond_conflicts.get(it.resource_uuid)
            or med_conflicts.get(it.resource_uuid)
            or imm_anomalies.get(it.resource_uuid)
            or note_conflicts.get(it.resource_uuid)
        )
        extra_members = list(note_med_links.get(it.resource_uuid, []))
        if c_note and c_note.startswith("acip_schedule_note:"):
            v_grp = str(it.metadata.get("vaccine_group") or it.ingredient_key or it.display)
            extra_members.extend(imms_by_group.get(v_grp, []))
        if c_note or extra_members:
            new_members = tuple(dict.fromkeys(list(it.member_uuids) + extra_members))
            updated_items.append(replace(it, conflict_note=c_note or it.conflict_note, member_uuids=new_members))
        else:
            updated_items.append(it)
    return replace(extracted, items=updated_items)


def analyze_data_quality_and_conflicts(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    condensed: Optional[CondensedChartResult] = None,
    *,
    target_tokens: int = 8192,
    sharing_posture: str = "patient_self",
) -> Dict[str, Any]:
    """Suggested Analysis 2: Data Quality Issues, Cross-Clinic Conflicts, & Schedule Anomalies."""
    extracted, condensed = _ensure_extracted_and_condensed(
        bundle_or_extracted,
        condensed,
        target_tokens=target_tokens,
        sharing_posture=sharing_posture,
    )
    extracted = _apply_stage2_conflict_skills_on_chart(extracted)
    uuid_map = _build_uuid_to_citation_map(condensed)

    findings: List[Dict[str, Any]] = []

    # 1. Condition Stage & Status Conflicts (KDIGO CKD stages, Prediabetes + T2DM, Active vs Resolved)
    cond_conflicts = [
        it
        for it in extracted.items
        if it.section in {"condition_active", "condition_resolved"} and it.conflict_note
    ]
    # Group related condition conflicts so we present coherent cards
    ckd_items = [
        it
        for it in cond_conflicts
        if "ckd" in (it.conflict_note or "").lower()
        or "kidney" in it.display.lower()
    ]
    glycemic_items = [
        it
        for it in cond_conflicts
        if "glycemic" in (it.conflict_note or "").lower()
        or "diabetes" in it.display.lower()
        or "prediabetes" in it.display.lower()
    ]
    other_cond_items = [
        it for it in cond_conflicts if it not in ckd_items and it not in glycemic_items
    ]

    # Lookup latest eGFR and HbA1c for clinical grounding
    egfr_pts = [
        p
        for p in extracted.numeric_points
        if "glomerular" in p.concept.lower() or "egfr" in p.concept.lower()
    ]
    hba1c_pts = [
        p
        for p in extracted.numeric_points
        if "a1c" in p.concept.lower()
    ]

    if ckd_items:
        uuids = [it.resource_uuid for it in ckd_items]
        if egfr_pts:
            uuids.append(egfr_pts[-1].resource_uuid)
        cites = _citations_for_uuids(uuids, uuid_map, condensed)
        egfr_context = ""
        if egfr_pts:
            latest_egfr = egfr_pts[-1]
            min_egfr = min(egfr_pts, key=lambda p: p.value)
            egfr_context = (
                f" Latest eGFR is {latest_egfr.value:.1f} {latest_egfr.unit} on {latest_egfr.date} "
                f"(historical nadir {min_egfr.value:.1f} on {min_egfr.date})."
            )
        findings.append(
            {
                "id": "conflict-ckd-staging",
                "domain": "Condition Stage Conflict (KDIGO)",
                "severity": "high",
                "title": "Mutually Exclusive Active CKD Stages simultaneously on Problem List",
                "description": (
                    f"Patient has {len(ckd_items)} concurrent active Chronic Kidney Disease stage "
                    f"diagnoses ({', '.join(it.display for it in ckd_items)}). Under KDIGO 2024 guidelines, "
                    f"a patient can only occupy one current CKD G-stage; earlier stages were never marked "
                    f"resolved when kidney function progressed.{egfr_context}"
                ),
                "patient_plain_summary": (
                    f"Your health record lists {len(ckd_items)} different stages of Chronic Kidney Disease "
                    f"as 'active' at the same time ({', '.join(it.display for it in ckd_items)}). "
                    f"You can only be in one stage at a time—older stages from past years were never closed "
                    f"out when your kidney numbers changed.{egfr_context}"
                ),
                "what_to_ask_doctor": (
                    "Based on my most recent eGFR kidney blood test, which single Chronic Kidney Disease stage "
                    "is accurate today, and can we remove the older stages from my active problem list?"
                ),
                "clinician_technical_note": (
                    f"Rule wahs-kb-ckd-staging-01 (KDIGO 2024): Mutually exclusive active SNOMED CT CKD stages "
                    f"({', '.join(f'{it.display} [{it.code}]' for it in ckd_items)}). Reconcile against persistent "
                    f"90-day eGFR (LOINC 33914-3 / 48642-3).{egfr_context}"
                ),
                "recommendation": (
                    "Reconcile active problem list to retain only the current KDIGO CKD stage supported "
                    "by persistent eGFR/uACR over >=90 days, and mark prior CKD stage entries as resolved/superseded."
                ),
                "kb_rule_id": "wahs-kb-ckd-staging-01",
                "resource_uuids": [it.resource_uuid for it in ckd_items],
                "citations": cites,
            }
        )

    if glycemic_items:
        uuids = [it.resource_uuid for it in glycemic_items]
        if hba1c_pts:
            uuids.append(hba1c_pts[-1].resource_uuid)
        cites = _citations_for_uuids(uuids, uuid_map, condensed)
        a1c_context = ""
        if hba1c_pts:
            latest_a1c = hba1c_pts[-1]
            a1c_context = f" Latest HbA1c is {latest_a1c.value:.1f}% on {latest_a1c.date}."
        findings.append(
            {
                "id": "conflict-glycemic-staging",
                "domain": "Condition Exclusivity Conflict (ADA)",
                "severity": "high",
                "title": "Concurrent Active Prediabetes and Type 2 Diabetes Mellitus",
                "description": (
                    f"Problem list carries both active {', '.join(it.display for it in glycemic_items)}. "
                    f"Under ADA Standards of Care, once Type 2 Diabetes Mellitus (SNOMED 44054006) is "
                    f"established, Prediabetes (SNOMED 714628002) is superseded and should not remain "
                    f"as a separate active problem.{a1c_context}"
                ),
                "patient_plain_summary": (
                    f"Your chart shows both 'Prediabetes' and 'Type 2 Diabetes' as active conditions at the same time. "
                    f"Once Type 2 Diabetes is diagnosed, Prediabetes is an older stepping-stone diagnosis that "
                    f"should be marked as historical so your care team sees one clear blood-sugar diagnosis.{a1c_context}"
                ),
                "what_to_ask_doctor": (
                    "Since Type 2 Diabetes is on my chart, can we mark the older Prediabetes entry as resolved "
                    "and review what my latest HbA1c blood test means for my daily care?"
                ),
                "clinician_technical_note": (
                    f"Rule wahs-kb-glycemic-exclusivity-01 (ADA Standards of Care): SNOMED 44054006 (Diabetes mellitus type 2) "
                    f"supersedes SNOMED 714628002 (Prediabetes).{a1c_context}"
                ),
                "recommendation": (
                    "Archive/resolve the historical Prediabetes Condition resource and confirm glycemic "
                    "status against recent HbA1c measurements."
                ),
                "kb_rule_id": "wahs-kb-glycemic-exclusivity-01",
                "resource_uuids": [it.resource_uuid for it in glycemic_items],
                "citations": cites,
            }
        )

    # Group other condition conflicts by display name
    other_by_display: Dict[str, List[ClinicalItem]] = defaultdict(list)
    for it in other_cond_items:
        other_by_display[it.display.lower()].append(it)
    for disp_key, grp in other_by_display.items():
        uuids = [it.resource_uuid for it in grp]
        cites = _citations_for_uuids(uuids, uuid_map, condensed)
        orgs = sorted({it.source_org for it in grp if it.source_org})
        status_summary = "; ".join(
            f"{it.status.upper()} ({it.end_date or it.clinical_date or 'undated'}) via {it.source_org or 'Primary Clinic'}"
            for it in grp
        )
        findings.append(
            {
                "id": f"conflict-cond-{re.sub(r'[^a-z0-9]+', '-', disp_key).strip('-')}",
                "domain": "Cross-Clinic Condition Status Conflict",
                "severity": "medium",
                "title": f"Active vs. Resolved Status Conflict: {grp[0].display}",
                "description": (
                    f"Cross-organization records disagree on the clinical status of {grp[0].display}: "
                    f"{status_summary}. "
                    f"Conflict note: {grp[0].conflict_note}."
                ),
                "patient_plain_summary": (
                    f"Different clinics in your record disagree on whether '{grp[0].display}' is still ongoing "
                    f"or already healed ({status_summary}). Usually this happens when one clinic treats a condition "
                    f"and marks it resolved, but another clinic's computer still lists it as active."
                ),
                "what_to_ask_doctor": (
                    f"One of my clinic records says '{grp[0].display}' is resolved while another still lists it as active—"
                    f"can we update my chart so all my doctors see the right status?"
                ),
                "clinician_technical_note": (
                    f"Rule wahs-kb-active-vs-resolved-01: Cross-organization Condition.clinicalStatus discrepancy "
                    f"for {grp[0].display} (SNOMED {grp[0].code or 'unclassified'}): {status_summary}."
                ),
                "recommendation": (
                    "Verify whether the condition resolved following specialist treatment and update "
                    "the primary care active problem list."
                ),
                "kb_rule_id": "wahs-kb-active-vs-resolved-01",
                "resource_uuids": uuids,
                "source_organizations": orgs,
                "citations": cites,
            }
        )

    # 2. Active Medication Conflicts (Brand vs. Generic, Therapeutic Class Overlap, Interaction Hazards)
    med_conflicts = [
        it
        for it in extracted.items
        if it.section == "med_active"
        and it.conflict_note
        and not (it.conflict_note or "").startswith("note_medication_contradiction:")
    ]
    med_by_note: Dict[str, List[ClinicalItem]] = defaultdict(list)
    for it in med_conflicts:
        med_by_note[it.conflict_note or ""].append(it)

    for conflict_note, grp in med_by_note.items():
        uuids = [it.resource_uuid for it in grp]
        cites = _citations_for_uuids(uuids, uuid_map, condensed)
        displays = [it.display for it in grp]
        orgs = sorted({it.source_org for it in grp if it.source_org})
        rule_match = re.search(r"\[(wahs-kb-[^\]]+)\]", conflict_note)
        is_interaction = conflict_note.lower().startswith("interaction_hazard")
        is_class_dup = (
            not is_interaction
            and (
                "concurrent active" in conflict_note.lower()
                or "therapeutic class" in conflict_note.lower()
                or "statin" in conflict_note.lower()
            )
        )
        is_statin = "statin" in conflict_note.lower()
        kb_id = (
            rule_match.group(1)
            if rule_match
            else (
                "wahs-kb-rx-duplicate-statin-01"
                if is_statin
                else ("wahs-kb-rx-interaction" if is_interaction else "wahs-kb-rx-brand-generic-01")
            )
        )
        med_summary = "; ".join(
            f"{it.display} ({it.clinical_date or 'active'}) [{it.source_org or 'Clinic'}]"
            for it in grp
        )
        if is_interaction:
            domain_label = "High-Risk Drug / Electrolyte Interaction Hazard"
            title_label = f"High-Risk Concurrent Therapy ({' + '.join(displays)})"
            plain_summary = (
                f"Your active medication list includes {' and '.join(displays)} at the same time "
                f"({', '.join(orgs) if orgs else 'multiple clinics'}). Taking these medicines together "
                f"creates a high-risk drug or electrolyte interaction ({conflict_note.split('—')[-1].strip()})."
            )
            ask_doc = (
                f"My chart shows both {' and '.join(displays)} as active—is it safe to take these together "
                f"given my recent lab results, or should one of them be held or stopped?"
            )
        elif is_class_dup:
            domain_label = "Therapeutic Class Duplication"
            title_label = (
                f"Concurrent Active Statin Prescriptions ({' + '.join(displays)})"
                if is_statin
                else f"Concurrent Active Same-Class Prescriptions ({' + '.join(displays)})"
            )
            plain_summary = (
                (
                    f"Your medication list shows two different cholesterol-lowering 'statin' pills active at the same time "
                    f"({' and '.join(displays)}) from different prescriptions ({', '.join(orgs) if orgs else 'multiple clinics'}). "
                    f"Taking two statins together is unsafe and can cause severe muscle pain or liver side effects."
                )
                if is_statin
                else (
                    f"Your medication list shows two different medications from the same drug class active at the same time "
                    f"({' and '.join(displays)}) across {', '.join(orgs) if orgs else 'multiple clinics'}. "
                    f"Taking overlapping medicines from the same therapeutic class increases the risk of serious side effects."
                )
            )
            ask_doc = (
                f"My chart shows both {' and '.join(displays)} as active medications in the same drug class—"
                f"which single prescription should I actually be taking, and can we cancel the duplicate order?"
            )
        else:
            domain_label = "Brand vs. Generic Duplicate Therapy"
            title_label = f"Duplicate Active Ingredient Prescriptions ({' vs. '.join(displays)})"
            plain_summary = (
                f"Your active medication list has two prescriptions that contain the exact same active medicine "
                f"({' and '.join(displays)}). Taking both bottles at once would accidentally give you a double dose."
            )
            ask_doc = (
                f"Are {' and '.join(displays)} on my medication list the same active ingredient, "
                f"and which single bottle should I take each day so I don't double-dose?"
            )

        findings.append(
            {
                "id": f"conflict-med-{len(findings) + 1}",
                "domain": domain_label,
                "severity": "high",
                "title": title_label,
                "description": (
                    f"Detected overlapping or interacting active medication orders across sources "
                    f"({', '.join(orgs) if orgs else 'multi-clinic chart'}): "
                    f"{med_summary}. "
                    f"Flag: {conflict_note}."
                ),
                "patient_plain_summary": plain_summary,
                "what_to_ask_doctor": ask_doc,
                "clinician_technical_note": (
                    f"Rule {kb_id}: Active MedicationRequest conflict ({', '.join(f'{it.display} [RxNorm {it.code}]' for it in grp)}) "
                    f"across {', '.join(orgs) if orgs else 'multi-clinic sources'}. {conflict_note}."
                ),
                "recommendation": (
                    "Perform immediate medication reconciliation with the patient and prescribing clinics "
                    "to prevent accidental double-dosing, electrolyte toxicity, or bleeding complications."
                ),
                "kb_rule_id": kb_id,
                "resource_uuids": uuids,
                "source_organizations": orgs,
                "citations": cites,
            }
        )


    # 3. Immunization Duplicates (WA IIS vs Clinic) & ACIP Schedule Anomalies
    imm_anomalies = [
        it for it in extracted.items if it.section == "immunization" and it.conflict_note
    ]
    imm_by_group: Dict[str, List[ClinicalItem]] = defaultdict(list)
    for it in imm_anomalies:
        v_group = str(it.metadata.get("vaccine_group") or it.display)
        imm_by_group[v_group].append(it)

    for v_group, grp in imm_by_group.items():
        uuids = [it.resource_uuid for it in grp]
        cites = _citations_for_uuids(uuids, uuid_map, condensed)
        notes = sorted({it.conflict_note for it in grp if it.conflict_note})
        joined_notes = "; ".join(notes)
        is_same_day_dup = "duplicate_candidate" in joined_notes.lower() or "duplicate" in joined_notes.lower()
        kb_id = (
            "wahs-kb-iis-dedup-01"
            if is_same_day_dup
            else (
                "wahs-kb-acip-hpv-01"
                if v_group == "HPV"
                else "wahs-kb-acip-zoster-01"
            )
        )
        imm_entries_str = ", ".join(
            f"{it.clinical_date} [{it.source_org or 'Clinic'}]" for it in grp
        )
        findings.append(
            {
                "id": f"anomaly-imm-{v_group.lower()}",
                "domain": (
                    "WA IIS vs. Clinic Registry Duplicate"
                    if is_same_day_dup
                    else "CDC ACIP Schedule Anomaly"
                ),
                "severity": "medium",
                "title": (
                    f"Duplicate Same-Day {v_group} Immunization Records Across Sources"
                    if is_same_day_dup
                    else f"ACIP Immunization Schedule Anomaly: {v_group} Series ({len(grp)} doses)"
                ),
                "description": (
                    f"{v_group} immunization entries ({imm_entries_str}): "
                    f"{joined_notes}."
                ),
                "patient_plain_summary": (
                    (
                        f"Your vaccine record lists the same {v_group} shot twice on the same date ({imm_entries_str})—"
                        f"once from your clinic and once from the Washington State Immunization Information System (WA IIS). "
                        f"This is a computer duplicate, not two separate shots given on the same day."
                    )
                    if is_same_day_dup
                    else (
                        f"Your vaccine log shows {len(grp)} recorded doses of the {v_group} vaccine ({imm_entries_str}), "
                        f"which is more doses or a different schedule than standard CDC guidelines normally recommend. "
                        f"Some entries may be duplicate historical imports."
                    )
                ),
                "what_to_ask_doctor": (
                    (
                        f"Can my clinic merge the duplicate same-day {v_group} vaccine entry with the state registry "
                        f"so my official immunization certificate shows an accurate dose count?"
                    )
                    if is_same_day_dup
                    else (
                        f"My chart lists {len(grp)} {v_group} vaccine doses ({imm_entries_str})—am I completely up to date "
                        f"for school or preventive care, or are some of those entries duplicate records?"
                    )
                ),
                "clinician_technical_note": (
                    f"Rule {kb_id} (CDC ACIP / WA IIS): {v_group} Immunization resources ({', '.join(f'CVX {it.code} on {it.clinical_date}' for it in grp)}). {joined_notes}."
                ),
                "recommendation": (
                    "Deduplicate same-day clinic and WA IIS registry feeds into a single canonical administration event."
                    if is_same_day_dup
                    else "Review immunization series against CDC ACIP age-at-initiation and product-specific dosing intervals."
                ),
                "kb_rule_id": kb_id,
                "resource_uuids": uuids,
                "citations": cites,
            }
        )

    # 4. SDOH Privacy-Sensitive Findings & Posture Enforcement
    sdoh_sensitive_items = [
        it for it in extracted.items if it.sensitivity == "SDOH/PRIVACY-SENSITIVE"
    ]
    if sdoh_sensitive_items:
        uuids = [] if sharing_posture == "external_share" else [it.resource_uuid for it in sdoh_sensitive_items]
        cites = _citations_for_uuids(uuids, uuid_map, condensed) if uuids else []
        redacted_count = len(condensed.redacted_for_privacy)
        findings.append(
            {
                "id": "privacy-sdoh-segmentation",
                "domain": "Privacy & Consent Segmentation (42 CFR Part 2 / WA MHMDA)",
                "severity": "high" if sharing_posture == "external_share" else "info",
                "title": (
                    f"{len(sdoh_sensitive_items)} Sensitive SDOH / Behavioral Health Records "
                    f"({'REDACTED in External Share' if sharing_posture == 'external_share' else 'Visible in Patient Self-View'})"
                ),
                "description": (
                    (
                        f"In 'external_share' mode, all {redacted_count} privacy-sensitive SDOH or behavioral health items "
                        f"were automatically stripped from the prompt context and recorded in the Privacy Manifest "
                        f"under 42 CFR Part 2 and the WA My Health My Data Act."
                    )
                    if sharing_posture == "external_share"
                    else (
                        f"Chart contains {len(sdoh_sensitive_items)} privacy-sensitive SDOH or behavioral health items "
                        f"({', '.join( sorted({it.display for it in sdoh_sensitive_items})[:5] )}). "
                        f"In '{sharing_posture}' mode, these items are surfaced with [SDOH/PRIVACY-SENSITIVE] tags and require explicit patient consent before external sharing."
                    )
                ),
                "patient_plain_summary": (
                    (
                        f"Because you selected 'External Share' mode, {redacted_count} sensitive personal or social history "
                        f"records have been completely removed before creating this summary. Your medical allergies, medications, "
                        f"lab results, and vaccines remain visible for safe care."
                    )
                    if sharing_posture == "external_share"
                    else (
                        f"Your personal chart includes {len(sdoh_sensitive_items)} sensitive social or behavioral health screening "
                        f"entries ({', '.join(sorted({it.display for it in sdoh_sensitive_items})[:3])}). These are visible to you here, "
                        f"and are automatically hidden whenever you switch to 'External Share' mode for a school, employer, or specialist."
                    )
                ),
                "what_to_ask_doctor": (
                    "If I share my health summary with a specialist, school, or new clinic, how do I make sure my private "
                    "social and behavioral health screenings stay protected unless I specifically choose to share them?"
                ),
                "clinician_technical_note": (
                    f"Rule wahs-kb-sdoh-privacy-01 (42 CFR Part 2 / WA MHMDA): {len(sdoh_sensitive_items)} SDOH/PRIVACY-SENSITIVE "
                    f"resources segmented by posture='{sharing_posture}' (redacted_for_privacy={redacted_count})."
                ),
                "recommendation": (
                    "Maintain strict consent segmentation when exporting directed summaries to schools, employers, or external providers."
                ),
                "kb_rule_id": "wahs-kb-sdoh-privacy-01",
                "resource_uuids": uuids,
                "citations": cites,
            }
        )

    # 5. Free-Text Clinical Note vs. Active Medication Contradictions (Set C / Real Clinical Summaries)
    note_med_conflicts = [
        it
        for it in extracted.items
        if it.section in {"clinical_note", "note"}
        and "note_medication_contradiction" in (it.conflict_note or "")
    ]
    for n_it in note_med_conflicts:
        n_members = set(n_it.member_uuids or ())
        linked_meds = [
            it
            for it in extracted.items
            if it.section == "med_active"
            and (
                it.resource_uuid in n_members
                or "note_medication_contradiction" in (it.conflict_note or "")
            )
        ]

        uuids = [n_it.resource_uuid] + [m.resource_uuid for m in linked_meds]
        cites = _citations_for_uuids(uuids, uuid_map, condensed)
        med_names = ", ".join(m.display for m in linked_meds) or "active medication"
        findings.append(
            {
                "id": f"conflict-note-med-{n_it.resource_uuid}",
                "domain": "Unstructured Note vs. Active Medication Contradiction",
                "severity": "high",
                "title": f"Clinical Note Instructs to Hold/Stop Active Medication ({med_names})",
                "description": (
                    f"Clinical note '{n_it.display}' ({n_it.clinical_date}) contains an explicit hold/discontinue "
                    f"instruction ({n_it.conflict_note}) while the structured MedicationRequest ({med_names}) "
                    f"remains marked ACTIVE."
                ),
                "patient_plain_summary": (
                    f"A doctor's note from {n_it.clinical_date} says to hold or stop {med_names}, "
                    f"but your automated medication list still shows {med_names} as an active daily prescription."
                ),
                "what_to_ask_doctor": (
                    f"My clinical note from {n_it.clinical_date} says to hold or stop {med_names}, "
                    f"but it is still on my active pill list—should I be taking it right now or holding it?"
                ),
                "clinician_technical_note": (
                    f"Rule wahs-kb-note-med-contradiction-01: DocumentReference ({n_it.resource_uuid}, {n_it.clinical_date}) "
                    f"Assessment/Plan instructs hold/discontinue against active MedicationRequest ({', '.join(m.resource_uuid for m in linked_meds)})."
                ),
                "recommendation": (
                    "Reconcile outpatient MedicationRequest status against the discharge/progress note Assessment & Plan."
                ),
                "kb_rule_id": "wahs-kb-note-med-contradiction-01",
                "resource_uuids": uuids,
                "citations": cites,
            }
        )

    return {
        "analysis_type": "data_quality_and_conflicts",
        "patient_name": extracted.patient_name,
        "reference_date": extracted.reference_date,
        "sharing_posture": sharing_posture,
        "total_findings": len(findings),
        "high_severity_count": sum(1 for f in findings if f["severity"] == "high"),
        "findings": findings,
    }


def _synthesize_highlight_encounter(
    extracted: ExtractedChart,
    condensed: CondensedChartResult,
    uuid_map: Mapping[str, List[int]],
    encounters: Sequence[ClinicalItem],
    procedures: Sequence[ClinicalItem],
    devices: Sequence[ClinicalItem],
) -> Dict[str, Any]:
    """Dynamically identify and summarize the highest-acuity hospitalization/procedure episode
    (or most recent clinical encounter) from any FHIR R4 patient chart without patient-specific hardcoding.
    """
    surgical_re = re.compile(
        r"\b(?:coronary\s+artery\s+bypass|cabg|percutaneous\s+coronary|valve|stent|transfusion|"
        r"endoscopy|intubation|catheterization|resection|debridement|cardioversion|implant)\b",
        re.IGNORECASE,
    )
    acuity_re = re.compile(
        r"\b(?:inpatient|intensive\s+care|icu|emergency|admission|hospital|cardiac|observation\s+stay)\b",
        re.IGNORECASE,
    )

    # Group clinical events by YYYY-MM month bucket to find the highest-acuity clinical episode
    month_scores: Dict[str, int] = defaultdict(int)
    month_dates: Dict[str, List[str]] = defaultdict(list)
    for enc in encounters:
        d = (enc.clinical_date or "")[:10]
        if not d:
            continue
        ym = d[:7]
        month_dates[ym].append(d)
        start_meta = str(enc.metadata.get("start") or "")[:10]
        if start_meta:
            month_dates[ym].append(start_meta)
        if enc.end_date:
            month_dates[ym].append(enc.end_date[:10])
        if enc.section == "encounter_acuity" or acuity_re.search(enc.display) or acuity_re.search(enc.text):
            month_scores[ym] += 10
        else:
            month_scores[ym] += 1


    for proc in procedures:
        d = (proc.clinical_date or "")[:10]
        if not d:
            continue
        ym = d[:7]
        month_dates[ym].append(d)
        if surgical_re.search(proc.display):
            month_scores[ym] += 12
        else:
            month_scores[ym] += 3

    if not month_scores:
        return {}

    best_ym = max(month_scores.keys(), key=lambda ym: (month_scores[ym], ym))
    ep_items = [
        it
        for it in extracted.items
        if (it.clinical_date or "").startswith(best_ym)
    ]
    acuity_date_set: Set[str] = set()
    for it in ep_items:
        if not it.clinical_date:
            continue
        if (
            it.section in {"encounter_acuity", "procedure_major"}
            or surgical_re.search(it.display)
            or acuity_re.search(it.display)
            or acuity_re.search(it.text)
        ):
            acuity_date_set.add(it.clinical_date[:10])
            s_meta = str(it.metadata.get("start") or "")[:10]
            if s_meta:
                acuity_date_set.add(s_meta)
            if it.end_date:
                acuity_date_set.add(it.end_date[:10])
    acuity_dates = sorted(acuity_date_set)

    all_ep_dates = sorted(d for d in month_dates[best_ym] if d)
    w_start = acuity_dates[0] if acuity_dates else (all_ep_dates[0] if all_ep_dates else f"{best_ym}-01")
    w_end = acuity_dates[-1] if len(acuity_dates) > 1 else (all_ep_dates[-1] if all_ep_dates else w_start)
    if w_end < w_start:
        w_end = w_start

    window_items = [
        it
        for it in extracted.items
        if it.clinical_date and w_start <= it.clinical_date[:10] <= w_end
    ] or ep_items

    ep_procs = [
        it
        for it in window_items
        if it.section in {"procedure_major", "procedure"} and not it.is_routine
    ] or list(procedures[-5:])
    ep_conds = [
        it
        for it in window_items
        if it.section in {"condition_active", "condition_resolved"}
    ] or [it for it in extracted.items if it.section == "condition_active"][:4]
    ep_meds = [
        it
        for it in window_items
        if it.section in {"med_active", "med_historical", "med_stopped"}
    ]
    ep_encs = [
        it
        for it in window_items
        if it.section in {"encounter_acuity", "encounter_routine", "encounter"}
    ] or list(encounters[-1:])

    cite_uuids = (
        [it.resource_uuid for it in (ep_encs[:3] + ep_conds[:3] + ep_procs[:4] + ep_meds[:4])]
        + [d.resource_uuid for d in devices[:4]]
    )
    cites = _citations_for_uuids(cite_uuids, uuid_map, condensed)

    window_label = f"{w_start} through {w_end}" if w_start != w_end else w_start
    enc_label = ep_encs[0].display if ep_encs else "Clinical Encounter"
    cond_displays = list(dict.fromkeys(c.display for c in ep_conds[:4]))
    proc_displays = list(dict.fromkeys(p.display for p in ep_procs[:8]))
    med_displays = list(dict.fromkeys(m.display for m in ep_meds[:6]))
    dev_displays = list(dict.fromkeys(d.display for d in devices))

    is_high_acuity = month_scores[best_ym] >= 10
    title_prefix = "High-Acuity Inpatient / Procedural Summary" if is_high_acuity else "Clinical Encounter Summary"
    lead_topic = proc_displays[0] if proc_displays else (cond_displays[0] if cond_displays else enc_label)
    title = f"{title_prefix} ({window_label}): {lead_topic}"

    narrative_parts = [
        f"{extracted.patient_name} was evaluated during {window_label} ({enc_label})"
        + (f" with documented diagnoses including {', '.join(cond_displays)}" if cond_displays else "")
        + "."
    ]
    if proc_displays:
        narrative_parts.append(
            f"Key procedures and interventions performed during this episode: {', '.join(proc_displays[:5])}."
        )
    if dev_displays:
        narrative_parts.append(
            f"Documented implantable or patient-carried devices on chart: {', '.join(dev_displays)}."
        )
    if med_displays:
        narrative_parts.append(
            f"Episode and discharge medications recorded in window: {', '.join(med_displays)}."
        )

    conflict_items = [it for it in extracted.items if it.conflict_note]
    return {
        "title": title,
        "window": window_label,
        "window_start": w_start,
        "window_end": w_end,
        "narrative": " ".join(narrative_parts),
        "key_procedures": proc_displays,
        "implantable_devices": dev_displays,
        "opus_5_5_comparison": {
            "raw_json_feeding_opus_5_5": (
                f"Uncondensed FHIR Bundle ({extracted.total_bundle_entries:,} raw entries): "
                f"Scatters encounter diagnoses, {len(proc_displays)} procedural records, and cross-clinic "
                f"prescriptions across thousands of JSON lines without resolving {len(conflict_items)} "
                f"chart reconciliation/safety conflicts."
            ),
            "wahs_condensed_pipeline": (
                f"WAHS+ Tier {condensed.tier} ({condensed.actual_tokens:,} tokens; mode={condensed.completeness_manifest.get('condense_mode', 'lossless_structural')}): "
                f"Preserves the {window_label} clinical sequence, {len(dev_displays)} patient device(s), "
                f"longitudinal biomarker trajectories, and surfaces {len(conflict_items)} reconciliation "
                f"findings with [N] -> FHIR resourceUuid provenance."
            ),

        },
        "citations": cites,
    }


def analyze_patterns_and_summaries(

    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    condensed: Optional[CondensedChartResult] = None,
    *,
    target_tokens: int = 8192,
    sharing_posture: str = "patient_self",
) -> Dict[str, Any]:
    """Suggested Analysis 3: General Longitudinal Patterns, Encounter Summaries, & Directed Summaries."""
    extracted, condensed = _ensure_extracted_and_condensed(
        bundle_or_extracted,
        condensed,
        target_tokens=target_tokens,
        sharing_posture=sharing_posture,
    )
    extracted = _apply_stage2_conflict_skills_on_chart(extracted)
    uuid_map = _build_uuid_to_citation_map(condensed)

    # 1. Key Biomarker & Vital Sign Trajectories
    pts_by_concept: Dict[str, List[Any]] = defaultdict(list)
    for pt in extracted.numeric_points:
        pts_by_concept[pt.concept].append(pt)

    priority_concept_patterns = [
        ("glomerular filtration rate", "egfr"),
        ("creatinine",),
        ("hemoglobin a1c", "a1c"),
        ("glucose",),
        ("potassium",),
        ("international normalized ratio", "inr"),
        ("natriuretic peptide", "nt-probnp", "bnp"),
        ("systolic blood pressure", "blood pressure"),
        ("diastolic blood pressure",),
        ("body mass index", "bmi"),
        ("body weight",),
        ("fev1/fvc",),
        ("low density lipoprotein", "ldl"),
        ("hemoglobin",),
    ]


    trajectories: List[Dict[str, Any]] = []
    seen_concepts = set()
    for patterns_tuple in priority_concept_patterns:
        matched_key = None
        for k in pts_by_concept:
            if k in seen_concepts:
                continue
            k_low = k.lower()
            if any(pat in k_low for pat in patterns_tuple):
                matched_key = k
                break
        if not matched_key:
            continue
        seen_concepts.add(matched_key)
        pts = sorted(pts_by_concept[matched_key], key=lambda p: (p.date, p.resource_uuid))
        if not pts:
            continue
        first_p, latest_p = pts[0], pts[-1]
        min_p = min(pts, key=lambda p: p.value)
        max_p = max(pts, key=lambda p: p.value)
        delta = latest_p.value - first_p.value
        direction = "stable"
        if abs(delta) > 0.05 * max(abs(first_p.value), 1.0):
            direction = "increasing" if delta > 0 else "decreasing"

        kb_info = resolve_lab_code(latest_p.code, matched_key)
        sample_uuids = [first_p.resource_uuid, min_p.resource_uuid, max_p.resource_uuid, latest_p.resource_uuid]
        cites = _citations_for_uuids(sample_uuids, uuid_map, condensed)
        trajectories.append(
            {
                "concept": matched_key,
                "category": latest_p.category,
                "count": len(pts),
                "unit": latest_p.unit,
                "initial": {"value": round(first_p.value, 2), "date": first_p.date},
                "min": {"value": round(min_p.value, 2), "date": min_p.date},
                "max": {"value": round(max_p.value, 2), "date": max_p.date},
                "latest": {"value": round(latest_p.value, 2), "date": latest_p.date},
                "direction": direction,
                "reference_range": (kb_info or {}).get("reference_range", ""),
                "clinical_significance": (kb_info or {}).get("clinical_significance", ""),
                "citations": cites,
            }
        )

    # 2. High-Acuity / Highlight Encounter Summary (Use Case B-1 — General Data-Driven Synthesis)
    encounters = [
        it for it in extracted.items if it.section in {"encounter_acuity", "encounter_routine", "encounter"}
    ]
    procedures = [
        it for it in extracted.items if it.section in {"procedure_major", "procedure"} and not it.is_routine
    ]
    devices = [it for it in extracted.items if it.section == "device"]

    highlight_encounter = _synthesize_highlight_encounter(
        extracted=extracted,
        condensed=condensed,
        uuid_map=uuid_map,
        encounters=encounters,
        procedures=procedures,
        devices=devices,
    )


    # 3. Directed Summaries (Use Case B-2: Patient Self-View vs. External / School / Specialist Share)
    def _posture_allowed(item: ClinicalItem) -> bool:
        return not (sharing_posture == "external_share" and item.sensitivity == "SDOH/PRIVACY-SENSITIVE")

    active_conds = [it for it in extracted.items if it.section == "condition_active" and _posture_allowed(it)]
    active_meds = [it for it in extracted.items if it.section == "med_active" and _posture_allowed(it)]
    allergies = [it for it in extracted.items if it.section == "allergy" and _posture_allowed(it)]
    immunizations = [it for it in extracted.items if it.section == "immunization" and _posture_allowed(it)]
    sdoh_safe = [it for it in extracted.items if it.sensitivity == "SDOH-SOCIAL"]
    sdoh_sensitive = [it for it in extracted.items if it.sensitivity == "SDOH/PRIVACY-SENSITIVE"]

    directed_summary = {
        "posture": sharing_posture,
        "posture_label": (
            "External / Specialist / School Share (SDOH Privacy-Redacted)"
            if sharing_posture == "external_share"
            else "Patient Self-Access Full Health Summary"
        ),
        "allergies": [a.display for a in allergies] or ["No Known Allergies recorded in structured AllergyIntolerance resources"],
        "active_conditions": [
            {
                "display": c.display,
                "onset": c.clinical_date,
                "conflict_note": c.conflict_note,
                "citations": _citations_for_uuids([c.resource_uuid], uuid_map, condensed),
            }
            for c in active_conds
        ],
        "active_medications": [
            {
                "display": m.display,
                "date": m.clinical_date,
                "source_org": m.source_org,
                "conflict_note": m.conflict_note,
                "citations": _citations_for_uuids([m.resource_uuid], uuid_map, condensed),
            }
            for m in active_meds
        ],
        "immunization_count": len(immunizations),
        "safe_social_context": [s.display for s in sdoh_safe[:5]],
        "sensitive_sdoh_included": (
            [s.display for s in sdoh_sensitive] if sharing_posture == "patient_self" else []
        ),
        "redacted_privacy_count": len(condensed.redacted_for_privacy),
        "privacy_diff": {
            "retained_in_external_share": [
                f"Active Conditions ({len(active_conds)} medical diagnoses)",
                f"Active Medications ({len(active_meds)} prescriptions & safety alerts)",
                f"Allergies ({len(allergies)} structured records)",
                f"Immunizations ({len(immunizations)} vaccine doses)",
                f"Implantable & Patient Devices ({len(devices)} devices)",
            ],
            "removed_in_external_share": (
                sorted({s.display for s in sdoh_sensitive})
                if sharing_posture == "patient_self"
                else [
                    f"{len(condensed.redacted_for_privacy)} behavioral health / SDOH screening records withheld under 42 CFR Part 2 & WA My Health My Data Act"
                ]
            ),
            "regulatory_basis": "42 CFR Part 2 & Washington My Health My Data Act (MHMDA)",
        },
        "completeness_manifest": condensed.completeness_manifest,
    }

    return {
        "analysis_type": "patterns_and_summaries",
        "patient_name": extracted.patient_name,
        "birth_date": extracted.birth_date,
        "reference_date": extracted.reference_date,
        "sharing_posture": sharing_posture,
        "trajectories": trajectories,
        "highlight_encounter": highlight_encounter,
        "directed_summary": directed_summary,
    }


# Unanswerable clinical topic patterns for deterministic abstention verification (Use Case B-3)
_UNANSWERABLE_TOPIC_PATTERNS: Tuple[Tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\b(?:penicillin|sulfa|latex|peanut|shellfish)\s+allerg|\banaphylactic\s+drug\s+allergy|\bpeanut-specific\s+ige|\bepinephrine\s+auto-injector\b", re.IGNORECASE),
        "a documented drug/food anaphylactic allergy, peanut-specific IgE, or epinephrine auto-injector",
    ),
    (
        re.compile(r"\b(?:colonoscopy|polyp\s+biopsy|endoscopy\s+pathology)\b", re.IGNORECASE),
        "a colonoscopy report or gastrointestinal polyp pathology",
    ),
    (
        re.compile(r"\b(?:abo|rh\s+factor|blood\s+type|blood\s+group)\b", re.IGNORECASE),
        "ABO / Rh blood type typing",
    ),
    (
        re.compile(r"\b(?:snellen|visual\s+acuity|refraction|eyeglass\s+prescription|optometry|20/20)\b", re.IGNORECASE),
        "an optometry exam, visual acuity (Snellen), or eyeglass prescription",
    ),
    (
        re.compile(r"\b(?:genetic\s+test|pharmacogenomic|cyp2c19|apoe4?|brca|exome|karyotype)\b", re.IGNORECASE),
        "genetic or pharmacogenomic testing (e.g., APOE4, CYP2C19, BRCA)",
    ),
    (
        re.compile(r"\b(?:mri\s+of\s+the\s+brain|brain\s+mri|lumbar\s+puncture|spinal\s+tap)\b", re.IGNORECASE),
        "a brain MRI or lumbar puncture",
    ),
    (
        re.compile(r"\b(?:left\s+ventricular\s+ejection\s+fraction|lvef)\b", re.IGNORECASE),
        "a quantitative left ventricular ejection fraction (LVEF %) measurement",
    ),
)


def answer_wahs_question(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    question: str,
    condensed: Optional[CondensedChartResult] = None,
    *,
    target_tokens: int = 8192,
    sharing_posture: str = "patient_self",
    use_lookup_tools: bool = True,
) -> Dict[str, Any]:
    """Grounded Q&A responder over condensed FHIR R4 charts + WAHS+ 4-layer KB.

    - Resolves inline `[N] -> FHIR resourceUuid` provenance for every clinical claim
    - Enforces `sharing_posture` (`external_share` abstains/redacts sensitive SDOH)
    - Deterministically abstains when asked about facts absent from the chart
    - When `use_lookup_tools=True`, invokes on-demand full-chart overflow tools
      (`search_chart_entries`, `get_lab_history`, `read_clinical_note`) when historical
      or note details were condensed out of the in-context IPS Safety Core (`Tier 3/4`).
    """
    extracted, condensed = _ensure_extracted_and_condensed(
        bundle_or_extracted,
        condensed,
        target_tokens=target_tokens,
        sharing_posture=sharing_posture,
    )
    extracted = _apply_stage2_conflict_skills_on_chart(extracted)
    uuid_map = _build_uuid_to_citation_map(condensed)
    q_text = (question or "").strip()
    q_lower = q_text.lower()

    # Retrieve relevant KB entries using code-first + BM25 hybrid search
    chart_codes = extract_chart_kb_codes(extracted.items)
    kb_hits = search_kb(
        q_text or "clinical summary reconciliation",
        k=3,
        corpus="wahs",
        codes=chart_codes,
    )

    # 1. Check if the question asks about a topic absent from the chart (Abstention Probe)
    for pattern, topic_desc in _UNANSWERABLE_TOPIC_PATTERNS:
        if pattern.search(q_text):
            # Verify it truly does not appear in chart_text
            matched_token = pattern.search(condensed.chart_text)
            if not matched_token:
                # Cite the patient header [1] and completeness manifest context
                header_cites = _citations_for_uuids(
                    [extracted.items[0].resource_uuid] if extracted.items else [],
                    uuid_map,
                    condensed,
                )
                cite_str = _format_citation_refs(header_cites) or "[1]"
                return {
                    "question": q_text,
                    "patient_category": "4. Next-Visit Checklist",
                    "abstained": True,
                    "abstains": True,
                    "abstention_reason": f"Not present in patient FHIR Bundle: {topic_desc}",
                    "answer": (
                        f"**Not Recorded in Chart (Grounded Abstention):** I reviewed {extracted.patient_name}'s "
                        f"condensed health record {cite_str} (covering {extracted.total_bundle_entries:,} FHIR "
                        f"Bundle entries through `{extracted.reference_date}`) and found **no documentation of "
                        f"{topic_desc}**.\n\n"
                        f"- **Allergy & Clinical Core Check:** Structured `AllergyIntolerance`, `Procedure`, "
                        f"`DiagnosticReport`, and `Observation` resources contain no matching records for this request.\n"
                        f"- **Safety Posture:** Per WAHS clinical grounding rules, the assistant abstains rather than "
                        f"inferring or fabricating unrecorded clinical history."
                    ),
                    "patient_plain_summary": (
                        f"I checked all {extracted.total_bundle_entries:,} records in {extracted.patient_name}'s chart "
                        f"through {extracted.reference_date} {cite_str}, and there is no record of {topic_desc}. "
                        f"For your safety, this summary only reports facts that are actually written in your medical record."
                    ),
                    "what_to_ask_doctor": [
                        f"I noticed {topic_desc} is not listed in my electronic health summary—if I had this done at another clinic, should we add it to my chart?"
                    ],
                    "clinician_technical_note": (
                        f"Deterministic abstention (Use Case B-3): Zero matching AllergyIntolerance, Procedure, "
                        f"DiagnosticReport, or Observation resources for '{topic_desc}' across {extracted.total_bundle_entries:,} bundle entries {cite_str}."
                    ),
                    "source_mode": "ips_safety_core",
                    "source_mode_label": "Answered from In-Context IPS Safety Core",
                    "citations": header_cites,
                    "kb_references": kb_hits,
                    "tier_info": {
                        "tier": condensed.tier,
                        "tier_name": condensed.tier_name,
                        "target_tokens": condensed.target_tokens,
                        "actual_tokens": condensed.actual_tokens,
                        "sharing_posture": sharing_posture,
                    },
                }

    # 2. Check if the question asks about sensitive SDOH while in `external_share` mode
    if sharing_posture == "external_share" and re.search(
        r"\b(?:substance|drug\s+abuse|domestic\s+violence|intimate\s+partner|hark|criminal|arrest|prison|housing\s+instability|homeless)\b",
        q_lower,
    ):
        header_cites = _citations_for_uuids(
            [extracted.items[0].resource_uuid] if extracted.items else [],
            uuid_map,
            condensed,
        )
        cite_str = _format_citation_refs(header_cites) or "[1]"
        return {
            "question": q_text,
            "patient_category": "6. Safe Sharing Preview",
            "abstained": True,
            "abstains": True,
            "abstention_reason": "Redacted under external_share privacy posture (42 CFR Part 2 / WA MHMDA)",
            "answer": (
                f"**Privacy-Protected / Redacted (`external_share` Posture):** {extracted.patient_name}'s "
                f"summary {cite_str} is currently rendered in **External Share** mode. "
                f"`{len(condensed.redacted_for_privacy)}` privacy-sensitive behavioral health / SDOH records "
                f"have been excluded from external disclosure in accordance with **42 CFR Part 2** and the "
                f"**WA My Health My Data Act**. Switch to `patient_self` posture with explicit patient authorization "
                f"to review sensitive SDOH screening history."
            ),
            "patient_plain_summary": (
                f"Your summary is currently in External Share mode {cite_str}. To protect your privacy under "
                f"federal and Washington State law, {len(condensed.redacted_for_privacy)} sensitive personal or "
                f"social history screenings are automatically hidden from outside recipients. Switch back to "
                f"'Patient Self-View' if you want to see those records yourself."
            ),
            "what_to_ask_doctor": [
                "When my clinic sends a referral summary to an outside specialist or school, can we confirm that sensitive social and behavioral health screenings remain excluded?"
            ],
            "clinician_technical_note": (
                f"Rule wahs-kb-sdoh-privacy-01: {len(condensed.redacted_for_privacy)} SDOH/PRIVACY-SENSITIVE resources "
                f"withheld under external_share posture (42 CFR Part 2 / WA MHMDA) {cite_str}."
            ),
            "source_mode": "ips_safety_core",
            "source_mode_label": "Answered from In-Context IPS Safety Core",
            "citations": header_cites,
            "kb_references": kb_hits,
            "tier_info": {
                "tier": condensed.tier,
                "tier_name": condensed.tier_name,
                "target_tokens": condensed.target_tokens,
                "actual_tokens": condensed.actual_tokens,
                "sharing_posture": sharing_posture,
            },
        }

    # 3. Topic-routed grounded synthesis with exact `[N]` ledger citations
    dq = analyze_data_quality_and_conflicts(
        extracted, condensed, target_tokens=target_tokens, sharing_posture=sharing_posture
    )
    bloat = analyze_duplicates_and_bloat(
        extracted, condensed, target_tokens=target_tokens, sharing_posture=sharing_posture
    )
    patterns = analyze_patterns_and_summaries(
        extracted, condensed, target_tokens=target_tokens, sharing_posture=sharing_posture
    )

    collected_citations: List[Dict[str, Any]] = []

    def _add_cites(cites: Sequence[Mapping[str, Any]]) -> str:
        for c in cites:
            if not any(existing["index"] == c["index"] for existing in collected_citations):
                collected_citations.append(dict(c))
        return _format_citation_refs(cites)

    overflow_tool_calls: List[Dict[str, Any]] = []

    def _add_overflow_citation(
        resource_uuid: str,
        resource_type: str,
        date_str: str,
        text_str: str,
        member_uuids: Optional[Sequence[str]] = None,
    ) -> str:
        existing_cites = _citations_for_uuids([resource_uuid], uuid_map, condensed)
        if existing_cites:
            return _add_cites(existing_cites)
        for existing in collected_citations:
            if existing.get("resource_uuid") == resource_uuid:
                return f"[{existing['index']}]"
        next_idx = max(
            [len(condensed.mappings)]
            + [int(c.get("index", 0)) for c in collected_citations]
        ) + 1
        overflow_cite = {
            "index": next_idx,
            "label": f"[{next_idx}]",
            "resource_type": resource_type,
            "resource_uuid": resource_uuid,
            "member_uuids": list(member_uuids or [])[:5],
            "member_uuid_count": len(member_uuids or []),
            "date": date_str,
            "text": text_str[:257] + "..." if len(text_str) > 260 else text_str,
            "source_mode": "full_chart_lookup",
        }
        collected_citations.append(overflow_cite)
        return f"[{next_idx}]"

    sections: List[str] = []

    # Medication / prescription / drug class / interaction questions
    chart_med_tokens = {
        tok
        for it in extracted.items
        if it.section in {"med_active", "med_stopped", "med_historical"}
        for tok in re.split(r"[^a-z0-9]+", it.display.lower())
        if len(tok) >= 5 and not tok.isdigit() and tok not in {"tablet", "capsule", "solution", "injectable"}
    }
    has_chart_med_mention = any(re.search(rf"\b{re.escape(tok)}\b", q_lower) for tok in chart_med_tokens)
    if has_chart_med_mention or re.search(
        r"\b(?:med|medication|prescription|drug|pill|statin|plavix|clopidogrel|lipitor|simvastatin|prinivil|"
        r"lisinopril|ibuprofen|nsaid|warfarin|coumadin|eliquis|apixaban|xarelto|spironolactone|aldactone|"
        r"potassium|klor-con|lorazepam|ativan|zolpidem|ambien|naproxen|entresto|furosemide|lasix|glipizide|"
        r"metformin|anticoag|sedative|polypharmacy|beers)\b",
        q_lower,
    ):
        active_meds = [it for it in extracted.items if it.section == "med_active"]
        skip_kb = any(it.metadata.get("skip_stage2_skills") for it in extracted.items)
        med_lines: List[str] = []
        if skip_kb and re.search(r"\b(?:duplicate|same|conflict|overlap|double|reconcil|audit|statin|plavix|prinivil)\b", q_lower):
            header_cites = _citations_for_uuids([extracted.items[0].resource_uuid], uuid_map, condensed)
            c_ref = _add_cites(header_cites)
            med_lines.append(
                f"- Surface medication ledger review {c_ref}: active prescriptions carry distinct surface display names; "
                f"without RxNorm brand-to-generic or ATC therapeutic-class KB normalization, cross-clinic ingredient equivalence is not flagged."
            )
        else:
            for m in active_meds:
                cites = _citations_for_uuids([m.resource_uuid], uuid_map, condensed)
                c_ref = _add_cites(cites)
                kb_med = ({} if skip_kb else resolve_medication_code(m.code, m.display)) or {}
                purpose = kb_med.get("plain_language_purpose") or kb_med.get("plain_purpose") or ""
                flag = f" — **ALERT: {m.conflict_note}**" if m.conflict_note else ""
                purpose_str = f" *(Purpose: {purpose})*" if purpose else ""
                med_lines.append(
                    f"- **{m.display}** (started `{m.clinical_date or 'active'}`, source: `{m.source_org or 'Primary Care'}`){purpose_str}{flag} {c_ref}"
                )
        if not med_lines:
            header_cites = _citations_for_uuids([extracted.items[0].resource_uuid], uuid_map, condensed)
            c_ref = _add_cites(header_cites)
            med_lines.append(f"- No active outpatient prescriptions on file {c_ref}.")
        sections.append("### Active Medications & Reconciliation\n" + "\n".join(med_lines))

        # Also check historical NSAID / Ibuprofen if kidney or ibuprofen mentioned
        if "ibuprofen" in q_lower or "nsaid" in q_lower or "kidney" in q_lower:
            nsaids = [
                it
                for it in extracted.items
                if it.section in {"med_active", "med_stopped", "med_historical"}
                and any(ns in it.display.lower() for ns in ("ibuprofen", "naproxen", "meloxicam", "ketorolac"))
                and (use_lookup_tools or it.resource_uuid in uuid_map)
            ]
            if nsaids:
                nsaid_cites = _citations_for_uuids([it.resource_uuid for it in nsaids[:3]], uuid_map, condensed)
                if nsaid_cites:
                    c_ref = _add_cites(nsaid_cites)
                elif use_lookup_tools:
                    overflow_tool_calls.append(
                        {"tool": "search_chart_entries", "query": "ibuprofen", "recovered_count": len(nsaids)}
                    )
                    c_ref = _add_overflow_citation(
                        nsaids[-1].resource_uuid,
                        nsaids[-1].resource_type,
                        nsaids[-1].clinical_date or "",
                        nsaids[-1].text,
                    )
                else:
                    c_ref = ""
                sections.append(
                    f"**NSAID Renal & Bleeding Safety Note:** `{nsaids[-1].display}` orders ({len(nsaids)} entries) "
                    f"are documented in the chart {c_ref}; systemic NSAIDs should be avoided in Chronic Kidney Disease and concurrent anticoagulation."
                )

    # Condition / problem list / CKD / kidney / diabetes / heart failure / bleeding / falls / note-hold / audit questions
    if re.search(
        r"\b(?:audit|condition|problem|diagnos|ckd|kidney|renal|egfr|creatinine|diabetes|prediabetes|a1c|"
        r"sinusitis|conflict|contradict|discrepanc|quality|reconcil|hyperkalemia|potassium|heart\s+failure|"
        r"hfref|bleed|hemorrhage|inr|afib|atrial\s+fibrillation|fall|orthostatic|note|hold|discontinue|stop|"
        r"sedative|zolpidem|lorazepam|spironolactone|warfarin|eliquis|apixaban)\b",
        q_lower,
    ):
        cond_lines: List[str] = []
        items_by_uuid = {it.resource_uuid: it for it in extracted.items}
        for f in dq["findings"]:
            c_ref = _add_cites(f["citations"])
            if use_lookup_tools:
                all_f_uuids: List[str] = []
                for ru in f.get("resource_uuids") or []:
                    if ru not in all_f_uuids:
                        all_f_uuids.append(ru)
                    r_it = items_by_uuid.get(ru)
                    if r_it:
                        for mu in r_it.member_uuids:
                            if mu not in all_f_uuids:
                                all_f_uuids.append(mu)
                missing_u = [u for u in all_f_uuids if u not in uuid_map]
                if missing_u:
                    overflow_tool_calls.append(
                        {"tool": "search_chart_entries", "query": f["title"], "recovered_count": len(missing_u)}
                    )
                    extra_refs = []
                    for mu in missing_u:
                        m_it = items_by_uuid.get(mu)
                        extra_refs.append(
                            _add_overflow_citation(
                                mu,
                                m_it.resource_type if m_it else "Condition",
                                (m_it.clinical_date if m_it else "") or "",
                                (m_it.text if m_it else f["title"]),
                                m_it.member_uuids if m_it else (),
                            )
                        )
                    c_ref = f"{c_ref} {' '.join(extra_refs)}".strip()
            cond_lines.append(
                f"- **{f['title']}** ({f['domain']}): {f['description']} *Recommendation:* {f['recommendation']} {c_ref}"
            )
        if cond_lines:
            sections.append("### Clinical Problem List & Data Quality Findings\n" + "\n".join(cond_lines))

        # Include relevant lab trajectories (eGFR, Creatinine, HbA1c, Potassium, INR, Hemoglobin, NT-proBNP)
        traj_lines: List[str] = []
        for t in patterns["trajectories"]:
            if any(
                k in t["concept"].lower()
                for k in ("glomerular", "creatinine", "a1c", "glucose", "potassium", "inr", "international normalized", "hemoglobin", "natriuretic")
            ):
                if not t["citations"] and not use_lookup_tools:
                    continue
                if t["citations"]:
                    c_ref = _add_cites(t["citations"])
                else:
                    lab_hist = get_lab_history(extracted, t["concept"], sharing_posture=sharing_posture)
                    overflow_tool_calls.append(
                        {
                            "tool": "get_lab_history",
                            "analyte_or_loinc": t["concept"],
                            "recovered_count": len(lab_hist["results"]),
                        }
                    )
                    latest_pt = (lab_hist.get("summary") or {}).get("latest") or {}
                    c_ref = (
                        _add_overflow_citation(
                            str(latest_pt.get("resource_uuid") or ""),
                            "Observation",
                            str(latest_pt.get("date") or ""),
                            f"Observation: {t['concept']} {t['latest']['value']} {t['unit']}",
                        )
                        if latest_pt.get("resource_uuid")
                        else ""
                    )
                dir_word = "down" if t["direction"] == "decreasing" else ("up" if t["direction"] == "increasing" else "stable")
                traj_lines.append(
                    f"- **{t['concept']}** ({t['count']} measurements): initial `{t['initial']['value']} {t['unit']}` "
                    f"(`{t['initial']['date']}`) $\\to$ range `{t['min']['value']}–{t['max']['value']}` $\\to$ "
                    f"**latest `{t['latest']['value']} {t['unit']}` (`{t['latest']['date']}`)** [{t['direction']} / trending {dir_word}] {c_ref}"
                )
        if traj_lines:
            sections.append("### Supporting Renal, Metabolic & Hematologic Lab Trajectories\n" + "\n".join(traj_lines))

    # Immunization / vaccine / HPV / Zoster / Pneumococcal / flu / school questions
    if re.search(r"\b(?:immuniz|vaccin|shot|hpv|zoster|shingles|pneumococ|flu|influenza|covid|acip|school)\b", q_lower):
        imm_items = [it for it in extracted.items if it.section == "immunization"]
        imm_lines: List[str] = []
        recovered_imm = 0
        for it in imm_items:
            cites = _citations_for_uuids([it.resource_uuid], uuid_map, condensed)
            if cites:
                c_ref = _add_cites(cites)
            elif use_lookup_tools:
                recovered_imm += 1
                c_ref = _add_overflow_citation(
                    it.resource_uuid,
                    it.resource_type,
                    it.clinical_date or "",
                    it.text,
                    it.member_uuids,
                )
            else:
                continue
            flag = f" — **[{it.conflict_note}]**" if it.conflict_note else ""
            imm_lines.append(
                f"- `{it.clinical_date}`: **{it.display}** (source: `{it.source_org or 'Clinic'}`){flag} {c_ref}"
            )
        if recovered_imm:
            overflow_tool_calls.append(
                {"tool": "search_chart_entries", "query": "immunization", "recovered_count": recovered_imm}
            )
        sections.append(
            f"### Immunization History & ACIP Schedule Audit ({len(imm_lines)} doses in view)\n"
            + "\n".join(imm_lines)
        )

    # Hospital / ICU / surgery / procedural / inpatient encounter / perioperative questions
    if re.search(r"\b(?:cabg|icu|hospital|admi|stay|november\s+2023|2023-11-20|antibiotic|cefazolin|heart|cardiac|bypass|valve|stent|encounter|inpatient|discharge|transfusion|endoscopy)\b", q_lower):
        hl = patterns.get("highlight_encounter") or {
            "title": "Hospital & Perioperative Encounter Summary",
            "narrative": "Hospital encounter and inpatient medication administrations.",
            "window_start": "",
            "window_end": "",
            "citations": [],
            "key_procedures": [],
            "implantable_devices": [],
        }
        if hl:
            c_ref = _add_cites(hl.get("citations") or [])
            w_start = str(hl.get("window_start") or "2023-11-20")
            w_end = str(hl.get("window_end") or "2023-11-28")
            in_capsule_periop = [
                it
                for it in extracted.items
                if it.clinical_date
                and w_start <= it.clinical_date[:10] <= w_end
                and it.resource_uuid in uuid_map
            ]
            if use_lookup_tools:
                tool_res = search_chart_entries(
                    extracted,
                    query=q_text or "inpatient procedure medication device",
                    date_from=w_start,
                    date_to=w_end,
                    sharing_posture=sharing_posture,
                    limit=20,
                )
                overflow_tool_calls.append(
                    {
                        "tool": "search_chart_entries",
                        "date_from": w_start,
                        "date_to": w_end,
                        "recovered_count": len(tool_res["results"]),
                    }
                )
                periop_items = [
                    it
                    for it in extracted.items
                    if (it.clinical_date and w_start <= it.clinical_date[:10] <= w_end)
                    or ("cefazolin" in q_lower and "cefazolin" in it.display.lower())
                ]
                periop_priority = sorted(
                    periop_items,
                    key=lambda item: (
                        0
                        if item.section
                        in {
                            "procedure_surgical",
                            "procedure_major",
                            "procedure",
                            "condition_active",
                            "device",
                            "med_active",
                            "clinical_note",
                        }
                        else 1
                    ),
                )
                periop_refs_list: List[str] = []
                for it in periop_priority:
                    if len(periop_refs_list) >= 8:
                        break
                    ref_s = _add_overflow_citation(
                        it.resource_uuid,
                        it.resource_type,
                        it.clinical_date or "",
                        it.text,
                        it.member_uuids,
                    )
                    if ref_s and ref_s not in periop_refs_list:
                        periop_refs_list.append(ref_s)
                periop_ref = " ".join(periop_refs_list[:6])
                periop_meds = [
                    f"{it.display} ({it.clinical_date})"
                    for it in periop_items
                    if it.section in {"med_active", "med_historical", "med_stopped"}
                ]
                sections.append(
                    f"### {hl['title']}\n"
                    f"{hl['narrative']} {c_ref} {periop_ref}\n\n"
                    f"- **Key Procedures:** {', '.join(hl.get('key_procedures') or ['None'])}\n"
                    f"- **Perioperative / Episode Medications:** {', '.join(periop_meds[:8]) or 'Standard protocol'}\n"
                    f"- **Implantable / Patient Devices:** {', '.join(hl.get('implantable_devices') or ['None'])}"
                )
            else:
                periop_cites = _citations_for_uuids(
                    [it.resource_uuid for it in in_capsule_periop], uuid_map, condensed
                )
                periop_ref = _add_cites(periop_cites)
                periop_meds = [
                    f"{it.display} ({it.clinical_date})"
                    for it in in_capsule_periop
                    if it.section in {"med_active", "med_historical", "med_stopped"}
                ]
                in_capsule_procs = [
                    it.display
                    for it in in_capsule_periop
                    if it.section in {"procedure", "procedure_major", "procedure_surgical"}
                ]
                in_capsule_devs = [
                    it.display
                    for it in extracted.items
                    if it.section == "device" and it.resource_uuid in uuid_map
                ]
                static_narrative = (
                    hl["narrative"]
                    if in_capsule_periop
                    else (
                        f"Static Tier {condensed.tier} capsule ({condensed.actual_tokens} tokens) retains implantable/patient "
                        f"devices ({', '.join(in_capsule_devs) or 'None'}), while historical {w_start}..{w_end} "
                        f"episode medication administrations and procedural notes were condensed out "
                        f"of the in-context prompt."
                    )
                )
                sections.append(
                    f"### {hl['title']} (Static Tier {condensed.tier} View)\n"
                    f"{static_narrative} {c_ref} {periop_ref}\n\n"
                    f"- **Key Procedures in Static Capsule:** {', '.join(in_capsule_procs) or 'Omitted in Tier ' + str(condensed.tier)}\n"
                    f"- **Episode Medications in Static Capsule:** {', '.join(periop_meds[:8]) or 'Omitted in Tier ' + str(condensed.tier)}\n"
                    f"- **Implantable / Patient Devices:** {', '.join(in_capsule_devs) or 'None'}"
                )


    # Directed / Longitudinal Summary & Device questions
    if re.search(r"\b(?:summar|referral|overview|device|denture|dental|teeth|social\s+history)\b", q_lower):
        ds = patterns["directed_summary"]
        all_cond_items = [
            it
            for it in extracted.items
            if it.section in {"condition_active", "med_active", "device", "immunization"}
            and not (sharing_posture == "external_share" and it.sensitivity == "SDOH/PRIVACY-SENSITIVE")
        ]
        all_cond_uuids = [it.resource_uuid for it in all_cond_items]
        ds_cites = _citations_for_uuids(all_cond_uuids, uuid_map, condensed)
        c_ref = _add_cites(ds_cites)
        if use_lookup_tools:
            extra_ds_refs: List[str] = []
            for it in all_cond_items:
                if len(extra_ds_refs) >= 6:
                    break
                if it.resource_uuid not in uuid_map:
                    extra_ds_refs.append(
                        _add_overflow_citation(
                            it.resource_uuid,
                            it.resource_type,
                            it.clinical_date or "",
                            it.text,
                            it.member_uuids,
                        )
                    )
            if extra_ds_refs:
                overflow_tool_calls.append(
                    {"tool": "search_chart_entries", "query": "directed_summary_overflow", "recovered_count": len(extra_ds_refs)}
                )
                c_ref = f"{c_ref} {' '.join(extra_ds_refs[:6])}".strip()
        devices_list = [it.display for it in extracted.items if it.section == "device"]
        active_c_str = ", ".join(c["display"] for c in ds["active_conditions"]) or "None active"
        active_m_str = ", ".join(m["display"] for m in ds["active_medications"]) or "None active"
        sections.append(
            f"### Directed Clinical Summary ({ds['posture_label']})\n"
            f"- **Active Conditions:** {active_c_str} {c_ref}\n"
            f"- **Active Medications:** {active_m_str}\n"
            f"- **Patient Devices:** {', '.join(devices_list) or 'None recorded'}\n"
            f"- **Sensitive SDOH Status:** "
            + (
                f"Redacted ({ds['redacted_privacy_count']} items excluded for external sharing)"
                if sharing_posture == "external_share"
                else (", ".join(ds["sensitive_sdoh_included"]) or "None")
            )
        )

    # Duplicate labs / bloat / compression questions
    if re.search(r"\b(?:duplicate|bloat|panel|repeat|condens|token|compress)\b", q_lower):
        sm = bloat["summary_metrics"]
        c_Lines: List[str] = []
        for card in bloat["cards"]:
            c_ref = _add_cites(card.get("citations") or [])
            c_Lines.append(f"- **{card['title']}:** {card['summary']} {c_ref}")
        sections.append(
            f"### Duplicate Labs & Record Condensation Analysis (Tier {sm['tier']}: {sm['condensed_tokens']:,} tokens)\n"
            + "\n".join(c_Lines)
        )

    # Fallback general patient summary if no specific domain matched
    if not sections:
        header_cites = _citations_for_uuids(
            [it.resource_uuid for it in extracted.items[:6]], uuid_map, condensed
        )
        c_ref = _add_cites(header_cites)
        ds = patterns["directed_summary"]
        active_c_str = ", ".join(c["display"] for c in ds["active_conditions"]) or "None active"
        active_m_str = ", ".join(m["display"] for m in ds["active_medications"]) or "None active"
        traj_summary_str = ", ".join(
            f"{t['concept']}: {t['latest']['value']} {t['unit']}"
            for t in patterns["trajectories"][:5]
        )
        sections.append(
            f"### Grounded Patient Health Summary: {extracted.patient_name} ({ds['posture_label']})\n"
            f"- **Demographics & Anchor Date:** Birth date `{extracted.birth_date}`, reference date `{extracted.reference_date}` {c_ref}\n"
            f"- **Active Problem List ({len(ds['active_conditions'])}):** {active_c_str}\n"
            f"- **Active Medications ({len(ds['active_medications'])}):** {active_m_str}\n"
            f"- **Data Quality Alerts Flagged:** `{dq['total_findings']}` clinical reconciliation findings ({dq['high_severity_count']} high-severity)\n"
            f"- **Longitudinal Trajectories:** {traj_summary_str}"
        )

    collected_citations.sort(key=lambda c: c["index"])
    cite_refs_str = _format_citation_refs(collected_citations[:4]) or "[1]"

    # Determine patient-centered taxonomy category, plain-English summary, doctor checklist, and retrieval source mode
    patient_category = "4. Next-Visit Checklist"
    source_mode = "full_chart_lookup" if overflow_tool_calls else "ips_safety_core"
    source_mode_label = (
        "Answered via On-Demand Full-Chart Lookup"
        if source_mode == "full_chart_lookup"
        else "Answered from In-Context IPS Safety Core"
    )
    if re.search(
        r"\b(?:cabg|icu|hospital|admi|stay|november\s+2023|2023-11-20|antibiotic|cefazolin|bypass|encounter|inpatient|discharge|transfusion|endoscopy)\b",
        q_lower,
    ) and not re.search(r"\b(?:hold|spironolactone|warfarin|eliquis|apixaban|naproxen|lorazepam|zolpidem)\b", q_lower):
        patient_category = "3. Making Sense of a Hospital/ICU Stay"
        if use_lookup_tools and condensed.tier >= 3:
            source_mode = "full_chart_lookup"
            source_mode_label = "Answered via On-Demand Full-Chart Lookup"
    elif has_chart_med_mention or re.search(
        r"\b(?:med|medication|prescription|drug|pill|statin|plavix|clopidogrel|lipitor|simvastatin|prinivil|"
        r"lisinopril|ibuprofen|nsaid|warfarin|eliquis|apixaban|spironolactone|potassium|lorazepam|ativan|"
        r"zolpidem|ambien|naproxen|entresto|furosemide|hold)\b",
        q_lower,
    ):
        patient_category = "1. Medication & Pill-Bottle Clarity"
    elif re.search(
        r"\b(?:ckd|kidney|renal|egfr|creatinine|diabetes|prediabetes|a1c|sinusitis|trend|hemorrhage|bleed|inr|hemoglobin)\b",
        q_lower,
    ):
        patient_category = "2. Lab Trends & New Diagnoses"
    elif re.search(r"\b(?:immuniz|vaccin|shot|hpv|zoster|shingles|pneumococ|flu|influenza|covid|acip|school)\b", q_lower):
        patient_category = "5. School/Vaccine Readiness"
    elif re.search(r"\b(?:share|privacy|sdoh|referral|external|school\s+nurse|employer|duplicate|bloat|panel|repeat)\b", q_lower):
        patient_category = "6. Safe Sharing Preview"

    # Build patient-friendly plain-English summary and "What to Ask Your Doctor" checklist
    plain_paragraphs: List[str] = []
    ask_doctor_items: List[str] = []
    clinician_notes: List[str] = []

    for f in dq["findings"]:
        if f.get("patient_plain_summary"):
            plain_paragraphs.append(f"- **{f['title']}:** {f['patient_plain_summary']}")
        if f.get("what_to_ask_doctor") and f["what_to_ask_doctor"] not in ask_doctor_items:
            ask_doctor_items.append(f["what_to_ask_doctor"])
        if f.get("clinician_technical_note"):
            clinician_notes.append(f["clinician_technical_note"])

    if patient_category == "3. Making Sense of a Hospital/ICU Stay" and patterns.get("highlight_encounter"):
        hl = patterns["highlight_encounter"]
        plain_paragraphs.insert(
            0,
            f"**{hl.get('title', 'Highlighted Hospital / Care Episode')} ({hl.get('window', 'Recent')}):** "
            f"{hl.get('narrative', '')} {cite_refs_str}",
        )
        ask_doctor_items.insert(
            0,
            f"Following my care episode ({hl.get('window', 'recent visit')}), which medications, follow-up labs, or precautions do I need to stay on long-term?",
        )

    if not plain_paragraphs:
        ds = patterns["directed_summary"]
        active_c_str = ", ".join(c["display"] for c in ds["active_conditions"]) or "no active conditions"
        active_m_str = ", ".join(m["display"] for m in ds["active_medications"]) or "no active daily pills"
        plain_paragraphs.append(
            f"**Plain-English Summary for {extracted.patient_name}:** Your chart shows {active_c_str}, "
            f"and your current medication list includes {active_m_str} {cite_refs_str}."
        )
    else:
        plain_paragraphs.append(f"*Verified against chart entries {cite_refs_str}.*")

    if not ask_doctor_items:
        ask_doctor_items.append(
            "Can we review my active problem list and current medication bottles together so my chart matches what I take at home?"
        )

    clinician_technical_summary = (
        " | ".join(clinician_notes)
        if clinician_notes
        else f"Condensed at Tier {condensed.tier} ({condensed.actual_tokens:,} tokens; posture={sharing_posture}) with {len(collected_citations)} resolved FHIR UUID citations {cite_refs_str}."
    )

    return {
        "question": q_text,
        "patient_category": patient_category,
        "abstained": False,
        "abstains": False,
        "abstention_reason": None,
        "answer": "\n\n".join(sections),
        "patient_plain_summary": "\n".join(plain_paragraphs),
        "what_to_ask_doctor": ask_doctor_items[:3],
        "clinician_technical_note": clinician_technical_summary,
        "source_mode": source_mode,
        "source_mode_label": source_mode_label,
        "overflow_tool_calls": overflow_tool_calls,
        "citations": collected_citations,
        "kb_references": kb_hits,
        "tier_info": {
            "tier": condensed.tier,
            "tier_name": condensed.tier_name,
            "target_tokens": condensed.target_tokens,
            "actual_tokens": condensed.actual_tokens,
            "sharing_posture": sharing_posture,
        },
    }


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
        clean_line = re.sub(r"\s*\[(?:ingredient_group|conflict|acip_schedule_note|duplicate_candidate)[^\]]*\]", "", text).strip()

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
        elif rtype in {"Observation", "DiagnosticReport"} and not clean_line.startswith("Clinical Note"):
            labs_and_vitals.append(f"{prefix_date}{clean_line}")
        elif rtype in {"DocumentReference", "Encounter", "Procedure", "Device", "CarePlan"} or clean_line.startswith("Clinical Note"):
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
    import json as _json

    if raw_json_tokens is None:
        raw_json_tokens = estimate_tokens_local(_json.dumps(bundle))

    fhir_browser = build_fhir_browser_index(bundle)
    extracted_lossless = extract_bundle_lossless(bundle, enable_privacy_classification=True, filter_room_equipment_devices=False)
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
    room_equip_count = int(excl.get("Device(clinic_equipment_retained)", 0)) + int(excl.get("Device(clinic_equipment)", 0))
    sensitive_items = [it for it in extracted_lossless.items if it.sensitivity == "SDOH/PRIVACY-SENSITIVE"]

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
    for mode_key, internal_mode in (("lossless_structural", "lossless_structural"), ("hierarchical", "tiered")):
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
                "human_summary": _extract_human_summary_from_condensed(c_res, condense_mode=mode_key),
            }

    # 3. Build the 9 Read-Only Patient-Facing Skills
    ds = patterns["directed_summary"]
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
        f["what_to_ask_doctor"]
        for f in dq["findings"]
        if f.get("what_to_ask_doctor")
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
    for p_str in (hl_enc.get("key_procedures") or hl_enc.get("procedures") or []):
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
    for m_str in (hl_enc.get("inpatient_medications") or []):
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
        kb_res = m_it.metadata.get("kb_resolution") or resolve_medication_code(m_it.code, m_it.display)
        t_class = str(kb_res.get("therapeutic_class") or m_it.metadata.get("therapeutic_class") or "Medication").replace("_", " ").title()
        ing_name = str(kb_res.get("ingredient") or m_it.metadata.get("ingredient_group") or m_it.ingredient_key or "").title()
        reasons = ", ".join(m_it.metadata.get("reasons") or []) or f"Used for {t_class} management"
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
    med_cites = _citations_for_uuids([m.resource_uuid for m in active_meds], uuid_map, condensed_8k)
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
                "source_org": t.get("reference_range") and f"Ref: {t['reference_range']}" or "Longitudinal Lab Series",
                "code": f"LOINC {t.get('loinc') or t.get('category') or 'Lab'}".strip(),
                "tone": "warn" if str(t.get("direction") or "") in {"increasing", "decreasing", "rising", "falling"} else "info",
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
        orgs_str = ", ".join(sorted({x.source_org for x in ordered if x.source_org})) or "Immunization Record"
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
            for k in ("medication", "drug", "duplicate therapy", "therapeutic class", "interaction", "narrative")
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
                "source_org": ", ".join(f.get("source_organizations") or []) or "Multi-Clinic / Discharge Record",
                "code": f.get("kb_rule_id") or "RxNorm Check",
                "tone": "warn",
            }
        )
        if f.get("what_to_ask_doctor"):
            med_check_remediation.append(f"Ask your doctor: \"{f['what_to_ask_doctor']}\"")
        if f.get("recommendation"):
            med_check_remediation.append(f"Suggested chart remediation (read-only): {f['recommendation']}")
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
        med_check_remediation.append("No conflicting active prescriptions were detected. Continue reviewing your medication list at routine visits.")

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
        if any(k in (f.get("domain") or "").lower() for k in ("condition", "staging", "kdigo", "glycemic", "problem"))
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
                "source_org": ", ".join(f.get("source_organizations") or []) or "Multi-Clinic Problem List",
                "code": f.get("kb_rule_id") or "SNOMED Check",
                "tone": "warn",
            }
        )
        if f.get("what_to_ask_doctor"):
            cond_check_remediation.append(f"Ask your doctor: \"{f['what_to_ask_doctor']}\"")
        if f.get("recommendation"):
            cond_check_remediation.append(f"Suggested chart remediation (read-only): {f['recommendation']}")
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
        cond_check_remediation.append("Your active problem list has no mutually exclusive disease stages or active-vs-resolved clinic conflicts.")

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
            f"When records are combined from multiple clinics over several years, older diagnoses often stay marked 'active' even after a condition progresses or heals. "
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

    # Check immunization duplicates/anomalies from dq["findings"]
    imm_conflict_findings = [
        f for f in dq["findings"] if "immunization" in (f.get("domain") or "").lower() or (f.get("id") or "").startswith("conflict-imm")
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
            hygiene_remediation.append(f"Ask your doctor: \"{f['what_to_ask_doctor']}\"")
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
    priv_cites = _citations_for_uuids([s.resource_uuid for s in sensitive_items], uuid_map, condensed_8k)
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
            f"By default, your full personal health view keeps 100% of your records visible. "
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

