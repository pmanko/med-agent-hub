"""Explicit Chart-Hygiene & Clinical Reconciliation Skills (`W12`).

Decouples chart condensation (`lossless_structural` in `condenser.py`) from clinical
interpretation, duplicate detection, and cross-organization reconciliation.

Each skill inspects an unmutated `ExtractedChart` (produced by `extract_bundle_lossless(bundle)`)
and returns structured, user-facing `ChartFinding` cards (`DuplicateCandidate`,
`ReconciliationFinding`, `ScheduleAnomaly`) with `[N]` bracket citations, FHIR resource UUIDs,
and `Source A vs. Source B` organization provenance.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .extractors import (
    ClinicalItem,
    ExtractedChart,
    annotate_immunization_anomalies,
    classify_vaccine_group,
    detect_condition_conflicts,
    detect_medication_conflicts,
    extract_bundle_lossless,
    normalize_drug_ingredient,
)
from .kb_lookup import (
    resolve_condition_code,
    resolve_immunization_code,
    resolve_medication_code,
)


@dataclass(frozen=True)
class ChartFinding:
    finding_id: str
    skill_name: str
    category: str  # "DuplicateCandidate" | "ReconciliationFinding" | "ScheduleAnomaly"
    severity: str  # "high" | "moderate" | "info"
    title: str
    summary: str
    source_a: Optional[str]
    source_b: Optional[str]
    resource_uuids: Tuple[str, ...]
    bracket_citations: Tuple[int, ...] = ()
    recommended_action: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "skill_name": self.skill_name,
            "category": self.category,
            "severity": self.severity,
            "title": self.title,
            "summary": self.summary,
            "source_a": self.source_a,
            "source_b": self.source_b,
            "resource_uuids": list(self.resource_uuids),
            "bracket_citations": list(self.bracket_citations),
            "recommended_action": self.recommended_action,
        }


def _resolve_brackets_for_uuids(
    uuids: Sequence[str],
    mappings: Optional[Sequence[Mapping[str, Any]]],
) -> Tuple[int, ...]:
    if not mappings or not uuids:
        return ()
    target = set(uuids)
    indices: List[int] = []
    for idx, m in enumerate(mappings, start=1):
        primary = str(m.get("resourceUuid") or "")
        members = set(m.get("memberUuids") or [])
        if primary in target or (target & members):
            indices.append(idx)
    return tuple(dict.fromkeys(indices))


def run_duplicate_and_redundancy_skill(
    extracted: ExtractedChart,
    mappings: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[ChartFinding]:
    """Identify candidate duplicate or redundant records without deleting them from the chart:
    1. Same-day duplicate immunization records across clinic vs. registry sources.
    2. Duplicate active medication entries for the same ingredient/dose across sources.
    3. Redundant active problem-list Condition entries sharing the same code/display.
    """
    findings: List[ChartFinding] = []

    # 1. Same-day duplicate immunizations
    imms = [it for it in extracted.items if it.section == "immunization"]
    by_group_date: Dict[Tuple[str, str], List[ClinicalItem]] = {}
    for imm in imms:
        grp = imm.ingredient_key or classify_vaccine_group(imm.code, imm.display)
        if imm.date:
            by_group_date.setdefault((grp, imm.date), []).append(imm)

    for (grp, occ_date), group_items in sorted(by_group_date.items()):
        if len(group_items) < 2:
            continue
        uuids = tuple(dict.fromkeys(x.resource_uuid for x in group_items))
        orgs = [x.source_org or "Unattributed Source" for x in group_items]
        src_a = orgs[0]
        src_b = orgs[1] if len(orgs) > 1 else orgs[0]
        findings.append(
            ChartFinding(
                finding_id=f"dup-imm-{grp}-{occ_date}",
                skill_name="duplicate_and_redundancy_skill",
                category="DuplicateCandidate",
                severity="moderate",
                title=f"Candidate Same-Day Duplicate Immunization: {group_items[0].display} ({occ_date})",
                summary=(
                    f"{len(group_items)} immunization records for {group_items[0].display} ({grp} group) "
                    f"are recorded on the same date ({occ_date}) across {', '.join(dict.fromkeys(orgs))}."
                ),
                source_a=src_a,
                source_b=src_b,
                resource_uuids=uuids,
                bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                recommended_action=(
                    "Verify whether these represent a single vaccine administration logged by both "
                    "the administering clinic and the immunization registry before merging."
                ),
            )
        )

    # 2. Duplicate active medication entries (same ingredient + identical display/dosage)
    active_meds = [it for it in extracted.items if it.section == "med_active"]
    by_ing: Dict[str, List[ClinicalItem]] = {}
    for m in active_meds:
        ing = m.ingredient_key or normalize_drug_ingredient(m.display)
        by_ing.setdefault(ing, []).append(m)

    for ing, med_group in sorted(by_ing.items()):
        if len(med_group) < 2:
            continue
        displays = {m.display.strip().lower() for m in med_group}
        if len(displays) == 1:
            uuids = tuple(dict.fromkeys(x.resource_uuid for x in med_group))
            orgs = [x.source_org or "Unattributed Source" for x in med_group]
            src_entries = ", ".join(f"{x.date} [{x.source_org or 'clinic'}]" for x in med_group)
            findings.append(
                ChartFinding(
                    finding_id=f"dup-med-{ing}",
                    skill_name="duplicate_and_redundancy_skill",
                    category="DuplicateCandidate",
                    severity="moderate",
                    title=f"Duplicate Active Medication Order: {med_group[0].display}",
                    summary=(
                        f"{len(med_group)} active medication entries for {med_group[0].display} "
                        f"exist simultaneously ({src_entries})."
                    ),
                    source_a=orgs[0],
                    source_b=orgs[1] if len(orgs) > 1 else orgs[0],
                    resource_uuids=uuids,
                    bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                    recommended_action="Confirm active prescriber and cancel redundant active order if duplicated across encounters.",
                )
            )

    # 3. Redundant active Conditions with identical normalized display
    active_conds = [it for it in extracted.items if it.section == "condition_active"]
    by_cond_disp: Dict[str, List[ClinicalItem]] = {}
    for c in active_conds:
        by_cond_disp.setdefault(c.display.strip().lower(), []).append(c)

    for disp_key, cond_group in sorted(by_cond_disp.items()):
        if len(cond_group) < 2:
            continue
        uuids = tuple(dict.fromkeys(x.resource_uuid for x in cond_group))
        orgs = [x.source_org or "Unattributed Source" for x in cond_group]
        findings.append(
            ChartFinding(
                finding_id=f"dup-cond-{cond_group[0].code or disp_key[:20]}",
                skill_name="duplicate_and_redundancy_skill",
                category="DuplicateCandidate",
                severity="info",
                title=f"Redundant Active Condition Entries: {cond_group[0].display}",
                summary=(
                    f"{len(cond_group)} separate active Condition resources exist for {cond_group[0].display} "
                    f"(onsets: {', '.join(str(x.date) for x in cond_group)})."
                ),
                source_a=orgs[0],
                source_b=orgs[1] if len(orgs) > 1 else orgs[0],
                resource_uuids=uuids,
                bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                recommended_action="Consolidate redundant problem-list entries while retaining earliest onset date.",
            )
        )

    return findings


def run_medication_reconciliation_skill(
    extracted: ExtractedChart,
    mappings: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[ChartFinding]:
    """Detect active medication dose contradictions, therapeutic-class overlaps, and
    free-text clinical note vs. structured MedicationRequest contradictions.
    """
    findings: List[ChartFinding] = []
    active_meds = [it for it in extracted.items if it.section == "med_active"]

    raw_med_dicts: List[Dict[str, Any]] = []
    for m in active_meds:
        raw_med_dicts.append(
            {
                "resourceUuid": m.resource_uuid,
                "code": m.code,
                "display": m.display,
                "date": m.date,
                "source_org": m.source_org,
                "ingredient_key": m.ingredient_key or normalize_drug_ingredient(m.display),
            }
        )

    conflict_map = detect_medication_conflicts(raw_med_dicts)
    by_conflict_msg: Dict[str, List[ClinicalItem]] = {}
    for m in active_meds:
        if m.resource_uuid in conflict_map:
            msg = conflict_map[m.resource_uuid]
            by_conflict_msg.setdefault(msg, []).append(m)

    for idx, (note_text, med_group) in enumerate(sorted(by_conflict_msg.items()), start=1):
        uuids = tuple(dict.fromkeys(x.resource_uuid for x in med_group))
        orgs = [x.source_org or "Unattributed Source" for x in med_group]
        ings = list(dict.fromkeys(x.ingredient_key or normalize_drug_ingredient(x.display) for x in med_group))
        kb_info = resolve_medication_code(med_group[0].code, med_group[0].display)
        t_class = str(kb_info.get("therapeutic_class") or "medication")
        ing_label = " + ".join(i.title() for i in ings)
        findings.append(
            ChartFinding(
                finding_id=f"reconcile-med-{idx}-{'-'.join(ings)[:24].replace(' ', '-')}",
                skill_name="medication_reconciliation_skill",
                category="ReconciliationFinding",
                severity="high",
                title=f"Active Medication Conflict ({ing_label} / {t_class})",
                summary=(
                    f"Conflicting or overlapping active orders ({ing_label}): "
                    + "; ".join(
                        f"{x.display} (authored {x.date}, source: {x.source_org or 'unspecified'})"
                        for x in med_group
                    )
                    + (f" — {note_text}" if note_text else "")
                ),
                source_a=orgs[0],
                source_b=orgs[1] if len(orgs) > 1 else orgs[0],
                resource_uuids=uuids,
                bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                recommended_action=(
                    f"Reconcile active {', '.join(ings)} prescriptions across {', '.join(dict.fromkeys(orgs))} "
                    "to prevent duplicate dosing, therapeutic class overlap, or interaction toxicity."
                ),
            )
        )

    # Free-text clinical note vs. structured active MedicationRequest contradiction
    note_items = [it for it in extracted.items if it.section in {"clinical_note", "note"}]
    hold_re = re.compile(
        r"\b(?:hold|stop|discontinue|held|stopped|do\s+not\s+take)\s+([^.;\n]{3,90})",
        re.IGNORECASE,
    )
    seen_note_med_pairs: set[Tuple[str, str]] = set()
    for n_item in note_items:
        for match in hold_re.finditer(n_item.text or ""):
            clause = match.group(1).lower()
            for m_item in active_meds:
                m_ing = (m_item.ingredient_key or normalize_drug_ingredient(m_item.display)).lower()
                m_disp_tokens = [
                    t for t in re.split(r"[^a-z0-9]+", m_item.display.lower()) if len(t) >= 4 and not t.isdigit()
                ]
                if (m_ing and m_ing in clause) or any(tok in clause for tok in m_disp_tokens[:2]):
                    pair_key = (m_item.resource_uuid, n_item.resource_uuid)
                    if pair_key in seen_note_med_pairs:
                        continue
                    seen_note_med_pairs.add(pair_key)
                    uuids = (m_item.resource_uuid, n_item.resource_uuid)
                    findings.append(
                        ChartFinding(
                            finding_id=f"reconcile-note-med-{m_ing.replace(' ', '-')}-{n_item.resource_uuid[:8]}",
                            skill_name="medication_reconciliation_skill",
                            category="ReconciliationFinding",
                            severity="high",
                            title=f"Note vs. Active Order Contradiction: {m_item.display}",
                            summary=(
                                f"Clinical note ({n_item.date or 'recent'}, source: {n_item.source_org or 'clinic note'}) "
                                f"instructs to hold/discontinue {m_item.display}, but structured "
                                f"{m_item.resource_type} ({m_item.date}, source: {m_item.source_org or 'order ledger'}) "
                                "remains ACTIVE."
                            ),
                            source_a=n_item.source_org or "Clinical Note",
                            source_b=m_item.source_org or "Structured Medication Ledger",
                            resource_uuids=uuids,
                            bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                            recommended_action=(
                                f"Verify whether {m_item.display} should be placed on hold or discontinued "
                                "in the structured order list per the clinical note."
                            ),
                        )
                    )


    return findings


def run_condition_stage_reconciliation_skill(
    extracted: ExtractedChart,
    mappings: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[ChartFinding]:
    """Detect conflicting active disease stages (e.g., CKD stages 1, 2, and 3 simultaneously active)
    or active lower-stage vs. resolved higher-stage contradictions across sources.
    """
    findings: List[ChartFinding] = []
    cond_items = [
        it for it in extracted.items if it.section in {"condition_active", "condition_resolved"}
    ]
    raw_active: List[Dict[str, Any]] = []
    raw_resolved: List[Dict[str, Any]] = []
    for c in cond_items:
        d = {
            "resourceUuid": c.resource_uuid,
            "code": c.code,
            "display": c.display,
            "status": c.status,
            "onset": c.metadata.get("onset") or c.date,
            "abatement": c.metadata.get("abatement") or c.end_date,
            "source_org": c.source_org,
        }
        if c.section == "condition_active":
            raw_active.append(d)
        else:
            raw_resolved.append(d)

    c_conflicts = detect_condition_conflicts(raw_active, raw_resolved)
    if not c_conflicts:
        return findings

    by_msg: Dict[str, List[ClinicalItem]] = {}
    for c in cond_items:
        msg = c_conflicts.get(c.resource_uuid)
        if msg:
            by_msg.setdefault(msg, []).append(c)

    for idx, (msg, group) in enumerate(sorted(by_msg.items()), start=1):
        uuids = tuple(dict.fromkeys(x.resource_uuid for x in group))
        orgs = [x.source_org or "Unattributed Source" for x in group]
        kb = resolve_condition_code(group[0].code, group[0].display)
        family = str(kb.get("disease_family") or "Condition Stage")
        findings.append(
            ChartFinding(
                finding_id=f"reconcile-cond-stage-{idx}",
                skill_name="condition_stage_reconciliation_skill",
                category="ReconciliationFinding",
                severity="high",
                title=f"Condition Stage / Status Discrepancy ({family})",
                summary=(
                    f"{msg}. Involved records: "
                    + "; ".join(
                        f"{x.display} (status={x.status}, date={x.date}, source={x.source_org or 'unspecified'})"
                        for x in group
                    )
                ),
                source_a=orgs[0],
                source_b=orgs[1] if len(orgs) > 1 else orgs[0],
                resource_uuids=uuids,
                bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                recommended_action=(
                    "Reconcile active problem list against latest eGFR/diagnostic staging and mark "
                    "superseded disease stages as resolved."
                ),
            )
        )
    return findings


def run_acip_immunization_skill(
    extracted: ExtractedChart,
    mappings: Optional[Sequence[Mapping[str, Any]]] = None,
) -> List[ChartFinding]:
    """Evaluate immunization series against ACIP minimum intervals and series dose ceilings."""
    findings: List[ChartFinding] = []
    imms = [it for it in extracted.items if it.section == "immunization"]
    raw_imms: List[Dict[str, Any]] = []
    for imm in imms:
        raw_imms.append(
            {
                "resourceUuid": imm.resource_uuid,
                "code": imm.code,
                "display": imm.display,
                "date": imm.date,
                "status": imm.status,
                "source_org": imm.source_org,
                "group": imm.ingredient_key or classify_vaccine_group(imm.code, imm.display),
            }
        )

    anom_map = annotate_immunization_anomalies(raw_imms, extracted.birth_date)
    by_group: Dict[str, List[ClinicalItem]] = {}
    for imm in imms:
        grp = imm.ingredient_key or classify_vaccine_group(imm.code, imm.display)
        by_group.setdefault(grp, []).append(imm)

    seen_groups: set[str] = set()
    for imm in imms:
        anom = anom_map.get(imm.resource_uuid)
        if not anom or not anom.startswith("acip_schedule_note:"):
            continue
        grp = imm.ingredient_key or classify_vaccine_group(imm.code, imm.display)
        if grp in seen_groups:
            continue
        seen_groups.add(grp)
        series_items = sorted(by_group.get(grp, [imm]), key=lambda x: x.date or "")
        uuids = tuple(dict.fromkeys(x.resource_uuid for x in series_items))
        orgs = [x.source_org or "Unattributed Source" for x in series_items]
        imm_kb = resolve_immunization_code(imm.code, imm.display)
        findings.append(
            ChartFinding(
                finding_id=f"acip-schedule-{grp}",
                skill_name="acip_immunization_skill",
                category="ScheduleAnomaly",
                severity="moderate",
                title=f"ACIP Immunization Schedule Anomaly: {imm_kb.get('vaccine_group', grp)}",
                summary=(
                    f"{anom}. Full series ({len(series_items)} doses): "
                    + ", ".join(f"{x.display} on {x.date}" for x in series_items)
                ),
                source_a=orgs[0],
                source_b=orgs[-1] if len(orgs) > 1 else orgs[0],
                resource_uuids=uuids,
                bracket_citations=_resolve_brackets_for_uuids(uuids, mappings),
                recommended_action=(
                    "Review vaccine administration dates against ACIP minimum intervals and registry "
                    "import logs to determine whether an invalid/early dose must be repeated or deduplicated."
                ),
            )
        )
    return findings


def run_chart_hygiene_and_reconciliation_skills(
    bundle_or_extracted: Mapping[str, Any] | ExtractedChart,
    mappings: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """Execute all modular chart-hygiene and clinical reconciliation skills on an unmutated chart."""
    extracted = (
        bundle_or_extracted
        if isinstance(bundle_or_extracted, ExtractedChart)
        else extract_bundle_lossless(bundle_or_extracted)
    )
    dup_findings = run_duplicate_and_redundancy_skill(extracted, mappings=mappings)
    med_findings = run_medication_reconciliation_skill(extracted, mappings=mappings)
    cond_findings = run_condition_stage_reconciliation_skill(extracted, mappings=mappings)
    acip_findings = run_acip_immunization_skill(extracted, mappings=mappings)

    all_findings = dup_findings + med_findings + cond_findings + acip_findings
    counts_by_category: Dict[str, int] = {}
    counts_by_skill: Dict[str, int] = {}
    for f in all_findings:
        counts_by_category[f.category] = counts_by_category.get(f.category, 0) + 1
        counts_by_skill[f.skill_name] = counts_by_skill.get(f.skill_name, 0) + 1

    lines: List[str] = []
    for f in all_findings:
        cites = "".join(f"[{idx}]" for idx in f.bracket_citations)
        cite_prefix = f"{cites} " if cites else ""
        src_pair = (
            f" (Source A: {f.source_a} vs. Source B: {f.source_b})"
            if f.source_a and f.source_b and f.source_a != f.source_b
            else (f" (Source: {f.source_a})" if f.source_a else "")
        )
        lines.append(
            f"- [{f.category} | {f.severity.upper()}] {cite_prefix}{f.title}{src_pair}: "
            f"{f.summary} Action: {f.recommended_action}"
        )

    return {
        "patient_id": extracted.patient_id,
        "total_findings": len(all_findings),
        "counts_by_category": counts_by_category,
        "counts_by_skill": counts_by_skill,
        "findings": [f.to_dict() for f in all_findings],
        "formatted_findings_block": "\n".join(lines) if lines else "No duplicate or reconciliation findings detected.",
    }
