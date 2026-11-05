"""5-Tier Progressive FHIR Chart Condenser and Token-Budget Controller.

Condenses any arbitrary-size FHIR R4 Bundle (`ExtractedChart`) into a target token budget
(`target_tokens`) while preserving:
- 100% of safety-critical IPS core state (Allergies, Active Conditions, Active Medications,
  Anomalous/Duplicate Immunizations, Active Patient Devices)
- 100% `[N] -> (resourceType, resourceUuid, memberUuids)` provenance for citation grounding
- Temporal fidelity: folded spans always carry the latest clinical date in `(YYYY-MM-DD)` and
  state `first recorded YYYY-MM-DD through YYYY-MM-DD` (plus `(initial <val> on <date> -> ...)`
  for trajectories) so `temporal.py` validates both endpoints and trend directions cleanly
- Explicit `[Completeness Manifest]` so models never mistake budget omission for a clinical negative
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from ..chart_serializer import render_chart
from .extractors import (
    ClinicalItem,
    ExtractedChart,
    NumericPoint,
    extract_bundle,
    extract_bundle_lossless,
)
from .normalizers import classify_sensitivity

# Priority analytes surfaced first in Tier 3 / Tier 4 LabTrajectory rollups.
_PRIORITY_ANALYTE_PATTERNS = (
    r"glomerular\s+filtration|egfr",
    r"creatinine\s*\[mass/volume\]|^creatinine$",
    r"hemoglobin\s+a1c|hba1c",
    r"glucose\s*\(blood/serum\)|^glucose$",
    r"potassium",
    r"sodium",
    r"urea\s+nitrogen|bun",
    r"microalbumin|albumin/creatinine",
    r"systolic\s+blood\s+pressure|blood\s+pressure",
    r"diastolic\s+blood\s+pressure",
    r"low\s+density\s+lipoprotein|ldl",
    r"high\s+density\s+lipoprotein|hdl",
    r"cholesterol",
    r"triglycerides",
    r"hemoglobin\s*\[mass/volume\]|^hemoglobin$",
    r"leukocytes|white\s+blood",
    r"platelets",
    r"body\s+mass\s+index|bmi",
    r"body\s+weight",
    r"fev1/fvc",
    r"phq-?[29]|depression",
    r"gad-?7|anxiety",
    r"audit-?c|alcohol",
)


@dataclass(frozen=True)
class CondensedChartResult:
    tier: int
    tier_name: str
    target_tokens: int
    actual_tokens: int
    chart_text: str
    records: List[Dict[str, Any]]
    mappings: List[Dict[str, Any]]
    completeness_manifest: Dict[str, Any]
    redacted_for_privacy: List[Dict[str, Any]]
    compression_stats: Dict[str, Any]


def estimate_tokens_local(text: str) -> int:
    """Deterministic local token estimator calibrated to Gemma-4 / Llama-3 BPE tokenizers on clinical text.

    Clinical numbered lines contain dates `(YYYY-MM-DD)`, punctuation, numbers, and codes that split into
    ~1 token per 3.5 UTF-8 characters (or ~1.08x word/punctuation tokens). We take the conservative upper
    bound of word-piece splitting and char/3.5 so local budget fitting never underestimates router tokens.
    """
    if not text:
        return 0
    pieces = re.findall(r"[A-Za-z]+|\d+|\S", text)
    by_pieces = int(math.ceil(len(pieces) * 1.08))
    by_chars = int(math.ceil(len(text.encode("utf-8")) / 3.5))
    return max(by_pieces, by_chars)


def _date_sort_key(d: Optional[str]) -> str:
    return d if d and re.match(r"^\d{4}-\d{2}-\d{2}$", d) else "0000-00-00"


def _int_date(d: Optional[str]) -> int:
    if not d or not re.match(r"^\d{4}-\d{2}-\d{2}$", d):
        return 0
    return int(d.replace("-", ""))


def _cutoff_iso(reference_date: Optional[str], days: int) -> str:
    if not reference_date:
        return "0000-00-00"
    try:
        ref = date.fromisoformat(reference_date[:10])
        return (ref - timedelta(days=days)).isoformat()
    except ValueError:
        return "0000-00-00"


def _merge_uuids(items: Sequence[ClinicalItem]) -> Tuple[str, ...]:
    out: List[str] = []
    for it in items:
        if it.resource_uuid:
            out.append(it.resource_uuid)
        for m in it.member_uuids:
            if m:
                out.append(m)
    return tuple(dict.fromkeys(out))


def _span_phrase(dates: Sequence[Optional[str]]) -> Tuple[Optional[str], str]:
    valid = sorted({d for d in dates if d and re.match(r"^\d{4}-\d{2}-\d{2}$", d)})
    if not valid:
        return None, ""
    if len(valid) == 1:
        return valid[-1], f"recorded {valid[0]}"
    return valid[-1], f"first recorded {valid[0]} through {valid[-1]}"


def _fold_devices(devices: Sequence[ClinicalItem]) -> List[ClinicalItem]:
    """Fold repeated identical patient devices (e.g. annual CPAP appliance replacements) by (code/display, status)."""
    by_code: Dict[Tuple[str, str], List[ClinicalItem]] = {}
    for d in devices:
        by_code.setdefault((d.code or d.display, d.status), []).append(d)

    folded: List[ClinicalItem] = []
    for (_code, _status), group in by_code.items():
        ordered = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = ordered[-1]
        if len(ordered) == 1:
            folded.append(latest)
            continue
        latest_date, span_str = _span_phrase([x.date for x in ordered])
        folded.append(
            ClinicalItem(
                resource_type="Device",
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section="device",
                code=latest.code,
                display=latest.display,
                text=f"Device: {latest.display} (status: {latest.status}, {len(ordered)} records, {span_str})",
                mandatory=any(x.mandatory for x in ordered),
                status=latest.status,
                source_org=latest.source_org,
                member_uuids=_merge_uuids(ordered),
            )
        )
    return sorted(folded, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_social_history(social_items: Sequence[ClinicalItem]) -> List[ClinicalItem]:
    """Fold repeated social-history observations (e.g. 62 identical Tobacco smoking status assessments)."""
    by_key: Dict[Tuple[str, str], List[ClinicalItem]] = {}
    for s in social_items:
        val = str(s.metadata.get("value") or s.text)
        by_key.setdefault((s.code or s.display, val), []).append(s)

    folded: List[ClinicalItem] = []
    for (_code, _val), group in by_key.items():
        ordered = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = ordered[-1]
        if len(ordered) == 1:
            folded.append(latest)
            continue
        latest_date, span_str = _span_phrase([x.date for x in ordered])
        folded.append(
            ClinicalItem(
                resource_type="Observation",
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section="social_history",
                code=latest.code,
                display=latest.display,
                text=f"{latest.text} ({len(ordered)} assessments, {span_str})",
                mandatory=latest.mandatory,
                status=latest.status,
                sensitivity=latest.sensitivity,
                member_uuids=_merge_uuids(ordered),
                metadata=latest.metadata,
            )
        )
    return sorted(folded, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_encounters(encounters: Sequence[ClinicalItem], *, keep_latest_individual: int = 3) -> List[ClinicalItem]:
    """Keep the `keep_latest_individual` most recent high-acuity encounters unpacked and fold older
    repeated encounters by (display, class)."""
    ordered_desc = sorted(encounters, key=lambda x: _date_sort_key(x.date), reverse=True)
    kept = list(ordered_desc[:keep_latest_individual])
    older = ordered_desc[keep_latest_individual:]
    if not older:
        return kept

    by_type: Dict[str, List[ClinicalItem]] = {}
    for e in older:
        by_type.setdefault(e.display or e.code, []).append(e)

    folded_older: List[ClinicalItem] = []
    for _disp, group in by_type.items():
        chron = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = chron[-1]
        if len(chron) == 1:
            folded_older.append(latest)
            continue
        latest_date, span_str = _span_phrase([x.date for x in chron])
        orgs = sorted({x.source_org for x in chron if x.source_org})
        org_str = f", facility: {orgs[0]}" if orgs else ""
        folded_older.append(
            ClinicalItem(
                resource_type="Encounter",
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section=latest.section,
                code=latest.code,
                display=latest.display,
                text=(
                    f"Encounter History: {latest.display} — {len(chron)} visits "
                    f"({span_str}, class: {latest.code}{org_str})"
                ),
                mandatory=False,
                status=latest.status,
                source_org=latest.source_org,
                is_routine=latest.is_routine,
                member_uuids=_merge_uuids(chron),
            )
        )
    return kept + sorted(folded_older, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_lab_panels_by_type(
    panels: Sequence[ClinicalItem], *, keep_latest_per_type: int = 2
) -> List[ClinicalItem]:
    """Keep the `keep_latest_per_type` most recent panels of each panel code in full, and fold older
    panels of the same code into a single historical summary line carrying all constituent UUIDs."""
    by_code: Dict[str, List[ClinicalItem]] = {}
    for p in panels:
        by_code.setdefault(p.code or p.display, []).append(p)

    out: List[ClinicalItem] = []
    for _code, group in by_code.items():
        ordered_desc = sorted(group, key=lambda x: _date_sort_key(x.date), reverse=True)
        out.extend(ordered_desc[:keep_latest_per_type])
        older = ordered_desc[keep_latest_per_type:]
        if not older:
            continue
        chron = list(reversed(older))
        latest_older = chron[-1]
        l_date, span_str = _span_phrase([x.date for x in chron])
        out.append(
            ClinicalItem(
                resource_type=latest_older.resource_type,
                resource_uuid=latest_older.resource_uuid,
                date=l_date or latest_older.date,
                clinical_date=l_date or latest_older.clinical_date,
                date_kind="clinical_event",
                section=latest_older.section,
                code=latest_older.code,
                display=latest_older.display,
                text=(
                    f"Diagnostic Panel History — {latest_older.display}: "
                    f"{len(chron)} prior panels ({span_str})"
                ),
                mandatory=False,
                status=latest_older.status,
                source_org=latest_older.source_org,
                sensitivity=latest_older.sensitivity,
                is_routine=latest_older.is_routine,
                member_uuids=_merge_uuids(chron),
            )
        )
    return sorted(out, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_stopped_medications(meds: Sequence[ClinicalItem]) -> List[ClinicalItem]:
    """Fold historical/stopped prescriptions by normalized ingredient while keeping setting distinct."""
    by_key: Dict[Tuple[str, str], List[ClinicalItem]] = {}
    for m in meds:
        setting = str(m.metadata.get("setting") or "")
        key = (m.ingredient_key or m.code or m.display.lower(), setting)
        by_key.setdefault(key, []).append(m)

    folded: List[ClinicalItem] = []
    for (_ing, setting), group in by_key.items():
        ordered = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = ordered[-1]
        if len(ordered) == 1:
            folded.append(latest)
            continue
        latest_date, span_str = _span_phrase([x.date for x in ordered])
        reasons: List[str] = []
        for x in ordered:
            for r in x.metadata.get("reasons") or []:
                if r:
                    reasons.append(r)
        reason_str = f", reason: {', '.join(dict.fromkeys(reasons))}" if reasons else ""
        folded.append(
            ClinicalItem(
                resource_type=latest.resource_type,
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section="med_historical",
                code=latest.code,
                display=latest.display,
                text=(
                    f"Medication History: {latest.display} — {len(ordered)} records "
                    f"({span_str}, status: {latest.status}, type: {setting}{reason_str})"
                ),
                mandatory=False,
                status=latest.status,
                source_org=latest.source_org,
                ingredient_key=latest.ingredient_key,
                member_uuids=_merge_uuids(ordered),
                metadata={"count": len(ordered), "reasons": list(dict.fromkeys(reasons)), "setting": setting},
            )
        )
    return sorted(folded, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_resolved_conditions(conds: Sequence[ClinicalItem]) -> List[ClinicalItem]:
    """Run-length fold resolved conditions by code/display while preserving sensitivity and conflict tags."""
    anomalous = [c for c in conds if c.conflict_note]
    routine = [c for c in conds if not c.conflict_note]

    by_code: Dict[str, List[ClinicalItem]] = {}
    for c in routine:
        by_code.setdefault(c.code or c.display, []).append(c)

    folded: List[ClinicalItem] = list(anomalous)
    for _code, group in by_code.items():
        ordered = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = ordered[-1]
        if len(ordered) == 1:
            folded.append(latest)
            continue
        all_dates: List[Optional[str]] = []
        for x in ordered:
            all_dates.append(x.metadata.get("onset") or x.date)
            if x.metadata.get("abatement"):
                all_dates.append(x.metadata.get("abatement"))
        latest_date, span_str = _span_phrase(all_dates)
        sens_suffix = (
            " [sensitivity: SDOH/PRIVACY-SENSITIVE]"
            if latest.sensitivity == "SDOH/PRIVACY-SENSITIVE"
            else ""
        )
        folded.append(
            ClinicalItem(
                resource_type="Condition",
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section="condition_resolved",
                code=latest.code,
                display=latest.display,
                text=(
                    f"Condition History: {latest.display} — {len(ordered)} episodes "
                    f"({span_str}, status: {latest.status}, SNOMED: {latest.code}){sens_suffix}"
                ),
                mandatory=False,
                status=latest.status,
                source_org=latest.source_org,
                sensitivity=latest.sensitivity,
                is_routine=latest.is_routine,
                member_uuids=_merge_uuids(ordered),
                metadata={"count": len(ordered)},
            )
        )
    return sorted(folded, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_immunizations(imms: Sequence[ClinicalItem]) -> List[ClinicalItem]:
    """Keep all anomalous/duplicate immunizations as individual lines; fold routine multi-dose series."""
    anomalous = [x for x in imms if x.conflict_note]
    routine = [x for x in imms if not x.conflict_note]

    by_group: Dict[str, List[ClinicalItem]] = {}
    for x in routine:
        by_group.setdefault(x.ingredient_key or x.display, []).append(x)

    folded_routine: List[ClinicalItem] = []
    for _grp, group in by_group.items():
        ordered = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = ordered[-1]
        if len(ordered) <= 2:
            folded_routine.extend(ordered)
            continue
        latest_date, span_str = _span_phrase([x.date for x in ordered])
        orgs = sorted({x.source_org for x in ordered if x.source_org})
        org_str = f", sources: {'; '.join(orgs[:2])}" if orgs else ""
        folded_routine.append(
            ClinicalItem(
                resource_type="Immunization",
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section="immunization",
                code=latest.code,
                display=latest.display,
                text=(
                    f"Immunization Series: {latest.display} — {len(ordered)} doses "
                    f"({span_str}, status: completed, CVX: {latest.code}{org_str})"
                ),
                mandatory=False,
                status=latest.status,
                source_org=latest.source_org,
                ingredient_key=latest.ingredient_key,
                member_uuids=_merge_uuids(ordered),
            )
        )
    combined = anomalous + folded_routine
    return sorted(combined, key=lambda x: _date_sort_key(x.date), reverse=True)


def _fold_procedures(procs: Sequence[ClinicalItem], *, section: str) -> List[ClinicalItem]:
    """Fold repeated procedures by code/display."""
    by_code: Dict[str, List[ClinicalItem]] = {}
    for p in procs:
        by_code.setdefault(p.code or p.display, []).append(p)

    folded: List[ClinicalItem] = []
    for _code, group in by_code.items():
        ordered = sorted(group, key=lambda x: _date_sort_key(x.date))
        latest = ordered[-1]
        if len(ordered) == 1:
            folded.append(latest)
            continue
        latest_date, span_str = _span_phrase([x.date for x in ordered])
        folded.append(
            ClinicalItem(
                resource_type="Procedure",
                resource_uuid=latest.resource_uuid,
                date=latest_date or latest.date,
                clinical_date=latest_date or latest.clinical_date,
                date_kind="clinical_event",
                section=section,
                code=latest.code,
                display=latest.display,
                text=f"Procedure History: {latest.display} — {len(ordered)} times ({span_str})",
                mandatory=False,
                status=latest.status,
                is_routine=(section == "procedure_routine"),
                member_uuids=_merge_uuids(ordered),
            )
        )
    return sorted(folded, key=lambda x: _date_sort_key(x.date), reverse=True)


def _group_vitals_by_date(vitals: Sequence[ClinicalItem]) -> List[ClinicalItem]:
    """Group standalone Vital Sign observations on the same date into a single Vital Signs Panel line.

    Preserves distinct intraday readings for the same vital sign when values or timestamps differ.
    """
    by_date: Dict[str, List[ClinicalItem]] = {}
    for v in vitals:
        by_date.setdefault(v.date or "unknown", []).append(v)

    grouped: List[ClinicalItem] = []
    for d, items_on_date in sorted(by_date.items(), reverse=True):
        if len(items_on_date) == 1:
            grouped.append(items_on_date[0])
            continue
        values_by_label: Dict[str, List[str]] = {}
        for it in items_on_date:
            val = str(it.metadata.get("value") or "").strip()
            ts = str(it.metadata.get("timestamp") or "").strip()
            label = it.display
            if not val:
                continue
            formatted_val = f"{val} @{ts[11:16]}" if (ts and "T" in ts) else val
            bucket = values_by_label.setdefault(label, [])
            if formatted_val not in bucket:
                bucket.append(formatted_val)
        parts = [
            f"{label}: {' -> '.join(vals)}"
            for label, vals in values_by_label.items()
        ]
        lead = items_on_date[0]
        grouped.append(
            ClinicalItem(
                resource_type="Observation",
                resource_uuid=lead.resource_uuid,
                date=lead.date,
                clinical_date=lead.clinical_date,
                date_kind="clinical_event",
                section="vital_sign",
                code="vital-signs-panel",
                display="Vital Signs Panel",
                text=f"Vital Signs Panel: {'; '.join(parts)}",
                mandatory=False,
                status="final",
                is_routine=True,
                member_uuids=_merge_uuids(items_on_date),
            )
        )
    return grouped


def _canonical_analyte_name(raw: str) -> str:
    """Normalize verbose LOINC display names into clean, clinical analyte names."""
    s = re.sub(r"\s*\([^)]*\)\s*$", "", (raw or "")).strip() or raw
    low = s.lower()
    if "glomerular filtration rate" in low:
        return "Glomerular filtration rate (eGFR)"
    if "hemoglobin a1c" in low:
        return "Hemoglobin A1c (HbA1c)"
    if "cholesterol in ldl" in low:
        return "LDL Cholesterol"
    if "cholesterol in hdl" in low:
        return "HDL Cholesterol"
    if low == "cholesterol [mass/volume] in serum or plasma":
        return "Total Cholesterol"
    if "triglyceride" in low and ("serum" in low or "blood" in low):
        return "Triglycerides"
    if "microalbumin/creatinine" in low or "albumin/creatinine" in low:
        return "Urine Microalbumin/Creatinine Ratio"
    # Merge identical blood vs serum/plasma electrolyte/glucose/BUN series from BMP vs BMP-2000
    for chem in ("glucose", "potassium", "sodium", "urea nitrogen", "calcium", "chloride", "carbon dioxide"):
        if low.startswith(chem) and ("in blood" in low or "in serum or plasma" in low) and "urine" not in low:
            return f"{chem.title()} (Blood/Serum)"
    return s


def _build_lab_trajectories(
    numeric_points: Sequence[NumericPoint],
    *,
    max_analytes: Optional[int] = None,
) -> List[ClinicalItem]:
    """Collapse numeric Observation series into 1 longitudinal LabTrajectory record per (analyte, unit).

    Includes `(initial <val> on <date> -> latest <val> on <date>; ...)` so `temporal.py`
    extracts both endpoints (`first` and `latest`), intermediate min/max extrema, computes
    `trend_supported = True`, and validates trend claims deterministically.
    """
    by_concept_unit: Dict[Tuple[str, str], List[NumericPoint]] = {}
    units_per_concept: Dict[str, Set[str]] = {}
    for pt in numeric_points:
        clean_name = _canonical_analyte_name(pt.concept)
        norm_unit = (pt.unit or "").strip()
        by_concept_unit.setdefault((clean_name, norm_unit), []).append(pt)
        units_per_concept.setdefault(clean_name, set()).add(norm_unit)

    def analyte_priority(key: Tuple[str, str]) -> Tuple[int, int, str, str]:
        name, unit_key = key
        lower = name.lower()
        for idx, pat in enumerate(_PRIORITY_ANALYTE_PATTERNS):
            if re.search(pat, lower):
                return (0, idx, lower, unit_key.lower())
        return (1, 999, lower, unit_key.lower())

    trajectories: List[ClinicalItem] = []
    sorted_keys = sorted(by_concept_unit.keys(), key=analyte_priority)
    if max_analytes is not None:
        sorted_keys = sorted_keys[:max_analytes]

    def fmt(v: float) -> str:
        if v.is_integer():
            return str(int(v))
        s = f"{v:.6f}".rstrip("0").rstrip(".")
        return s if s not in {"", "-0"} else "0"

    for clean_name, norm_unit in sorted_keys:
        pts = sorted(by_concept_unit[(clean_name, norm_unit)], key=lambda p: p.date)
        dedup_pts: List[NumericPoint] = []
        seen_date_vals: Set[Tuple[str, float]] = set()
        for p in pts:
            dv_key = (p.date, p.value)
            if dv_key not in seen_date_vals:
                seen_date_vals.add(dv_key)
                dedup_pts.append(p)
        if not dedup_pts:
            continue
        latest = dedup_pts[-1]
        first = dedup_pts[0]
        latest_day = (latest.date or "")[:10]
        first_day = (first.date or "")[:10]
        p_min = min(dedup_pts, key=lambda p: p.value)
        p_max = max(dedup_pts, key=lambda p: p.value)
        min_day = (p_min.date or "")[:10]
        max_day = (p_max.date or "")[:10]
        unit = norm_unit or latest.unit

        concept = (
            f"{clean_name} [{unit or 'unitless'}]"
            if len(units_per_concept.get(clean_name, set())) > 1
            else clean_name
        )

        unit_sp = f" {unit}" if unit else ""
        if len(dedup_pts) == 1:
            text = f"Finding — {concept}: {fmt(latest.value)}{unit_sp} (recorded {latest_day}, n=1)"
        else:
            if latest.value > first.value:
                direction = "up"
            elif latest.value < first.value:
                direction = "down"
            else:
                direction = "flat"
            text = (
                f"Finding — {concept}: {fmt(latest.value)}{unit_sp} "
                f"(initial {fmt(first.value)} on {first_day} -> latest {fmt(latest.value)} on {latest_day}; "
                f"range {fmt(p_min.value)} on {min_day} to {fmt(p_max.value)} on {max_day}{unit_sp} "
                f"over n={len(dedup_pts)} measurements, "
                f"first recorded {first_day} through {latest_day}, trend: {direction})"
            )
        member_uuids = tuple(dict.fromkeys(p.resource_uuid for p in pts if p.resource_uuid))
        p_group, p_idx, _, _ = analyte_priority((clean_name, norm_unit))
        trajectories.append(
            ClinicalItem(
                resource_type="Observation",
                resource_uuid=latest.resource_uuid,
                date=latest_day,
                clinical_date=latest_day,
                date_kind="clinical_event",
                section="lab_trajectory",
                code=latest.code,
                display=concept,
                text=text,
                mandatory=False,
                status="final",
                member_uuids=member_uuids,
                metadata={"n_points": len(dedup_pts), "analyte_rank": p_group * 100 + p_idx},
            )
        )
    return trajectories


def _build_completeness_manifest(
    extracted: ExtractedChart,
    tier: int,
    tier_name: str,
    included_items: Sequence[ClinicalItem],
    redacted_count: int,
) -> Tuple[ClinicalItem, Dict[str, Any]]:
    """Build the mandatory `[Completeness Manifest]` item and structured manifest dictionary."""
    ex_str = ", ".join(
        f"{k}={v}" for k, v in sorted(extracted.excluded_resource_counts.items())
    )
    redacted_str = f"; privacy_redacted={redacted_count}" if redacted_count else ""
    dedup_str = (
        f"; base64_duplicate_notes_merged={extracted.deduplicated_note_count}"
        if extracted.deduplicated_note_count
        else ""
    )

    covered_uuids: Set[str] = set()
    for it in included_items:
        if it.resource_uuid:
            covered_uuids.add(it.resource_uuid)
        if it.section != "lab_trajectory":
            for m in it.member_uuids:
                if m:
                    covered_uuids.add(m)

    omitted_by_section: Dict[str, int] = {}
    for src_it in extracted.items:
        if src_it.section == "patient":
            continue
        item_uuids = {src_it.resource_uuid} | set(src_it.member_uuids)
        if not (item_uuids & covered_uuids):
            omitted_by_section[src_it.section] = omitted_by_section.get(src_it.section, 0) + 1
    omitted_clinical_count = max(0, sum(omitted_by_section.values()) - redacted_count)
    omitted_str = (
        f"; omitted_clinical_items={omitted_clinical_count}"
        if omitted_clinical_count > 0
        else ""
    )

    if tier == 0 and omitted_clinical_count == 0:
        coverage = "ALL clinical sections COMPLETE (lossless 1-line projection)"
    elif tier == 0:
        coverage = (
            "IPS Safety Core and structured clinical ledger COMPLETE; "
            f"{omitted_clinical_count} clinical item(s) omitted by mode/budget filter"
        )
    elif tier == 1:
        coverage = (
            "IPS Safety Core (Allergies, Active Conditions, Active Medications, Anomalous Immunizations, "
            "Devices) COMPLETE; historical renewals, resolved conditions, repeated panels, and routine "
            "screenings RUN-LENGTH FOLDED with full date spans and UUIDs"
        )
    elif tier == 2:
        coverage = (
            "IPS Safety Core COMPLETE; Diagnostics/Vitals/Notes WINDOWED to recent horizon + "
            "longitudinal analyte trajectories + folded historical spans"
        )
    elif tier == 3:
        coverage = (
            "IPS Safety Core COMPLETE; Labs & Vitals condensed to longitudinal per-analyte trajectories "
            "(initial/latest/min/max/trend); routine notes & outpatient visits summarized"
        )
    else:
        coverage = (
            "IPS Micro-Capsule: Safety Core (Allergies, Active Conditions, Active Medications, Key Alerts) "
            "COMPLETE; historical labs/visits condensed to key trajectories and surgical/inpatient summary"
        )

    manifest_text = (
        f"Completeness Manifest: Tier {tier} ({tier_name}) | {coverage} | "
        f"bundle_entries={extracted.total_bundle_entries}, rendered_lines={len(included_items) + 1}"
        f"{dedup_str}{redacted_str}{omitted_str}"
        + (f" | non_clinical_excluded: {ex_str}" if ex_str else "")
    )
    if tier == 4 and "Ultra-Compact" in tier_name:
        manifest_text = (
            f"Completeness Manifest: Tier 4 ({tier_name}) | IPS Safety Core COMPLETE | "
            f"entries={extracted.total_bundle_entries}, lines={len(included_items) + 1}{redacted_str}{omitted_str}"
        )

    manifest_item = ClinicalItem(
        resource_type="Bundle",
        resource_uuid=f"manifest-{extracted.patient_id or 'chart'}-t{tier}",
        date=extracted.reference_date,
        clinical_date=None,
        date_kind="administrative",
        section="manifest",
        code="completeness-manifest",
        display="Completeness Manifest",
        text=manifest_text,
        mandatory=True,
        status="current",
    )
    manifest_dict = {
        "tier": tier,
        "tier_name": tier_name,
        "coverage": coverage,
        "total_bundle_entries": extracted.total_bundle_entries,
        "rendered_records": len(included_items) + 1,
        "deduplicated_note_count": extracted.deduplicated_note_count,
        "excluded_non_clinical": dict(extracted.excluded_resource_counts),
        "redacted_for_privacy_count": redacted_count,
        "omitted_clinical_items_count": omitted_clinical_count,
        "omitted_by_section": omitted_by_section,
        "omitted_section_counts": dict(omitted_by_section),
    }
    return manifest_item, manifest_dict


_HOLD_CLAUSE_RE = re.compile(
    r"\b(?:hold|stop|discontinue|held|stopped|discontinued|do\s+not\s+take)\s+[a-z][a-z0-9-]{2,25}\b",
    re.IGNORECASE,
)


def _extract_note_conflict_clauses(text: str, max_chars: int = 160) -> str:
    clauses = [
        c.strip()
        for c in re.split(r"[.;|\n]+", text or "")
        if c.strip() and _HOLD_CLAUSE_RE.search(c)
    ]
    if not clauses:
        return (text or "").strip()[:max_chars]
    joined = "; ".join(clauses)
    return joined[:max_chars] + ("..." if len(joined) > max_chars else "")


def _excerpt_notes(notes: Sequence[ClinicalItem], max_chars: int = 400) -> List[ClinicalItem]:
    """Excerpt clinical notes to `max_chars` for condensed Tiers 1..4 while keeping Tier 0 lossless."""
    out: List[ClinicalItem] = []
    for n in notes:
        full_norm = str(n.metadata.get("full_text") or n.text or "")
        title = str(n.metadata.get("title") or n.display or "Clinical Note")
        conf_suffix = f" [{n.conflict_note}]" if n.conflict_note else ""
        sens_suffix = (
            f" [sensitivity: {n.sensitivity}]"
            if n.sensitivity == "SDOH/PRIVACY-SENSITIVE"
            else ""
        )
        if full_norm and len(full_norm) > max_chars:
            if n.conflict_note:
                c_clause = _extract_note_conflict_clauses(full_norm, max_chars=min(200, max_chars))
                head_budget = max(60, max_chars - len(c_clause) - 6)
                head = full_norm[:head_budget].strip()
                if c_clause and c_clause.lower() not in head.lower():
                    excerpt = f"{head} ... {c_clause}"
                else:
                    excerpt = full_norm[:max_chars] + "..."
            else:
                excerpt = full_norm[:max_chars] + "..."
            txt = f"Note ({title}): {excerpt}{conf_suffix}{sens_suffix}"
        else:
            txt = n.text
            if n.conflict_note and f"[{n.conflict_note}]" not in txt:
                txt = f"{txt}{conf_suffix}"
        out.append(
            ClinicalItem(
                resource_type=n.resource_type,
                resource_uuid=n.resource_uuid,
                date=n.date,
                clinical_date=n.clinical_date,
                date_kind=n.date_kind,
                section=n.section,
                code=n.code,
                display=n.display,
                text=txt,
                mandatory=n.mandatory,
                status=n.status,
                end_date=n.end_date,
                source_org=n.source_org,
                sensitivity=n.sensitivity,
                conflict_note=n.conflict_note,
                ingredient_key=n.ingredient_key,
                is_routine=n.is_routine,
                member_uuids=n.member_uuids,
                metadata=n.metadata,
            )
        )
    return out


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


def items_to_querystore_records(items: Sequence[ClinicalItem]) -> List[Dict[str, Any]]:
    """Convert `ClinicalItem` objects into the exact dict schema expected by `chart_serializer.render_chart`."""
    records: List[Dict[str, Any]] = []
    for it in items:
        meta = dict(it.metadata)
        if it.mandatory:
            meta["mandatory_context"] = True
        if it.member_uuids:
            meta["memberUuids"] = list(it.member_uuids)
        if it.sensitivity:
            meta["sensitivity"] = it.sensitivity
        if it.source_org:
            meta["source_org"] = it.source_org
        if it.conflict_note:
            meta["conflict_note"] = it.conflict_note
        records.append(
            {
                "resourceType": it.resource_type,
                "resourceUuid": it.resource_uuid,
                "date": it.date,
                "clinicalDate": it.clinical_date,
                "dateKind": it.date_kind,
                "text": it.text,
                "metadata": meta,
            }
        )
    return records


def _pack_items_to_token_budget(
    extracted: ExtractedChart,
    items: Sequence[ClinicalItem],
    redacted: List[Dict[str, Any]],
    *,
    tier: int,
    tier_label: str,
    target_tokens: int,
    question: str,
    token_counter: Callable[[str], int],
) -> Tuple[List[ClinicalItem], Dict[str, Any], str, List[Dict[str, Any]], int]:
    """Pack optional items below mandatory IPS safety core so rendered chart tokens <= target_tokens."""
    q_terms = {
        tok.lower()
        for tok in re.findall(r"[A-Za-z0-9]+", question or "")
        if len(tok) >= 3
    }

    mandatory_items = [it for it in items if it.mandatory and it.section != "manifest"]
    optional_items = [it for it in items if not it.mandatory and it.section != "manifest"]

    # Rank optional items by: (1) query keyword overlap, (2) section clinical priority, (3) recency
    # LabTrajectory is ranked #1 among optional items so key numeric series (eGFR, HbA1c, BP, Weight)
    # are always present even in tight 1,024-token capsules.
    section_rank = {
        "lab_trajectory": 1,
        "med_historical": 2,
        "procedure_major": 3,
        "encounter_acuity": 4,
        "immunization": 5,
        "lab_panel": 6,
        "lab_observation": 7,
        "condition_resolved": 8,
        "vital_sign": 9,
        "careplan_active": 10,
        "social_history": 11,
        "clinical_note": 12,
        "survey_panel": 13,
        "imaging": 14,
        "procedure_routine": 15,
        "encounter_routine": 16,
    }

    ranked_optional = sorted(
        optional_items,
        key=lambda it: (
            -sum(1 for t in q_terms if t in f"{it.display} {it.text}".lower()),
            section_rank.get(it.section, 20),
            0 if int(it.metadata.get("n_points", 0)) >= 2 else 1,
            int(it.metadata.get("analyte_rank", 999)),
            -_int_date(it.date),
        ),
    )

    def render_candidate(
        cand_body: Sequence[ClinicalItem],
    ) -> Tuple[List[ClinicalItem], Dict[str, Any], str, List[Dict[str, Any]], int]:
        manifest_item, manifest_dict = _build_completeness_manifest(
            extracted, tier, tier_label, cand_body, len(redacted)
        )
        if cand_body and cand_body[0].section == "patient":
            ordered = [cand_body[0], manifest_item] + list(cand_body[1:])
        else:
            ordered = [manifest_item] + list(cand_body)
        recs = items_to_querystore_records(ordered)
        c_text, c_maps = render_chart(recs)
        toks = token_counter(c_text)
        return ordered, manifest_dict, c_text, c_maps, toks

    # Reserve at least ~280 tokens for top optional trajectories/history when target_tokens <= 1200
    mandatory_ceiling = int(target_tokens * 0.68) if target_tokens <= 1200 else target_tokens
    _, _, _, _, mand_tokens = render_candidate(mandatory_items)
    if mand_tokens > mandatory_ceiling:
        patient_only = [it for it in mandatory_items if it.section == "patient"]
        allergy_only = [it for it in mandatory_items if it.section == "allergy"]
        device_only = [it for it in mandatory_items if it.section == "device"]
        conflict_conds = [
            it
            for it in mandatory_items
            if it.section in {"condition_active", "condition_resolved"}
            and (it.conflict_note or it.sensitivity == "SDOH/PRIVACY-SENSITIVE")
        ]
        conflict_imms = [
            it
            for it in mandatory_items
            if it.section == "immunization" and it.conflict_note
        ]
        conflict_notes = [
            it
            for it in mandatory_items
            if it.section in {"clinical_note", "note"}
        ]
        other_conds = [
            it
            for it in mandatory_items
            if it.section == "condition_active" and it not in conflict_conds
        ]
        other_meds = [
            it
            for it in mandatory_items
            if it.section == "med_active"
        ]
        compact_conflict_notes: List[ClinicalItem] = []
        for cn in conflict_notes:
            raw_nt = str(cn.metadata.get("full_text") or cn.text or "")
            clause_str = _extract_note_conflict_clauses(raw_nt, max_chars=120)
            c_tag = f" [{cn.conflict_note}]" if cn.conflict_note else ""
            compact_conflict_notes.append(
                ClinicalItem(
                    resource_type=cn.resource_type,
                    resource_uuid=cn.resource_uuid,
                    date=cn.date,
                    clinical_date=cn.clinical_date,
                    date_kind=cn.date_kind,
                    section=cn.section,
                    code=cn.code,
                    display=cn.display,
                    text=f"Note ({cn.display or 'Clinical Note'}): {clause_str}{c_tag}",
                    mandatory=True,
                    status=cn.status,
                    conflict_note=cn.conflict_note,
                    member_uuids=cn.member_uuids,
                    metadata=cn.metadata,
                )
            )
        compressed_mandatory: List[ClinicalItem] = list(
            patient_only + allergy_only + compact_conflict_notes
        )
        # Group conflict conditions that share the exact same conflict_note (e.g. CKD stages 1, 2, 3)
        by_conflict_key: Dict[str, List[ClinicalItem]] = {}
        for cc in conflict_conds:
            key = cc.conflict_note or f"sens:{cc.resource_uuid}"
            by_conflict_key.setdefault(key, []).append(cc)
        for _ckey, cgroup in by_conflict_key.items():
            lead = cgroup[-1]
            l_date, _ = _span_phrase([x.date for x in cgroup])
            has_mixed_status = any((x.status or "active") != "active" for x in cgroup)
            if has_mixed_status:
                names = "; ".join(
                    f"{x.display} ({x.date}, status: {x.status or 'active'})" for x in cgroup
                )
                cond_prefix = "Condition:"
            else:
                names = "; ".join(f"{x.display} ({x.date})" for x in cgroup)
                cond_prefix = "Condition (status: active):"
            sens_tag = (
                " [sensitivity: SDOH/PRIVACY-SENSITIVE]"
                if any(x.sensitivity == "SDOH/PRIVACY-SENSITIVE" for x in cgroup)
                else ""
            )
            note_tag = f" [{lead.conflict_note}]" if lead.conflict_note else ""
            compressed_mandatory.append(
                ClinicalItem(
                    resource_type="Condition",
                    resource_uuid=lead.resource_uuid,
                    date=l_date or lead.date,
                    clinical_date=l_date or lead.clinical_date,
                    date_kind="clinical_event",
                    section="condition_active",
                    code=lead.code,
                    display=lead.display,
                    text=f"{cond_prefix} {names}{note_tag}{sens_tag}",
                    mandatory=True,
                    status=lead.status,
                    sensitivity=lead.sensitivity,
                    conflict_note=lead.conflict_note,
                    member_uuids=_merge_uuids(cgroup),
                )
            )
        for ci in conflict_imms:
            compressed_mandatory.append(
                ClinicalItem(
                    resource_type="Immunization",
                    resource_uuid=ci.resource_uuid,
                    date=ci.date,
                    clinical_date=ci.clinical_date,
                    date_kind="clinical_event",
                    section="immunization",
                    code=ci.code,
                    display=ci.display,
                    text=f"Immunization: {ci.display} [{ci.conflict_note}]",
                    mandatory=True,
                    conflict_note=ci.conflict_note,
                    member_uuids=ci.member_uuids,
                )
            )
        if other_conds:
            l_date, _ = _span_phrase([c.date for c in other_conds])
            c_names = "; ".join(f"{c.display} ({c.date})" for c in other_conds)
            compressed_mandatory.append(
                ClinicalItem(
                    resource_type="Condition",
                    resource_uuid=other_conds[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="condition_active",
                    code="active-conditions-folded",
                    display="Active Conditions",
                    text=f"Condition (status: active): {c_names}",
                    mandatory=True,
                    member_uuids=_merge_uuids(other_conds),
                )
            )
        if other_meds:
            l_date, span_str = _span_phrase([m.date for m in other_meds])
            m_parts: List[str] = []
            for m in other_meds:
                dm = re.search(r"dosage:\s*([^,\)\]]+)", m.text)
                dosage_str = dm.group(1).strip() if dm else ""
                quals: List[str] = []
                if m.date:
                    quals.append(m.date)
                if m.status and m.status != "active":
                    quals.append(f"status: {m.status}")
                if dosage_str:
                    quals.append(f"dosage: {dosage_str}")
                q_str = f" ({', '.join(quals)})" if quals else ""
                c_str = f" [{m.conflict_note}]" if m.conflict_note else ""
                m_parts.append(f"{m.display}{q_str}{c_str}")
            compressed_mandatory.append(
                ClinicalItem(
                    resource_type=other_meds[0].resource_type,
                    resource_uuid=other_meds[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="med_active",
                    code="active-meds-folded",
                    display="Active Medications",
                    text=f"Medication (status: active): {'; '.join(m_parts)}",
                    mandatory=True,
                    member_uuids=_merge_uuids(other_meds),
                )
            )
        if device_only:
            l_date, _ = _span_phrase([d.date for d in device_only])
            d_names = "; ".join(d.display for d in device_only)
            compressed_mandatory.append(
                ClinicalItem(
                    resource_type="Device",
                    resource_uuid=device_only[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="device",
                    code="active-devices-folded",
                    display="Active Devices",
                    text=f"Device (status: active): {d_names}",
                    mandatory=True,
                    member_uuids=_merge_uuids(device_only),
                )
            )
        _, _, _, _, mand_tokens_after = render_candidate(compressed_mandatory)
        if mand_tokens_after > mandatory_ceiling:
            tier_label = "IPS Ultra-Compact Micro-Capsule"
            raw_all_conds = conflict_conds + other_conds
            all_imms = [it for it in compressed_mandatory if it.section == "immunization"]
            non_cond_imm_med = [
                it
                for it in compressed_mandatory
                if it.section not in {"condition_active", "immunization", "med_active", "clinical_note", "note"}
            ]
            ultra: List[ClinicalItem] = list(non_cond_imm_med)
            if conflict_notes:
                l_date, _ = _span_phrase([cn.date for cn in conflict_notes])
                per_note_chars = max(24, min(90, 220 // max(1, len(conflict_notes))))
                n_parts: List[str] = []
                for cn in conflict_notes:
                    raw_nt = str(cn.metadata.get("full_text") or cn.text or "")
                    c_clause = _extract_note_conflict_clauses(raw_nt, max_chars=per_note_chars)
                    n_parts.append(f"{cn.date or 'note'}: {c_clause}")
                lead_cn = conflict_notes[0]
                c_tag = f" [{lead_cn.conflict_note}]" if lead_cn.conflict_note else ""
                ultra.append(
                    ClinicalItem(
                        resource_type=lead_cn.resource_type,
                        resource_uuid=lead_cn.resource_uuid,
                        date=l_date or lead_cn.date,
                        clinical_date=l_date or lead_cn.clinical_date,
                        date_kind="clinical_event",
                        section="clinical_note",
                        code="conflict-notes-ultra",
                        display="Clinical Note Alerts",
                        text=f"Clinical Note Alerts: {'; '.join(n_parts)}{c_tag}",
                        mandatory=True,
                        conflict_note=lead_cn.conflict_note,
                        member_uuids=_merge_uuids(conflict_notes),
                    )
                )
            if raw_all_conds:
                l_date, _ = _span_phrase([c.date for c in raw_all_conds])
                has_non_active_cond = any((c.status or "active") != "active" for c in raw_all_conds)
                c_parts = []
                for c in raw_all_conds:
                    tag = f" [{c.conflict_note}]" if c.conflict_note else ""
                    sens = " [SDOH/PRIVACY-SENSITIVE]" if c.sensitivity == "SDOH/PRIVACY-SENSITIVE" else ""
                    st_str = (
                        f", status: {c.status or 'active'}"
                        if has_non_active_cond
                        else ""
                    )
                    c_parts.append(f"{c.display} ({c.date}{st_str}){tag}{sens}")
                ultra_cond_prefix = "Condition:" if has_non_active_cond else "Condition (status: active):"
                ultra.append(
                    ClinicalItem(
                        resource_type="Condition",
                        resource_uuid=raw_all_conds[0].resource_uuid,
                        date=l_date or extracted.reference_date,
                        clinical_date=l_date or extracted.reference_date,
                        date_kind="clinical_event",
                        section="condition_active",
                        code="active-conditions-ultra",
                        display="Active Conditions",
                        text=f"{ultra_cond_prefix} {'; '.join(c_parts)}",
                        mandatory=True,
                        member_uuids=_merge_uuids(raw_all_conds),
                    )
                )
            if other_meds:
                l_date, _ = _span_phrase([m.date for m in other_meds])
                by_med_qual: Dict[Tuple[str, str, str], List[ClinicalItem]] = {}
                for m in other_meds:
                    dm = re.search(r"dosage:\s*([^,\)\]]+)", m.text)
                    dosage_str = dm.group(1).strip() if dm else ""
                    key_q = (m.status or "active", dosage_str, m.conflict_note or "")
                    by_med_qual.setdefault(key_q, []).append(m)

                def _build_ultra_med_text(include_dosage: bool, max_med_names: Optional[int] = None) -> str:
                    group_strs: List[str] = []
                    for (st, dos, conf), mgroup in by_med_qual.items():
                        _, g_span = _span_phrase([x.date for x in mgroup])
                        short_names = [
                            re.sub(
                                r"\s+Oral\s+(?:Tablet|Capsule|Solution)s?\b",
                                "",
                                x.display,
                                flags=re.IGNORECASE,
                            ).strip()
                            for x in mgroup
                        ]
                        if max_med_names is not None and len(short_names) > max_med_names:
                            omitted_n = len(short_names) - max_med_names
                            names_joined = f"{', '.join(short_names[:max_med_names])} (+{omitted_n} more)"
                        else:
                            names_joined = ", ".join(short_names)
                        quals: List[str] = []
                        if g_span:
                            quals.append(g_span)
                        if st and st != "active":
                            quals.append(f"status: {st}")
                        if include_dosage and dos:
                            quals.append(f"dosage: {dos}")
                        q_str = f" ({', '.join(quals)})" if quals else ""
                        c_str = f" [{conf}]" if conf else ""
                        group_strs.append(f"{names_joined}{q_str}{c_str}")
                    return f"Medication (status: active): {'; '.join(group_strs)}"

                med_ultra_text = _build_ultra_med_text(include_dosage=True)
                med_ultra_item = ClinicalItem(
                    resource_type=other_meds[0].resource_type,
                    resource_uuid=other_meds[0].resource_uuid,
                    date=l_date or extracted.reference_date,
                    clinical_date=l_date or extracted.reference_date,
                    date_kind="clinical_event",
                    section="med_active",
                    code="active-meds-ultra",
                    display="Active Medications",
                    text=med_ultra_text,
                    mandatory=True,
                    member_uuids=_merge_uuids(other_meds),
                )
                ultra.append(med_ultra_item)
            if all_imms:
                l_date, _ = _span_phrase([i.date for i in all_imms])
                include_imm_notes = target_tokens >= 1024
                i_parts = [
                    f"{i.display} ({i.date}) [{i.conflict_note}]"
                    if (include_imm_notes and i.conflict_note)
                    else f"{i.display} ({i.date})"
                    for i in all_imms
                ]
                ultra.append(
                    ClinicalItem(
                        resource_type="Immunization",
                        resource_uuid=all_imms[0].resource_uuid,
                        date=l_date or extracted.reference_date,
                        clinical_date=l_date or extracted.reference_date,
                        date_kind="clinical_event",
                        section="immunization",
                        code="anomalous-imms-ultra",
                        display="Immunization Alerts",
                        text=f"Immunization Alerts: {'; '.join(i_parts)}",
                        mandatory=True,
                        member_uuids=_merge_uuids(all_imms),
                    )
                )
            mandatory_items = ultra
        else:
            mandatory_items = compressed_mandatory

    accepted_optional: List[ClinicalItem] = []
    if len(ranked_optional) > 60 or target_tokens > 4096:
        _, _, _, _, running_toks = render_candidate(mandatory_items)
        for opt in ranked_optional:
            est_opt = max(6, token_counter(opt.text) + 5)
            if running_toks + est_opt <= target_tokens:
                accepted_optional.append(opt)
                running_toks += est_opt
        ordered, manifest_dict, c_text, c_maps, final_toks = render_candidate(
            mandatory_items + sorted(accepted_optional, key=lambda x: _date_sort_key(x.date), reverse=True)
        )
        while final_toks > target_tokens and accepted_optional:
            # Drop several items at once if far over budget, otherwise 1 by 1
            drop_n = max(1, (final_toks - target_tokens) // 35)
            del accepted_optional[-drop_n:]
            ordered, manifest_dict, c_text, c_maps, final_toks = render_candidate(
                mandatory_items + sorted(accepted_optional, key=lambda x: _date_sort_key(x.date), reverse=True)
            )
    else:
        for opt in ranked_optional:
            trial_body = mandatory_items + accepted_optional + [opt]
            _, _, _, _, trial_toks = render_candidate(trial_body)
            if trial_toks <= target_tokens:
                accepted_optional.append(opt)
        accepted_optional.sort(key=lambda x: _date_sort_key(x.date), reverse=True)
        ordered, manifest_dict, c_text, c_maps, final_toks = render_candidate(
            mandatory_items + accepted_optional
        )
    if final_toks > target_tokens:
        raise ValueError(
            f"target_tokens ({target_tokens}) is too small to fit mandatory clinical safety core ({final_toks} tokens required)"
        )
    return ordered, manifest_dict, c_text, c_maps, final_toks


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
    from .normalizers import classify_sensitivity

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

    Supports two `condense_mode` strategies:
    - `"tiered"` / `"hierarchical"` (default): 5-Tier Progressive Condensation Ladder (`Tiers 0 -> 4`) with semantic
      renewal folding, same-day vital panel grouping, and longitudinal lab trend compression.
    - `"lossless_structural"` (`W12`/`W15`): Non-destructive structural condensation that strips only FHIR
      JSON syntax overhead and SHA-256 byte-identical Base64 duplicates while keeping active conditions,
      active medications, allergies, and recent clinical records verbatim without pre-baked
      `[conflict_note]` annotations.
    """
    if not isinstance(target_tokens, int) or isinstance(target_tokens, bool) or target_tokens <= 0:
        raise ValueError("target_tokens must be a positive integer")
    if mode not in {"full", "reconcile"}:
        raise ValueError(f"Unsupported condensation mode: {mode!r}")
    if sharing_posture not in {"patient_self", "external_share"}:
        raise ValueError(f"Unsupported sharing_posture: {sharing_posture!r}")

    counter = token_counter or estimate_tokens_local
    if condense_mode == "lossless_structural":
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

    filter_equip_tiered = True if filter_room_equipment_devices is None else filter_room_equipment_devices
    extracted = (
        bundle
        if isinstance(bundle, ExtractedChart)
        else extract_bundle(bundle, filter_room_equipment_devices=filter_equip_tiered)
    )

    prev_tier_data: Optional[Tuple[int, List[ClinicalItem], List[Dict[str, Any]], Dict[str, Any]]] = None

    for tier in (0, 1, 2, 3, 4):
        items, redacted, manifest_dict = build_tier_items(
            extracted,
            tier,
            mode=mode,
            question=question,
            sharing_posture=sharing_posture,
        )
        records = items_to_querystore_records(items)
        chart_text, mappings = render_chart(records)
        tokens = counter(chart_text)

        if tokens <= target_tokens:
            # If Tier 0 fits, or this tier utilizes >= 55% of target_tokens, return it whole.
            if tier == 0 or tokens >= int(target_tokens * 0.55) or prev_tier_data is None:
                return CondensedChartResult(
                    tier=tier,
                    tier_name=manifest_dict["tier_name"],
                    target_tokens=target_tokens,
                    actual_tokens=tokens,
                    chart_text=chart_text,
                    records=records,
                    mappings=mappings,
                    completeness_manifest=manifest_dict,
                    redacted_for_privacy=redacted,
                    compression_stats={
                        "bundle_entries": extracted.total_bundle_entries,
                        "extracted_clinical_items": len(extracted.items),
                        "rendered_records": len(records),
                        "target_tokens": target_tokens,
                        "actual_tokens": tokens,
                        "budget_utilization": round(tokens / target_tokens, 3),
                    },
                )
            # Otherwise pack the richer previous tier down to target_tokens
            p_tier, p_items, p_redacted, p_manifest = prev_tier_data
            packed_items, p_man_dict, p_text, p_maps, p_toks = _pack_items_to_token_budget(
                extracted,
                p_items,
                p_redacted,
                tier=p_tier,
                tier_label=f"{p_manifest['tier_name']} (Budget-Fitted)",
                target_tokens=target_tokens,
                question=question,
                token_counter=counter,
            )
            p_recs = items_to_querystore_records(packed_items)
            return CondensedChartResult(
                tier=p_tier,
                tier_name=p_man_dict["tier_name"],
                target_tokens=target_tokens,
                actual_tokens=p_toks,
                chart_text=p_text,
                records=p_recs,
                mappings=p_maps,
                completeness_manifest=p_man_dict,
                redacted_for_privacy=p_redacted,
                compression_stats={
                    "bundle_entries": extracted.total_bundle_entries,
                    "extracted_clinical_items": len(extracted.items),
                    "rendered_records": len(p_recs),
                    "target_tokens": target_tokens,
                    "actual_tokens": p_toks,
                    "budget_utilization": round(p_toks / target_tokens, 3),
                },
            )

        prev_tier_data = (tier, items, redacted, manifest_dict)

    # Even Tier 4 exceeded target_tokens -> pack Tier 4 into target_tokens
    assert prev_tier_data is not None
    t4_tier, t4_items, t4_redacted, t4_manifest = prev_tier_data
    packed_items, manifest_dict, chart_text, mappings, tokens = _pack_items_to_token_budget(
        extracted,
        t4_items,
        t4_redacted,
        tier=t4_tier,
        tier_label=f"{t4_manifest['tier_name']} (Budget-Fitted)",
        target_tokens=target_tokens,
        question=question,
        token_counter=counter,
    )
    records = items_to_querystore_records(packed_items)
    return CondensedChartResult(
        tier=4,
        tier_name=manifest_dict["tier_name"],
        target_tokens=target_tokens,
        actual_tokens=tokens,
        chart_text=chart_text,
        records=records,
        mappings=mappings,
        completeness_manifest=manifest_dict,
        redacted_for_privacy=t4_redacted,
        compression_stats={
            "bundle_entries": extracted.total_bundle_entries,
            "extracted_clinical_items": len(extracted.items),
            "rendered_records": len(records),
            "target_tokens": target_tokens,
            "actual_tokens": tokens,
            "budget_utilization": round(tokens / target_tokens, 3),
        },
    )
