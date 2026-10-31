"""Read-only, privacy-enforcing full-chart overflow lookup tools for WAHS+ (`W7`).

When a patient or clinician question asks for deep historical records (> 2 years old),
full-text clinical notes, or perioperative details that were condensed out of the
in-context IPS Safety Core (`Tier 3: 4k` or `Tier 4: 2k`), these tools query the
uncondensed `Tier 0/1` `ExtractedChart` in memory while strictly enforcing
`sharing_posture` (`external_share` redacts `SDOH/PRIVACY-SENSITIVE` records under
42 CFR Part 2 and the WA My Health My Data Act).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .extractors import ClinicalItem, ExtractedChart, extract_bundle


def _ensure_extracted(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
) -> ExtractedChart:
    if isinstance(bundle_or_extracted, ExtractedChart):
        return bundle_or_extracted
    return extract_bundle(bundle_or_extracted)


def _is_privacy_redacted(item: ClinicalItem, sharing_posture: str) -> bool:
    return (
        sharing_posture == "external_share"
        and item.sensitivity == "SDOH/PRIVACY-SENSITIVE"
    )


def search_chart_entries(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    query: str = "",
    *,
    section: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sharing_posture: str = "patient_self",
    limit: int = 15,
) -> Dict[str, Any]:
    """Search uncondensed `Tier 0/1` chart entries by keyword, section, and date window.

    Enforces `sharing_posture`: when `sharing_posture == 'external_share'`, all items
    tagged `SDOH/PRIVACY-SENSITIVE` are withheld and counted in `redacted_for_privacy_count`.
    """
    extracted = _ensure_extracted(bundle_or_extracted)
    raw_tokens = [
        tok
        for tok in re.findall(r"[a-z0-9.\-]+", (query or "").lower())
        if len(tok) >= 2
    ]
    stopwords = {"record", "records", "status", "active", "condition", "medication", "observation", "the", "and", "for"}
    specific_tokens = [t for t in raw_tokens if t not in stopwords]
    q_tokens = specific_tokens or raw_tokens

    matches: List[Dict[str, Any]] = []
    redacted_count = 0

    for item in extracted.items:
        if item.section == "patient":
            continue
        if _is_privacy_redacted(item, sharing_posture):
            redacted_count += 1
            continue
        if section and section.lower() not in item.section.lower():
            continue
        item_date = item.clinical_date or item.date or ""
        if date_from and item_date and item_date < date_from:
            continue
        if date_to and item_date and item_date > date_to:
            continue

        haystack = f"{item.display} {item.text} {item.code} {item.section} {item.source_org or ''}".lower()
        if q_tokens:
            score = sum(1 for tok in q_tokens if tok in haystack)
            if score == 0:
                continue
        else:
            score = 1

        matches.append(
            {
                "score": score,
                "resource_type": item.resource_type,
                "resource_uuid": item.resource_uuid,
                "member_uuids": list(item.member_uuids),
                "section": item.section,
                "code": item.code,
                "display": item.display,
                "date": item_date,
                "source_org": item.source_org or "",
                "status": item.status,
                "conflict_note": item.conflict_note or "",
                "text": item.text,
            }
        )

    matches.sort(key=lambda m: (m["score"], m["date"] or ""), reverse=True)
    top_matches = matches[: max(1, int(limit))]
    return {
        "tool": "search_chart_entries",
        "query": query,
        "section": section,
        "date_from": date_from,
        "date_to": date_to,
        "sharing_posture": sharing_posture,
        "total_matches": len(matches),
        "redacted_for_privacy_count": redacted_count,
        "results": top_matches,
    }


def get_lab_history(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    analyte_or_loinc: str,
    *,
    limit: int = 25,
    sharing_posture: str = "patient_self",
) -> Dict[str, Any]:
    """Retrieve longitudinal quantitative lab/vital measurements from the uncondensed chart."""
    extracted = _ensure_extracted(bundle_or_extracted)
    q_norm = (analyte_or_loinc or "").strip().lower()
    q_tokens = [t for t in re.findall(r"[a-z0-9.\-]+", q_norm) if len(t) >= 2]

    pts = []
    for p in extracted.numeric_points:
        hay = f"{p.concept} {p.code} {p.category}".lower()
        if q_norm in hay or (q_tokens and any(t in hay for t in q_tokens)):
            pts.append(p)

    pts.sort(key=lambda p: p.date)
    selected = pts[-max(1, int(limit)) :]

    series = [
        {
            "resource_uuid": p.resource_uuid,
            "date": p.date,
            "code": p.code,
            "concept": p.concept,
            "value": p.value,
            "unit": p.unit,
            "category": p.category,
        }
        for p in selected
    ]
    summary: Dict[str, Any] = {}
    if pts:
        min_pt = min(pts, key=lambda x: x.value)
        max_pt = max(pts, key=lambda x: x.value)
        summary = {
            "total_measurements": len(pts),
            "initial": {"value": pts[0].value, "unit": pts[0].unit, "date": pts[0].date, "resource_uuid": pts[0].resource_uuid},
            "min": {"value": min_pt.value, "unit": min_pt.unit, "date": min_pt.date, "resource_uuid": min_pt.resource_uuid},
            "max": {"value": max_pt.value, "unit": max_pt.unit, "date": max_pt.date, "resource_uuid": max_pt.resource_uuid},
            "latest": {"value": pts[-1].value, "unit": pts[-1].unit, "date": pts[-1].date, "resource_uuid": pts[-1].resource_uuid},
        }

    return {
        "tool": "get_lab_history",
        "analyte_or_loinc": analyte_or_loinc,
        "sharing_posture": sharing_posture,
        "summary": summary,
        "results": series,
    }


def read_clinical_note(
    bundle_or_extracted: Union[Mapping[str, Any], ExtractedChart],
    resource_uuid: str = "",
    *,
    query: str = "",
    sharing_posture: str = "patient_self",
) -> Dict[str, Any]:
    """Read full-text clinical notes (`DocumentReference` / `DiagnosticReport`) by UUID or keyword.

    Enforces `sharing_posture`: withholds any note tagged `SDOH/PRIVACY-SENSITIVE` when
    `sharing_posture == 'external_share'`.
    """
    extracted = _ensure_extracted(bundle_or_extracted)
    target_uuid = (resource_uuid or "").strip()
    q_norm = (query or "").strip().lower()

    notes: List[Dict[str, Any]] = []
    redacted_count = 0

    for item in extracted.items:
        if item.section not in {"clinical_note", "note"}:
            continue
        if _is_privacy_redacted(item, sharing_posture):
            redacted_count += 1
            continue
        all_uuids = {item.resource_uuid} | set(item.member_uuids)
        if target_uuid and target_uuid not in all_uuids:
            continue
        if q_norm and q_norm not in f"{item.display} {item.text}".lower():
            continue
        notes.append(
            {
                "resource_type": item.resource_type,
                "resource_uuid": item.resource_uuid,
                "member_uuids": list(item.member_uuids),
                "date": item.clinical_date or item.date or "",
                "display": item.display,
                "source_org": item.source_org or "",
                "conflict_note": item.conflict_note or "",
                "text": item.text,
            }
        )

    return {
        "tool": "read_clinical_note",
        "resource_uuid": target_uuid,
        "query": query,
        "sharing_posture": sharing_posture,
        "redacted_for_privacy_count": redacted_count,
        "results": notes,
    }
