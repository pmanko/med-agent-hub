"""Deterministic 4-Layer WAHS+ Clinical Knowledge Base lookup & code resolution.

Loads the structured WAHS+ reference packs in `server/kb_data/`:
- Layer 1 (`acip_rules.json`): CDC ACIP Immunization Schedule & CVX Vaccine Group rules
- Layer 2 (`rxnorm_lookup.json`): NLM RxNorm Ingredient, Brand↔Generic, & Therapeutic Class table
- Layer 3 (`clinical_exclusivity_and_privacy.json`): KDIGO/ADA/MedlinePlus SNOMED CT condition stage
  exclusivity, SDOH privacy classifications, and LOINC lab/vital reference ranges
- Layer 4 (`wahs_corpus.jsonl`, searched via `server.kb`): Provenance-bearing clinical & patient-education
  snippets retrieved via hybrid code-first + FTS5 BM25 matching
"""

from __future__ import annotations

import functools
import json
import os
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .normalizers import classify_sensitivity, classify_vaccine_group, normalize_drug_ingredient

_KB_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "kb_data")


@functools.lru_cache(maxsize=4)
def _load_json_pack(filename: str) -> Dict[str, Any]:
    path = os.path.join(_KB_DATA_DIR, filename)
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_acip_rules() -> Dict[str, Any]:
    return _load_json_pack("acip_rules.json")


def get_rxnorm_lookup() -> Dict[str, Any]:
    return _load_json_pack("rxnorm_lookup.json")


def get_clinical_exclusivity_and_privacy() -> Dict[str, Any]:
    return _load_json_pack("clinical_exclusivity_and_privacy.json")


def resolve_condition_code(code: str, display: str = "") -> Dict[str, Any]:
    """Resolve a SNOMED CT condition code or display string to plain-language MedlinePlus/KDIGO
    description, stage exclusivity group, and SDOH sensitivity rule."""
    pack = get_clinical_exclusivity_and_privacy()
    snomed_map = pack.get("snomed_conditions") or {}
    c_str = str(code or "").strip()
    if c_str in snomed_map:
        entry = dict(snomed_map[c_str])
        entry["resolved_by"] = "snomed_code"
        return entry

    # Fallback by normalized display match
    disp_low = re.sub(r"\s*\((?:disorder|finding|situation)\)\s*$", "", (display or "").strip(), flags=re.I).lower()
    for k, v in snomed_map.items():
        v_disp = re.sub(
            r"\s*\((?:disorder|finding|situation)\)\s*$",
            "",
            str(v.get("display") or ""),
            flags=re.I,
        ).lower()
        if disp_low and (disp_low == v_disp or disp_low in v_disp or v_disp in disp_low):
            entry = dict(v)
            entry["resolved_by"] = "display_match"
            return entry

    sens = classify_sensitivity(c_str, display, ())
    return {
        "snomed": c_str,
        "display": display or c_str,
        "exclusivity_group": None,
        "stage_rank": None,
        "sensitivity": sens,
        "plain_description": f"Clinical condition: {display or c_str}.",
        "resolved_by": "heuristic_fallback",
    }


def resolve_medication_code(code: str, display: str = "") -> Dict[str, Any]:
    """Resolve an RxNorm code or medication display string to normalized active ingredient,
    therapeutic class, brand names, and plain-language purpose."""
    pack = get_rxnorm_lookup()
    rx_map = pack.get("rxnorm_codes") or {}
    brand_map = pack.get("brand_to_generic") or {}
    c_str = str(code or "").strip()
    if c_str in rx_map:
        entry = dict(rx_map[c_str])
        entry["resolved_by"] = "rxnorm_code"
        return entry

    ing = normalize_drug_ingredient(display or c_str)
    if ing in brand_map:
        ing = brand_map[ing]

    for _k, v in rx_map.items():
        if str(v.get("ingredient") or "").lower() == ing and ing:
            entry = dict(v)
            entry["rxnorm"] = c_str or entry.get("rxnorm", "")
            entry["display"] = display or entry.get("display", "")
            entry["resolved_by"] = "ingredient_match"
            return entry

    return {
        "rxnorm": c_str,
        "display": display or c_str,
        "ingredient": ing or (display or c_str).lower(),
        "therapeutic_class": "medication",
        "brand_names": [],
        "plain_purpose": f"Prescribed or administered medication ({display or c_str}).",
        "resolved_by": "heuristic_fallback",
    }


def resolve_immunization_code(cvx_code: str, display: str = "") -> Dict[str, Any]:
    """Resolve a CVX code or vaccine display string to CDC ACIP vaccine group and schedule rules."""
    pack = get_acip_rules()
    cvx_map = pack.get("cvx_groups") or {}
    c_str = str(cvx_code or "").strip()
    if c_str in cvx_map:
        entry = dict(cvx_map[c_str])
        entry["resolved_by"] = "cvx_code"
        return entry

    v_group = classify_vaccine_group(c_str, display)
    for _k, v in cvx_map.items():
        if str(v.get("vaccine_group") or "").lower() == v_group.lower():
            entry = dict(v)
            entry["cvx"] = c_str or entry.get("cvx", "")
            entry["display"] = display or entry.get("display", "")
            entry["resolved_by"] = "vaccine_group_match"
            return entry

    return {
        "cvx": c_str,
        "vaccine_group": v_group,
        "display": display or c_str,
        "plain_purpose": f"Immunization against {v_group}.",
        "series_rule": "Standard CDC ACIP schedule applies.",
        "duplicate_window_days": 14,
        "resolved_by": "heuristic_fallback",
    }


def resolve_lab_code(loinc_code: str, display: str = "") -> Dict[str, Any]:
    """Resolve a LOINC lab panel, vital sign, or analyte code/display to a plain-language
    explanation and reference range."""
    pack = get_clinical_exclusivity_and_privacy()
    loinc_map = pack.get("loinc_labs_and_vitals") or {}
    c_str = str(loinc_code or "").strip()
    if c_str in loinc_map:
        entry = dict(loinc_map[c_str])
        entry["resolved_by"] = "loinc_code"
        return entry

    disp_low = (display or "").strip().lower()
    for _k, v in loinc_map.items():
        v_disp = str(v.get("display") or "").lower()
        if disp_low and (disp_low == v_disp or disp_low in v_disp or v_disp in disp_low):
            entry = dict(v)
            entry["resolved_by"] = "display_match"
            return entry

    return {
        "loinc": c_str,
        "display": display or c_str,
        "plain_description": f"Diagnostic measurement or panel: {display or c_str}.",
        "reference_range": "Consult laboratory reference range",
        "resolved_by": "heuristic_fallback",
    }


def extract_chart_kb_codes(extracted: Any) -> List[str]:
    """Return prioritized clinical codes (SNOMED, RxNorm, CVX, LOINC) from an `ExtractedChart`
    so `kb.search(..., corpus='wahs', codes=...)` can perform deterministic code-first retrieval."""
    priority_codes: List[str] = []
    secondary_codes: List[str] = []

    items: Sequence[Any] = getattr(extracted, "items", None) or []
    for it in items:
        code = str(getattr(it, "code", "") or "").strip()
        if not code:
            continue
        if getattr(it, "conflict_note", None) or getattr(it, "sensitivity", None) == "SDOH/PRIVACY-SENSITIVE":
            priority_codes.append(code)
        elif getattr(it, "section", "") in {
            "condition_active",
            "med_active",
            "immunization",
            "lab_panel",
            "procedure_major",
        }:
            secondary_codes.append(code)

    for np in getattr(extracted, "numeric_points", None) or []:
        np_code = str(getattr(np, "code", "") or "").strip()
        if np_code:
            secondary_codes.append(np_code)

    return list(dict.fromkeys(priority_codes + secondary_codes))


def audit_cohort_code_coverage(extracted: Any) -> Dict[str, Any]:
    """Audit exact KB resolution coverage across active conditions, medications, lab panels,
    and immunizations in an `ExtractedChart`."""
    counts = {
        "condition_active": {"total": 0, "resolved": 0},
        "medications": {"total": 0, "resolved": 0},
        "immunizations": {"total": 0, "resolved": 0},
        "lab_panels_and_vitals": {"total": 0, "resolved": 0},
    }
    unresolved: List[Dict[str, str]] = []

    for it in getattr(extracted, "items", None) or []:
        sec = getattr(it, "section", "")
        code = str(getattr(it, "code", "") or "")
        disp = str(getattr(it, "display", "") or "")
        if sec == "condition_active":
            counts["condition_active"]["total"] += 1
            res = resolve_condition_code(code, disp)
            if res.get("resolved_by") != "heuristic_fallback":
                counts["condition_active"]["resolved"] += 1
            else:
                unresolved.append({"section": sec, "code": code, "display": disp})
        elif sec in {"med_active", "med_historical"}:
            counts["medications"]["total"] += 1
            res = resolve_medication_code(code, disp)
            if res.get("resolved_by") != "heuristic_fallback":
                counts["medications"]["resolved"] += 1
            else:
                unresolved.append({"section": sec, "code": code, "display": disp})
        elif sec == "immunization":
            counts["immunizations"]["total"] += 1
            res = resolve_immunization_code(code, disp)
            if res.get("resolved_by") != "heuristic_fallback":
                counts["immunizations"]["resolved"] += 1
            else:
                unresolved.append({"section": sec, "code": code, "display": disp})
        elif sec in {"lab_panel", "vital_sign"}:
            counts["lab_panels_and_vitals"]["total"] += 1
            res = resolve_lab_code(code, disp)
            if res.get("resolved_by") != "heuristic_fallback":
                counts["lab_panels_and_vitals"]["resolved"] += 1
            else:
                unresolved.append({"section": sec, "code": code, "display": disp})

    total_checked = sum(v["total"] for v in counts.values())
    total_resolved = sum(v["resolved"] for v in counts.values())
    return {
        "total_checked": total_checked,
        "total_resolved": total_resolved,
        "resolution_rate": 1.0 if total_checked == 0 else total_resolved / total_checked,
        "by_category": counts,
        "unresolved": unresolved,
    }
