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

import json
import os
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .normalizers import classify_sensitivity, classify_vaccine_group, normalize_drug_ingredient

_KB_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "kb_data")
_PACK_CACHE: Dict[str, Tuple[int, Dict[str, Any]]] = {}


def reset_kb_lookup_cache() -> None:
    """Clear cached JSON packs (useful for tests)."""
    _PACK_CACHE.clear()


def _load_json_pack(filename: str) -> Dict[str, Any]:
    path = os.path.join(_KB_DATA_DIR, filename)
    if not os.path.exists(path):
        _PACK_CACHE.pop(filename, None)
        return {}
    try:
        mtime_ns = os.stat(path).st_mtime_ns
    except OSError:
        _PACK_CACHE.pop(filename, None)
        return {}
    cached = _PACK_CACHE.get(filename)
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        _PACK_CACHE.pop(filename, None)
        return {}
    if not isinstance(data, dict):
        _PACK_CACHE.pop(filename, None)
        return {}
    _PACK_CACHE[filename] = (mtime_ns, data)
    return data


def get_acip_rules() -> Dict[str, Any]:
    return _load_json_pack("acip_rules.json")


def get_rxnorm_lookup() -> Dict[str, Any]:
    return _load_json_pack("rxnorm_lookup.json")


def get_clinical_exclusivity_and_privacy() -> Dict[str, Any]:
    return _load_json_pack("clinical_exclusivity_and_privacy.json")


def _norm_cond_display(text: str) -> str:
    return re.sub(
        r"\s*\((?:disorder|finding|situation)\)\s*$",
        "",
        (text or "").strip(),
        flags=re.I,
    ).lower()


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

    # Fallback by exact normalized display or explicit alias match (never arbitrary substring)
    disp_low = _norm_cond_display(display)
    if disp_low:
        for _k, v in snomed_map.items():
            v_disp = _norm_cond_display(str(v.get("display") or ""))
            aliases = {_norm_cond_display(str(a)) for a in (v.get("aliases") or ()) if a}
            if disp_low == v_disp or disp_low in aliases:
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

    raw_low = (display or c_str).strip().lower()
    matched_ing = ""

    # 1. Check explicit brand_to_generic keys and entry brand_names / multi-word ingredients on raw text first
    if raw_low:
        for b_key, g_val in sorted(brand_map.items(), key=lambda kv: len(kv[0]), reverse=True):
            if re.search(rf"\b{re.escape(b_key.lower())}\b", raw_low):
                matched_ing = str(g_val).lower()
                break
        if not matched_ing:
            # Sort entries by ingredient length descending so specific multi-word ingredients
            # (e.g. "insulin isophane / regular", "penicillin v") match before shorter prefixes
            for _k, v in sorted(
                rx_map.items(),
                key=lambda kv: len(str(kv[1].get("ingredient") or "")),
                reverse=True,
            ):
                v_ing = str(v.get("ingredient") or "").lower().strip()
                v_brands = [str(b).lower().strip() for b in (v.get("brand_names") or ()) if b]
                if v_ing and re.search(rf"\b{re.escape(v_ing)}\b", raw_low):
                    matched_ing = v_ing
                    break
                if any(b and re.search(rf"\b{re.escape(b)}\b", raw_low) for b in v_brands):
                    matched_ing = v_ing
                    break

    ing = matched_ing or normalize_drug_ingredient(display or c_str)
    if ing in brand_map:
        ing = str(brand_map[ing]).lower()

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

    # Fallback by exact normalized display or explicit specimen-preserving alias match
    disp_low = (display or "").strip().lower()
    if disp_low:
        for _k, v in loinc_map.items():
            v_disp = str(v.get("display") or "").strip().lower()
            aliases = {str(a).strip().lower() for a in (v.get("aliases") or ()) if a}
            if disp_low == v_disp or disp_low in aliases:
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


def extract_chart_kb_codes(
    extracted: Any,
    *,
    sharing_posture: str = "patient_self",
) -> List[str]:
    """Return prioritized clinical codes (SNOMED, RxNorm, CVX, LOINC) from an `ExtractedChart`
    so `kb.search(..., corpus='wahs', codes=...)` can perform deterministic code-first retrieval."""
    if sharing_posture not in {"patient_self", "external_share"}:
        raise ValueError(f"Unsupported sharing_posture: {sharing_posture!r}")
    priority_codes: List[str] = []
    secondary_codes: List[str] = []

    items: Sequence[Any] = getattr(extracted, "items", None) or []
    for it in items:
        code = str(getattr(it, "code", "") or "").strip()
        if not code:
            continue
        disp = str(getattr(it, "display", "") or "")
        sens = getattr(it, "sensitivity", None) or classify_sensitivity(code, disp)
        if sharing_posture == "external_share" and sens == "SDOH/PRIVACY-SENSITIVE":
            continue
        if getattr(it, "conflict_note", None) or sens == "SDOH/PRIVACY-SENSITIVE":
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
        if not np_code:
            continue
        np_concept = str(getattr(np, "concept", "") or "")
        np_cat = str(getattr(np, "category", "") or "")
        if (
            sharing_posture == "external_share"
            and classify_sensitivity(np_code, np_concept, (np_cat,) if np_cat else ())
            == "SDOH/PRIVACY-SENSITIVE"
        ):
            continue
        secondary_codes.append(np_code)

    return list(dict.fromkeys(priority_codes + secondary_codes))


def audit_cohort_code_coverage(extracted: Any) -> Dict[str, Any]:
    """Audit exact KB resolution coverage across active conditions, medications, lab panels,
    standalone lab observations, vital signs, and immunizations in an `ExtractedChart`."""
    exact_resolvers = {"snomed_code", "rxnorm_code", "cvx_code", "loinc_code"}
    counts = {
        "condition_active": {"total": 0, "resolved": 0, "exact_code_resolved": 0, "display_or_alias_resolved": 0},
        "medications": {"total": 0, "resolved": 0, "exact_code_resolved": 0, "display_or_alias_resolved": 0},
        "immunizations": {"total": 0, "resolved": 0, "exact_code_resolved": 0, "display_or_alias_resolved": 0},
        "lab_panels_and_vitals": {"total": 0, "resolved": 0, "exact_code_resolved": 0, "display_or_alias_resolved": 0},
    }
    unresolved: List[Dict[str, str]] = []

    def _record_result(bucket_name: str, sec_name: str, code_val: str, disp_val: str, res_obj: Mapping[str, Any]) -> None:
        bucket = counts[bucket_name]
        bucket["total"] += 1
        r_by = str(res_obj.get("resolved_by") or "heuristic_fallback")
        if r_by != "heuristic_fallback":
            bucket["resolved"] += 1
            if r_by in exact_resolvers:
                bucket["exact_code_resolved"] += 1
            else:
                bucket["display_or_alias_resolved"] += 1
        else:
            unresolved.append({"section": sec_name, "code": code_val, "display": disp_val})

    for it in getattr(extracted, "items", None) or []:
        sec = getattr(it, "section", "")
        code = str(getattr(it, "code", "") or "")
        disp = str(getattr(it, "display", "") or "")
        if sec == "condition_active":
            _record_result("condition_active", sec, code, disp, resolve_condition_code(code, disp))
        elif sec in {"med_active", "med_historical"}:
            _record_result("medications", sec, code, disp, resolve_medication_code(code, disp))
        elif sec == "immunization":
            _record_result("immunizations", sec, code, disp, resolve_immunization_code(code, disp))
        elif sec in {"lab_panel", "lab_observation", "vital_sign"}:
            _record_result("lab_panels_and_vitals", sec, code, disp, resolve_lab_code(code, disp))

    total_checked = sum(v["total"] for v in counts.values())
    total_resolved = sum(v["resolved"] for v in counts.values())
    exact_code_resolved = sum(v["exact_code_resolved"] for v in counts.values())
    display_or_alias_resolved = sum(v["display_or_alias_resolved"] for v in counts.values())
    return {
        "total_checked": total_checked,
        "total_resolved": total_resolved,
        "exact_code_resolved": exact_code_resolved,
        "display_or_alias_resolved": display_or_alias_resolved,
        "resolution_rate": 1.0 if total_checked == 0 else total_resolved / total_checked,
        "exact_code_resolution_rate": 1.0 if total_checked == 0 else exact_code_resolved / total_checked,
        "by_category": counts,
        "unresolved": unresolved,
    }
