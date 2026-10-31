"""Deterministic US Core / FHIR R4 clinical normalizers, privacy classifiers, and conflict detectors."""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# Sensitive SNOMED-CT / ICD-10 / LOINC codes and patterns requiring sharing warnings or redaction
# under HL7 DS4P / SAMHSA 42 CFR Part 2 / Gravity Project SDOH guidance.
_PRIVACY_SENSITIVE_CODES: Set[str] = {
    "266948004",  # Has a criminal record (finding)
    "706893006",  # Victim of intimate partner abuse (finding)
    "160968000",  # Reports of violence in the environment (finding)
    "228366006",  # Unhealthy alcohol drinking behavior (finding)
    "423315002",  # Limited social contact (finding)
    "55600006",   # Social isolation (finding)
}

_PRIVACY_SENSITIVE_PATTERNS = re.compile(
    r"\b(?:"
    r"criminal\s+record|"
    r"intimate\s+partner\s+abuse|"
    r"domestic\s+(?:abuse|violence)|"
    r"violence\s+in\s+the\s+environment|"
    r"unhealthy\s+alcohol|"
    r"alcohol\s+abuse|"
    r"drug\s+abuse|"
    r"substance\s+abuse|"
    r"sexual\s+assault|"
    r"limited\s+social\s+contact|"
    r"social\s+isolation"
    r")\b",
    re.IGNORECASE,
)

# Non-disease social/administrative findings often stored in Synthea/US Core Condition resources.
_SDOH_NON_MEDICAL_CODES: Set[str] = {
    "314529007",  # Medication review due (situation)
    "160903007",  # Full-time employment (finding)
    "160904001",  # Part-time employment (finding)
    "73438004",   # Unemployed (finding)
    "741062008",  # Not in labor force (finding)
    "224299000",  # Received higher education (finding)
    "473461003",  # Educated to high school level (finding)
    "73595000",   # Stress (finding)
}

_CKD_STAGE_BY_CODE: Dict[str, int] = {
    "431855005": 1,  # Chronic kidney disease stage 1 (disorder)
    "431856006": 2,  # Chronic kidney disease stage 2 (disorder)
    "433144002": 3,  # Chronic kidney disease stage 3 (disorder)
    "700378005": 3,  # CKD stage 3A
    "700379002": 3,  # CKD stage 3B
    "431857002": 4,  # Chronic kidney disease stage 4 (disorder)
    "46177005": 5,   # End-stage renal disease / CKD stage 5
}

_PREDIABETES_CODES: Set[str] = {
    "714628002",  # Prediabetes (finding)
    "15777000",   # Prediabetes
}

_ESTABLISHED_DIABETES_CODES: Set[str] = {
    "44054006",       # Type 2 diabetes mellitus
    "46635009",       # Type 1 diabetes mellitus
    "127013003",      # Disorder of kidney due to diabetes mellitus (disorder)
    "90781000119102", # Microalbuminuria due to type 2 diabetes mellitus (disorder)
    "157141000119108",# Proteinuria due to type 2 diabetes mellitus (disorder)
    "422034002",      # Diabetic retinopathy
    "230572002",      # Neuropathy due to diabetes mellitus
}

_BRAND_TO_GENERIC: Dict[str, str] = {
    "plavix": "clopidogrel",
    "lipitor": "atorvastatin",
    "zocor": "simvastatin",
    "crestor": "rosuvastatin",
    "lopressor": "metoprolol",
    "toprol": "metoprolol",
    "toprol-xl": "metoprolol",
    "glucophage": "metformin",
    "prinivil": "lisinopril",
    "zestril": "lisinopril",
    "norvasc": "amlodipine",
    "cozaar": "losartan",
    "lasix": "furosemide",
    "microzide": "hydrochlorothiazide",
    "humulin": "insulin human",
    "novolin": "insulin human",
    "lantus": "insulin glargine",
    "humalog": "insulin lispro",
    "novolog": "insulin aspart",
    "nitrostat": "nitroglycerin",
    "coumadin": "warfarin",
    "eliquis": "apixaban",
    "xarelto": "rivaroxaban",
    "advil": "ibuprofen",
    "motrin": "ibuprofen",
    "tylenol": "acetaminophen",
}

_DOSE_NOISE = re.compile(
    r"\b(?:"
    r"\d+(?:\.\d+)?\s*(?:mg|mcg|ug|g|ml|iu|unit|units|meq|%|hr|actuat|unt)(?:/(?:ml|mg|hr|actuat|dose))?|"
    r"oral|injectable|injection|tablet|tablets|capsule|capsules|solution|suspension|spray|gel|cream|"
    r"extended\s+release|delayed\s+release|sustained\s+release|24\s*hr|12\s*hr|"
    r"sublingual|transdermal|inhalation|prefilled\s+syringe|auto-injector|pen\s+injector|"
    r"tartrate|succinate|besylate|hydrochloride|hcl|sodium|potassium|calcium|maleate|fumarate|mononitrate"
    r")\b",
    re.IGNORECASE,
)

# Transient clinic room equipment that Synthea emits as Device resources but are not patient-carried devices.
_CLINIC_ROOM_DEVICE_PATTERNS = re.compile(
    r"\b(?:dental\s+x-ray\s+system|radiography\s+system|operating\s+room|hospital\s+bed|examination\s+table)\b",
    re.IGNORECASE,
)


def classify_sensitivity(
    code: Optional[str], display: str, categories: Sequence[str] = ()
) -> Optional[str]:
    """Return 'SDOH/PRIVACY-SENSITIVE' for sensitive legal/safety/behavioral items,
    'SDOH-SOCIAL' for routine social/administrative status items, or None."""
    clean_code = (code or "").strip()
    if clean_code in _PRIVACY_SENSITIVE_CODES or _PRIVACY_SENSITIVE_PATTERNS.search(display or ""):
        return "SDOH/PRIVACY-SENSITIVE"
    if clean_code in _SDOH_NON_MEDICAL_CODES:
        return "SDOH-SOCIAL"
    lowered_cats = {c.lower() for c in categories if c}
    if "social-history" in lowered_cats and _PRIVACY_SENSITIVE_PATTERNS.search(display or ""):
        return "SDOH/PRIVACY-SENSITIVE"
    return None


def is_clinic_room_equipment(display: str) -> bool:
    """True when a FHIR Device resource represents clinic room hardware rather than a patient device."""
    return bool(_CLINIC_ROOM_DEVICE_PATTERNS.search(display or ""))


def normalize_drug_ingredient(display: str) -> str:
    """Normalize a medication display string to its canonical active-ingredient key."""
    text = (display or "").lower()
    # Replace bracketed brand names e.g. "[Plavix]" -> check brand map first
    for brand, generic in _BRAND_TO_GENERIC.items():
        if re.search(rf"\b{re.escape(brand)}\b", text):
            return generic
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = _DOSE_NOISE.sub(" ", text)
    text = re.sub(r"[^a-z0-9/+\s-]", " ", text)
    tokens = [tok for tok in text.split() if len(tok) > 1 and not tok.isdigit()]
    if not tokens:
        return (display or "unknown-medication").strip().lower()
    head = " ".join(tokens[:3]).strip()
    return _BRAND_TO_GENERIC.get(tokens[0], head)


def detect_condition_conflicts(
    active_conditions: Sequence[Mapping[str, Any]],
) -> Dict[str, str]:
    """Return a map of condition `resourceUuid -> conflict_tag` for mutually exclusive active conditions."""
    conflicts: Dict[str, str] = {}
    ckd_by_stage: Dict[int, List[str]] = {}
    prediabetes_uuids: List[str] = []
    diabetes_uuids: List[Tuple[str, str]] = []

    for cond in active_conditions:
        uuid = str(cond.get("resourceUuid") or "")
        code = str(cond.get("code") or "")
        display = str(cond.get("display") or "")
        if not uuid:
            continue
        stage = _CKD_STAGE_BY_CODE.get(code)
        if stage is None:
            m = re.search(r"chronic\s+kidney\s+disease\s+stage\s+([1-5])", display, re.IGNORECASE)
            if m:
                stage = int(m.group(1))
        if stage is not None:
            ckd_by_stage.setdefault(stage, []).append(uuid)

        if code in _PREDIABETES_CODES or re.search(r"\bprediabetes\b", display, re.IGNORECASE):
            prediabetes_uuids.append(uuid)
        elif code in _ESTABLISHED_DIABETES_CODES or re.search(
            r"\b(?:type\s+[12]\s+diabetes|due\s+to\s+(?:type\s+[12]\s+)?diabetes\s+mellitus)\b",
            display,
            re.IGNORECASE,
        ):
            diabetes_uuids.append((uuid, display))

    if len(ckd_by_stage) > 1:
        stages_str = ", ".join(f"stage {s}" for s in sorted(ckd_by_stage))
        max_stage = max(ckd_by_stage)
        for stage, uuids in ckd_by_stage.items():
            for uuid in uuids:
                conflicts[uuid] = (
                    f"stage_conflict: CKD {stages_str} simultaneously marked ACTIVE "
                    f"(highest documented: stage {max_stage})"
                )

    if prediabetes_uuids and diabetes_uuids:
        dm_names = ", ".join(sorted({disp for _, disp in diabetes_uuids})[:2])
        for uuid in prediabetes_uuids:
            conflicts[uuid] = (
                f"stage_conflict: Prediabetes marked ACTIVE alongside established diabetes ({dm_names})"
            )

    return conflicts


def classify_vaccine_group(code: Optional[str], display: str) -> str:
    """Map CVX code or vaccine display string to a canonical vaccine group."""
    c = (code or "").strip()
    d = (display or "").lower()
    if c in {"62", "118", "137", "165"} or "hpv" in d or "papillomavirus" in d:
        return "HPV"
    if c in {"121"} or ("zoster" in d and "live" in d):
        return "Zoster-Live"
    if c in {"187", "188"} or "zoster" in d or "shingrix" in d:
        return "Zoster-Recombinant"
    if "influenza" in d or "flu" in d or c in {"140", "141", "150", "158", "168", "197"}:
        return "Influenza"
    if "covid" in d or "sars-cov-2" in d or c in {"207", "208", "212", "213", "300", "301", "309"}:
        return "COVID-19"
    if "tdap" in d or "tetanus" in d or "diphtheria" in d or c in {"115", "09", "113", "20", "106"}:
        return "Tdap/DTaP/Td"
    if "pneumococcal" in d or c in {"33", "133", "152", "215", "216"}:
        return "Pneumococcal"
    if "meningococcal" in d or c in {"114", "136", "147"}:
        return "Meningococcal"
    if "hepatitis b" in d or "hep b" in d or c in {"08", "43", "44", "45"}:
        return "HepB"
    if "hepatitis a" in d or "hep a" in d or c in {"83", "85"}:
        return "HepA"
    if "mmr" in d or "measles" in d or c in {"03", "94"}:
        return "MMR"
    if "varicella" in d or c in {"21"}:
        return "Varicella"
    return (display or "Vaccine").strip()


def _age_in_years(birth_date_iso: Optional[str], event_date_iso: Optional[str]) -> Optional[float]:
    if not birth_date_iso or not event_date_iso:
        return None
    try:
        b = date.fromisoformat(birth_date_iso[:10])
        e = date.fromisoformat(event_date_iso[:10])
        return round((e - b).days / 365.2425, 2)
    except ValueError:
        return None


def annotate_immunization_anomalies(
    immunizations: Sequence[Mapping[str, Any]],
    birth_date_iso: Optional[str],
) -> Dict[str, str]:
    """Return `resourceUuid -> acip_or_duplicate_note` for immunization series deviations or near-date duplicates."""
    notes: Dict[str, str] = {}
    by_group: Dict[str, List[Mapping[str, Any]]] = {}
    for imm in immunizations:
        group = classify_vaccine_group(imm.get("code"), str(imm.get("display") or ""))
        by_group.setdefault(group, []).append(imm)

    for group, items in by_group.items():
        ordered = sorted(items, key=lambda x: str(x.get("date") or ""))
        # 1. Check near-date cross-source duplicates (< 14 days apart in same vaccine group)
        for i in range(1, len(ordered)):
            prev, curr = ordered[i - 1], ordered[i]
            d1, d2 = str(prev.get("date") or ""), str(curr.get("date") or "")
            try:
                days_apart = abs((date.fromisoformat(d2[:10]) - date.fromisoformat(d1[:10])).days)
            except ValueError:
                days_apart = 999
            if days_apart <= 14:
                u1, u2 = str(prev.get("resourceUuid") or ""), str(curr.get("resourceUuid") or "")
                msg = (
                    f"duplicate_candidate: {group} recorded {days_apart}d apart ({d1} vs {d2})"
                )
                if u1:
                    notes[u1] = msg
                if u2:
                    notes[u2] = msg

        # 2. Check ACIP HPV 2-dose vs 3-dose schedule rule
        if group == "HPV" and len(ordered) >= 3:
            first_date = str(ordered[0].get("date") or "")
            age_first = _age_in_years(birth_date_iso, first_date)
            if age_first is not None and age_first < 15.0:
                last_uuid = str(ordered[-1].get("resourceUuid") or "")
                if last_uuid and last_uuid not in notes:
                    notes[last_uuid] = (
                        f"acip_schedule_note: 3rd HPV dose recorded after series initiated at age "
                        f"{int(age_first)} ({first_date}); CDC ACIP specifies 2-dose series when started before age 15"
                    )

        # 3. Check Live Zoster (Zostavax CVX 121) repeat doses
        if group == "Zoster-Live" and len(ordered) >= 2:
            last_uuid = str(ordered[-1].get("resourceUuid") or "")
            dates_list = ", ".join(str(x.get("date") or "") for x in ordered)
            if last_uuid and last_uuid not in notes:
                notes[last_uuid] = (
                    f"acip_schedule_note: {len(ordered)} doses of live zoster vaccine recorded ({dates_list}); "
                    f"live zoster was a single-dose vaccine (superseded by 2-dose recombinant Shingrix in US)"
                )

    return notes
