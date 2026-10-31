"""FHIR R4 Bundle parser and clinical resource extractors for progressive chart condensation.

Handles arbitrary US Core / IPS / Synthea FHIR R4 Bundles:
- Resolves internal references (`Patient`, `Organization`, `Practitioner`, `Encounter`, `Medication`)
- Inverts `Provenance.target[]` into per-resource `source_org` metadata
- Deduplicates Base64 clinical notes across `DocumentReference` and `DiagnosticReport.presentedForm`
- Groups `DiagnosticReport.result[]` panels and `Observation.component[]` multi-part measurements
- Unifies `MedicationRequest`, `MedicationAdministration`, `MedicationStatement`, and `MedicationDispense`
- Classifies SDOH privacy sensitivity (`SDOH/PRIVACY-SENSITIVE` vs `SDOH-SOCIAL`)
- Pre-annotates active condition stage conflicts and immunization duplicate/ACIP anomalies
"""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from .kb_lookup import (
    resolve_condition_code,
    resolve_immunization_code,
    resolve_lab_code,
    resolve_medication_code,
)
from .normalizers import (
    annotate_immunization_anomalies,
    classify_sensitivity,
    classify_vaccine_group,
    detect_condition_conflicts,
    detect_medication_conflicts,
    is_clinic_room_equipment,
    normalize_drug_ingredient,
)

# Resources that carry billing/supply/structural lookup data rather than direct clinical events.
NON_CLINICAL_RESOURCE_TYPES: Set[str] = {
    "Claim",
    "ExplanationOfBenefit",
    "SupplyDelivery",
    "Coverage",
    "Account",
    "ChargeItem",
    "Invoice",
    "PaymentNotice",
    "PaymentReconciliation",
}

LOOKUP_ONLY_RESOURCE_TYPES: Set[str] = {
    "Provenance",
    "Organization",
    "Practitioner",
    "PractitionerRole",
    "Location",
    "Medication",
}

_SURVEY_PANEL_PATTERNS = re.compile(
    r"\b(?:"
    r"phq-?[29]|patient\s+health\s+questionnaire|"
    r"gad-?7|generalized\s+anxiety|"
    r"audit-?c|alcohol\s+use\s+disorder|"
    r"dast-?10|drug\s+abuse\s+screening|"
    r"hark|humiliation|intimate\s+partner|"
    r"prapare|fall\s+scale|morse\s+fall|"
    r"web-?based\s+danger\s+assessment"
    r")\b",
    re.IGNORECASE,
)

_ROUTINE_SCREENING_PROCEDURE_PATTERNS = re.compile(
    r"\b(?:"
    r"depression\s+screening|"
    r"assessment\s+of\s+(?:health\s+and\s+social\s+care\s+needs|anxiety|substance\s+use|alcohol)|"
    r"screening\s+for\s+(?:domestic\s+abuse|drug\s+abuse|occult\s+blood)|"
    r"medication\s+reconciliation|"
    r"assessment\s+using\s+(?:morse\s+fall\s+scale|hark|audit|dast|alcohol\s+use\s+disorders\s+identification)|"
    r"sleep\s+apnea\s+assessment|"
    r"patient\s+referral\s+for\s+dental\s+care|"
    r"oral\s+health\s+education|"
    r"postoperative\s+care\s+for\s+dental|"
    r"examination\s+of\s+gingivae|"
    r"gingival\s+disease\s+prevention|"
    r"removal\s+of\s+(?:subgingival|supragingival)\s+plaque|"
    r"dental\s+(?:fluoride\s+treatment|x-ray|plain\s+x-ray|care|consultation)"
    r")\b",
    re.IGNORECASE,
)

# Synthea note sections that merely repeat structured problem/allergy/med/vital lists verbatim.
_BOILERPLATE_NOTE_SECTIONS: Set[str] = {
    "history of present illness",
    "past medical history",
    "social history",
    "family history",
    "allergies",
    "medications",
    "vital signs",
    "results",
    "procedures",
    "review of systems",
    "physical exam",
}


@dataclass
class NumericPoint:
    resource_uuid: str
    date: str
    code: str
    concept: str
    value: float
    unit: str
    category: str


@dataclass
class ClinicalItem:
    resource_type: str
    resource_uuid: str
    date: Optional[str]
    clinical_date: Optional[str]
    date_kind: str  # "clinical_event" | "administrative" | "unknown"
    section: str    # "patient" | "allergy" | "condition_active" | "condition_resolved" | "med_active" | ...
    code: str
    display: str
    text: str
    mandatory: bool = False
    status: str = ""
    end_date: Optional[str] = None
    source_org: Optional[str] = None
    sensitivity: Optional[str] = None
    conflict_note: Optional[str] = None
    ingredient_key: str = ""
    is_routine: bool = False
    member_uuids: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractedChart:
    patient_id: Optional[str]
    patient_name: str
    birth_date: Optional[str]
    reference_date: Optional[str]
    items: List[ClinicalItem]
    numeric_points: List[NumericPoint]
    excluded_resource_counts: Dict[str, int]
    deduplicated_note_count: int
    total_bundle_entries: int


def _strip_urn(ref: Optional[str]) -> str:
    if not ref:
        return ""
    s = str(ref).strip()
    if s.startswith("urn:uuid:"):
        return s[len("urn:uuid:") :]
    if "/" in s:
        return s.rsplit("/", 1)[-1]
    return s


def _iso_date(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()
    if len(s) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    return None


def _iso_timestamp(val: Optional[str]) -> Optional[str]:
    """Return `YYYY-MM-DDTHH:MM` when an intraday time component is present on an ISO datetime string."""
    if not val:
        return None
    s = str(val).strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})", s)
    if m:
        return f"{m.group(1)}T{m.group(2)}"
    return None


def _coding_pair(codeable: Optional[Mapping[str, Any]]) -> Tuple[str, str]:
    if not isinstance(codeable, Mapping):
        return "", ""
    codings = codeable.get("coding") or []
    code = ""
    display = str(codeable.get("text") or "").strip()
    for c in codings:
        if isinstance(c, Mapping):
            if not code and c.get("code") is not None:
                code = str(c.get("code")).strip()
            if not display and c.get("display"):
                display = str(c.get("display")).strip()
    return code, display


def _codeable_status(val: Any) -> str:
    if isinstance(val, str):
        return val.strip().lower()
    if isinstance(val, Mapping):
        code, display = _coding_pair(val)
        return (code or display).lower()
    return ""


def _categories(res: Mapping[str, Any]) -> List[str]:
    cats: List[str] = []
    raw_cats = res.get("category")
    if isinstance(raw_cats, Mapping):
        raw_cats = [raw_cats]
    for cat in raw_cats or []:
        if isinstance(cat, Mapping):
            code, disp = _coding_pair(cat)
            if code:
                cats.append(code.lower())
            elif disp:
                cats.append(disp.lower())
    return cats


def _format_num(val: Any) -> str:
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        return f"{val:.2f}".rstrip("0").rstrip(".")
    return str(val)


def _format_obs_value(obs: Mapping[str, Any]) -> Tuple[str, Optional[float], str]:
    """Return (formatted_value_with_unit, numeric_float_or_None, unit_str)."""
    components = obs.get("component")
    if isinstance(components, list) and components:
        comp_parts: List[str] = []
        sbp: Optional[float] = None
        dbp: Optional[float] = None
        bp_unit = "mm[Hg]"
        for comp in components:
            if not isinstance(comp, Mapping):
                continue
            c_code, c_disp = _coding_pair(comp.get("code"))
            vq = comp.get("valueQuantity")
            if isinstance(vq, Mapping) and vq.get("value") is not None:
                v = vq.get("value")
                u = str(vq.get("unit") or vq.get("code") or "").strip()
                if c_code == "8480-6" or "systolic" in c_disp.lower():
                    sbp = float(v)
                    bp_unit = u or bp_unit
                elif c_code == "8462-4" or "diastolic" in c_disp.lower():
                    dbp = float(v)
                    bp_unit = u or bp_unit
                comp_parts.append(f"{c_disp or c_code}: {_format_num(v)} {u}".strip())
            else:
                _, vc_disp = _coding_pair(comp.get("valueCodeableConcept"))
                if vc_disp:
                    comp_parts.append(f"{c_disp or c_code}: {vc_disp}")
        if sbp is not None and dbp is not None:
            return f"{_format_num(sbp)}/{_format_num(dbp)} {bp_unit}".strip(), sbp, bp_unit
        if comp_parts:
            return "; ".join(comp_parts), None, ""

    vq = obs.get("valueQuantity")
    if isinstance(vq, Mapping) and vq.get("value") is not None:
        raw_val = vq.get("value")
        unit = str(vq.get("unit") or vq.get("code") or "").strip()
        try:
            num_val = float(raw_val)
        except (TypeError, ValueError):
            num_val = None
        val_str = _format_num(num_val if num_val is not None else raw_val)
        return (f"{val_str} {unit}".strip() if unit else val_str), num_val, unit

    if "valueCodeableConcept" in obs:
        _, disp = _coding_pair(obs.get("valueCodeableConcept"))
        return disp, None, ""
    if "valueString" in obs:
        return str(obs.get("valueString") or "").strip(), None, ""
    if "valueBoolean" in obs:
        return str(obs.get("valueBoolean")), None, ""
    if "valueInteger" in obs:
        v = obs.get("valueInteger")
        return str(v), float(v) if isinstance(v, int) else None, ""
    return "", None, ""


def _extract_unique_note_narrative(decoded_text: str, *, preserve_all_sections: bool = False) -> str:
    """Extract unique encounter narrative (`# Chief Complaint`, `# Assessment and Plan`, etc.)
    from a clinical note. When `preserve_all_sections=True` (non-destructive W12 mode), retains
    all non-empty sections (including `# Medications`, `# Allergies`, `# HPI`) unless they are
    literal empty placeholders (`no active allergies.`, `none`, `n/a`)."""
    lines = [line.strip() for line in (decoded_text or "").splitlines()]
    if not lines:
        return ""

    sections: Dict[str, List[str]] = {}
    current_header = "preamble"
    for line in lines:
        if line.startswith("#"):
            current_header = line.lstrip("#").strip().lower()
            sections.setdefault(current_header, [])
            continue
        if not line or re.match(r"^[-=_*]{3,}$", line):
            continue
        sections.setdefault(current_header, []).append(line)

    kept_parts: List[str] = []
    # Check if structured Markdown sections exist
    has_markdown_headers = any(k != "preamble" for k in sections)
    if has_markdown_headers:
        for header, body_lines in sections.items():
            if header == "preamble":
                continue
            if not preserve_all_sections and header in _BOILERPLATE_NOTE_SECTIONS:
                continue
            compact_body = " ".join(body_lines).strip()
            if not compact_body or compact_body.lower() in {"no active allergies.", "none", "n/a"}:
                continue
            kept_parts.append(f"{header.title()}: {compact_body}")
        if kept_parts:
            summary = " | ".join(kept_parts)
            limit = 520 if preserve_all_sections else 360
            return summary[:limit] + ("..." if len(summary) > limit else "")
        # If every section was boilerplate (e.g. "No complaints."), check chief complaint fallback
        cc = " ".join(sections.get("chief complaint") or []).strip()
        ap = " ".join(sections.get("assessment and plan") or []).strip()
        fallback = " | ".join(p for p in (cc, ap) if p)
        if fallback:
            return fallback[:280]

    # Plain-text non-Synthea note fallback: keep first non-empty lines up to 280 chars
    non_empty = [l for l in lines if l and not l.lower().startswith(("patient:", "date:", "provider:"))]
    joined = " ".join(non_empty[:6]).strip()
    return joined[:280] + ("..." if len(joined) > 280 else "")


def extract_bundle_lossless(
    bundle: Mapping[str, Any],
    *,
    enable_privacy_classification: bool = False,
    filter_room_equipment_devices: bool = False,
) -> ExtractedChart:
    """Non-destructive W12/W15 FHIR Bundle extractor:
    - Zero pre-baked `[conflict_note]` mutations in rendered chart lines
    - Optional `enable_privacy_classification` (`False` by default, deferred to W9)
    - Optional `filter_room_equipment_devices` (`False` by default so clinic-equipment `Device` artifacts are retained non-destructively and inspectable)
    - Preserves intraday timestamps (`YYYY-MM-DDTHH:MM`) on lab/vital observations
    - Preserves all non-empty clinical note sections (`# Medications`, `# HPI`, etc.) plus full decoded text in metadata
    """
    return extract_bundle(
        bundle,
        include_conflict_annotations=False,
        enable_privacy_classification=enable_privacy_classification,
        preserve_intraday_timestamps=True,
        preserve_all_note_sections=True,
        filter_room_equipment_devices=filter_room_equipment_devices,
    )


def extract_bundle(
    bundle: Mapping[str, Any],
    *,
    include_conflict_annotations: bool = True,
    enable_privacy_classification: bool = True,
    preserve_intraday_timestamps: bool = False,
    preserve_all_note_sections: bool = False,
    filter_room_equipment_devices: bool = True,
) -> ExtractedChart:
    """Parse a FHIR R4 Bundle dict into normalized `ClinicalItem` objects and numeric series points."""
    entries = bundle.get("entry") or []
    resources: List[Mapping[str, Any]] = []
    for entry in entries:
        if isinstance(entry, Mapping):
            res = entry.get("resource")
            if isinstance(res, Mapping) and res.get("resourceType"):
                resources.append(res)
    if not resources and bundle.get("resourceType") and bundle.get("resourceType") != "Bundle":
        resources = [bundle]

    excluded_counts: Dict[str, int] = {}
    orgs_by_id: Dict[str, str] = {}
    pracs_by_id: Dict[str, str] = {}
    meds_by_id: Dict[str, Tuple[str, str]] = {}
    encounters_by_id: Dict[str, Mapping[str, Any]] = {}
    obs_by_id: Dict[str, Mapping[str, Any]] = {}
    conds_by_id: Dict[str, str] = {}
    provenance_org_by_target: Dict[str, str] = {}

    patient_res: Optional[Mapping[str, Any]] = None

    # Pass 1: Index lookup tables (Organization, Practitioner, Medication, Encounter, Observation, Condition)
    for res in resources:
        rtype = str(res.get("resourceType") or "")
        rid = str(res.get("id") or "")
        if rtype in NON_CLINICAL_RESOURCE_TYPES:
            excluded_counts[rtype] = excluded_counts.get(rtype, 0) + 1
            continue
        if rtype == "Patient" and patient_res is None:
            patient_res = res
        elif rtype == "Organization" and rid:
            orgs_by_id[rid] = str(res.get("name") or rid).strip()
            excluded_counts[rtype] = excluded_counts.get(rtype, 0) + 1
        elif rtype == "Practitioner" and rid:
            names = res.get("name") or []
            if names and isinstance(names[0], Mapping):
                given = " ".join(names[0].get("given") or [])
                family = str(names[0].get("family") or "")
                pracs_by_id[rid] = f"{given} {family}".strip()
            excluded_counts[rtype] = excluded_counts.get(rtype, 0) + 1
        elif rtype == "Medication" and rid:
            meds_by_id[rid] = _coding_pair(res.get("code"))
            excluded_counts[rtype] = excluded_counts.get(rtype, 0) + 1
        elif rtype == "Encounter" and rid:
            encounters_by_id[rid] = res
        elif rtype == "Observation" and rid:
            obs_by_id[rid] = res
        elif rtype == "Condition" and rid:
            _, c_disp = _coding_pair(res.get("code"))
            if c_disp:
                conds_by_id[rid] = c_disp
        elif rtype in LOOKUP_ONLY_RESOURCE_TYPES and rtype != "Provenance":
            excluded_counts[rtype] = excluded_counts.get(rtype, 0) + 1

    # Pass 1b: Invert Provenance.target[] -> source organization/author
    for res in resources:
        if res.get("resourceType") != "Provenance":
            continue
        excluded_counts["Provenance"] = excluded_counts.get("Provenance", 0) + 1
        agent_name = ""
        for agent in res.get("agent") or []:
            if not isinstance(agent, Mapping):
                continue
            who = agent.get("who") or {}
            on_behalf = agent.get("onBehalfOf") or {}
            org_id = _strip_urn(on_behalf.get("reference") or who.get("reference"))
            disp = str(on_behalf.get("display") or who.get("display") or orgs_by_id.get(org_id) or "").strip()
            if disp:
                agent_name = disp
                break
        if agent_name:
            for tgt in res.get("target") or []:
                if isinstance(tgt, Mapping):
                    tid = _strip_urn(tgt.get("reference"))
                    if tid and tid not in provenance_org_by_target:
                        provenance_org_by_target[tid] = agent_name

    def resolve_source_org(res: Mapping[str, Any]) -> Optional[str]:
        rid = str(res.get("id") or "")
        # Check explicit performer / serviceProvider / recorder first
        sp = res.get("serviceProvider")
        if isinstance(sp, Mapping):
            sp_disp = str(sp.get("display") or orgs_by_id.get(_strip_urn(sp.get("reference"))) or "").strip()
            if sp_disp:
                return sp_disp
        for perf in res.get("performer") or []:
            if isinstance(perf, Mapping):
                actor = perf.get("actor") or perf
                p_ref = _strip_urn(actor.get("reference"))
                p_disp = str(actor.get("display") or orgs_by_id.get(p_ref) or pracs_by_id.get(p_ref) or "").strip()
                if p_disp:
                    return p_disp
        enc_ref = _strip_urn((res.get("encounter") or {}).get("reference"))
        if enc_ref and enc_ref in encounters_by_id:
            enc_sp = encounters_by_id[enc_ref].get("serviceProvider")
            if isinstance(enc_sp, Mapping):
                sp_disp = str(
                    enc_sp.get("display") or orgs_by_id.get(_strip_urn(enc_sp.get("reference"))) or ""
                ).strip()
                if sp_disp:
                    return sp_disp
        return provenance_org_by_target.get(rid)

    items: List[ClinicalItem] = []
    numeric_points: List[NumericPoint] = []
    all_clinical_dates: List[str] = []

    # Pre-scan clinical dates to establish reference_date (latest clinical event in bundle)
    for res in resources:
        rtype = str(res.get("resourceType") or "")
        if rtype in NON_CLINICAL_RESOURCE_TYPES or rtype in LOOKUP_ONLY_RESOURCE_TYPES or rtype == "Patient":
            continue
        for field_name in (
            "effectiveDateTime",
            "occurrenceDateTime",
            "onsetDateTime",
            "authoredOn",
            "performedDateTime",
            "recordedDate",
            "issued",
            "date",
        ):
            d = _iso_date(res.get(field_name))
            if d:
                all_clinical_dates.append(d)
                break
        else:
            for period_field in ("period", "effectivePeriod", "performedPeriod"):
                p = res.get(period_field)
                if isinstance(p, Mapping):
                    d = _iso_date(p.get("end") or p.get("start"))
                    if d:
                        all_clinical_dates.append(d)
                        break

    reference_date = max(all_clinical_dates) if all_clinical_dates else None

    # 1. Extract Patient Demographics Header
    patient_id: Optional[str] = None
    patient_name = "Unknown Patient"
    birth_date: Optional[str] = None
    if patient_res is not None:
        patient_id = str(patient_res.get("id") or "patient-1")
        birth_date = _iso_date(patient_res.get("birthDate"))
        names = patient_res.get("name") or []
        if names and isinstance(names[0], Mapping):
            chosen = next((n for n in names if n.get("use") == "official"), names[0])
            given = " ".join(chosen.get("given") or [])
            family = str(chosen.get("family") or "")
            patient_name = f"{given} {family}".strip() or patient_name
        gender = str(patient_res.get("gender") or "unknown")
        age_str = ""
        if birth_date and reference_date:
            try:
                b_dt = date.fromisoformat(birth_date)
                r_dt = date.fromisoformat(reference_date)
                age_yrs = int((r_dt - b_dt).days // 365.2425)
                age_str = f", Age: {age_yrs}y (as of {reference_date})"
            except ValueError:
                pass
        langs: List[str] = []
        for comm in patient_res.get("communication") or []:
            if isinstance(comm, Mapping):
                _, l_disp = _coding_pair(comm.get("language"))
                if l_disp:
                    langs.append(l_disp)
        lang_str = f", Language: {', '.join(langs)}" if langs else ""
        p_date = reference_date or birth_date
        items.append(
            ClinicalItem(
                resource_type="Patient",
                resource_uuid=patient_id,
                date=p_date,
                clinical_date=None,
                date_kind="administrative",
                section="patient",
                code="patient-demographics",
                display=patient_name,
                text=f"Patient: {patient_name}, DOB: {birth_date or 'unknown'}, Gender: {gender}{age_str}{lang_str}",
                mandatory=True,
                status="active",
            )
        )

    # 2. Extract AllergyIntolerance (100% Mandatory IPS Safety Core)
    for res in resources:
        if res.get("resourceType") != "AllergyIntolerance":
            continue
        rid = str(res.get("id") or "")
        code, disp = _coding_pair(res.get("code"))
        c_status = _codeable_status(res.get("clinicalStatus")) or "active"
        v_status = _codeable_status(res.get("verificationStatus"))
        crit = str(res.get("criticality") or "").strip()
        cats = [str(c) for c in (res.get("category") or []) if c]
        reactions: List[str] = []
        for rxn in res.get("reaction") or []:
            if isinstance(rxn, Mapping):
                sev = str(rxn.get("severity") or "").strip()
                for manif in rxn.get("manifestation") or []:
                    _, m_disp = _coding_pair(manif)
                    if m_disp:
                        reactions.append(f"{m_disp} ({sev})" if sev else m_disp)
        rec_date = _iso_date(res.get("recordedDate") or res.get("onsetDateTime")) or reference_date
        qualifiers = [f"status: {c_status}"]
        if v_status:
            qualifiers.append(f"verification: {v_status}")
        if crit:
            qualifiers.append(f"criticality: {crit}")
        if cats:
            qualifiers.append(f"category: {'/'.join(cats)}")
        if reactions:
            qualifiers.append(f"reaction: {', '.join(reactions)}")
        items.append(
            ClinicalItem(
                resource_type="AllergyIntolerance",
                resource_uuid=rid,
                date=rec_date,
                clinical_date=rec_date,
                date_kind="clinical_event",
                section="allergy",
                code=code or disp,
                display=disp or code or "Unspecified allergy",
                text=f"Allergy: {disp or code} ({', '.join(qualifiers)})",
                mandatory=True,
                status=c_status,
                source_org=resolve_source_org(res),
            )
        )

    # 3. Extract Condition (Active vs Resolved/Historical, with conflict & SDOH sensitivity classification)
    raw_conditions: List[Dict[str, Any]] = []
    for res in resources:
        if res.get("resourceType") != "Condition":
            continue
        rid = str(res.get("id") or "")
        code, disp = _coding_pair(res.get("code"))
        c_status = _codeable_status(res.get("clinicalStatus")) or "active"
        v_status = _codeable_status(res.get("verificationStatus"))
        onset = _iso_date(res.get("onsetDateTime") or res.get("recordedDate"))
        abatement = _iso_date(res.get("abatementDateTime"))
        cats = _categories(res)
        sens = classify_sensitivity(code, disp, cats) if enable_privacy_classification else None
        raw_conditions.append(
            {
                "resourceUuid": rid,
                "code": code,
                "display": disp or code or "Condition",
                "status": c_status,
                "verification": v_status,
                "onset": onset,
                "abatement": abatement,
                "categories": cats,
                "sensitivity": sens,
                "source_org": resolve_source_org(res),
            }
        )

    active_cond_dicts = [
        c for c in raw_conditions if c["status"] in {"active", "recurrence", "relapse"}
    ]
    resolved_cond_dicts = [
        c for c in raw_conditions if c["status"] not in {"active", "recurrence", "relapse"}
    ]
    condition_conflicts = (
        detect_condition_conflicts(active_cond_dicts, resolved_cond_dicts)
        if include_conflict_annotations
        else {}
    )

    for c in raw_conditions:
        rid = c["resourceUuid"]
        is_active = c["status"] in {"active", "recurrence", "relapse"}
        conflict = condition_conflicts.get(rid)
        sens = c["sensitivity"]
        event_date = c["onset"] if is_active else (c["abatement"] or c["onset"])
        event_date = event_date or reference_date
        details = [f"status: {c['status']}"]
        if c["onset"]:
            details.append(f"onset: {c['onset']}")
        if c["abatement"]:
            details.append(f"resolved: {c['abatement']}")
        if c["code"]:
            details.append(f"SNOMED: {c['code']}")
        suffix = ""
        if conflict:
            suffix += f" [{conflict}]"
        if sens == "SDOH/PRIVACY-SENSITIVE":
            suffix += " [sensitivity: SDOH/PRIVACY-SENSITIVE]"
        cond_kb = resolve_condition_code(c["code"], c["display"])
        items.append(
            ClinicalItem(
                resource_type="Condition",
                resource_uuid=rid,
                date=event_date,
                clinical_date=event_date,
                date_kind="clinical_event",
                section="condition_active" if is_active else "condition_resolved",
                code=c["code"] or c["display"],
                display=c["display"],
                text=f"Condition: {c['display']} ({', '.join(details)}){suffix}",
                mandatory=is_active or bool(conflict),
                status=c["status"],
                end_date=c["abatement"],
                source_org=c["source_org"],
                sensitivity=sens,
                conflict_note=conflict,
                is_routine=(sens == "SDOH-SOCIAL"),
                metadata={
                    "onset": c["onset"],
                    "abatement": c["abatement"],
                    "kb_resolution": cond_kb,
                },
            )
        )

    # 4. Extract Unified Medications (MedicationRequest, MedicationAdministration, MedicationStatement, MedicationDispense)
    med_types = {
        "MedicationRequest",
        "MedicationAdministration",
        "MedicationStatement",
        "MedicationDispense",
    }
    raw_meds: List[Dict[str, Any]] = []
    for res in resources:
        rtype = str(res.get("resourceType") or "")
        if rtype not in med_types:
            continue
        rid = str(res.get("id") or "")
        code, disp = _coding_pair(res.get("medicationCodeableConcept"))
        if not disp and isinstance(res.get("medicationReference"), Mapping):
            m_ref = _strip_urn(res["medicationReference"].get("reference"))
            if m_ref in meds_by_id:
                code, disp = meds_by_id[m_ref]
            else:
                disp = str(res["medicationReference"].get("display") or "")
        status = str(res.get("status") or "unknown").strip().lower()
        m_date = _iso_date(
            res.get("authoredOn")
            or res.get("effectiveDateTime")
            or (res.get("effectivePeriod") or {}).get("start")
            or res.get("whenHandedOver")
        ) or reference_date
        end_date = _iso_date((res.get("effectivePeriod") or {}).get("end"))

        # Extract dosage text if available
        dose_strs: List[str] = []
        for d_inst in res.get("dosageInstruction") or []:
            if isinstance(d_inst, Mapping):
                d_txt = str(d_inst.get("text") or "").strip()
                if d_txt:
                    dose_strs.append(d_txt)
        if not dose_strs and isinstance(res.get("dosage"), Mapping):
            d_txt = str(res["dosage"].get("text") or "").strip()
            if d_txt:
                dose_strs.append(d_txt)

        # Extract reason (reasonCode or reasonReference -> Condition)
        reasons: List[str] = []
        for rc in res.get("reasonCode") or []:
            _, r_disp = _coding_pair(rc)
            if r_disp:
                reasons.append(r_disp)
        for rr in res.get("reasonReference") or []:
            if isinstance(rr, Mapping):
                c_id = _strip_urn(rr.get("reference"))
                r_disp = str(rr.get("display") or conds_by_id.get(c_id) or "").strip()
                if r_disp:
                    reasons.append(r_disp)

        setting = "inpatient/administered" if rtype == "MedicationAdministration" else "prescribed"
        is_active = status in {"active", "in-progress", "on-hold"}
        ingredient = normalize_drug_ingredient(disp or code)
        raw_meds.append(
            {
                "resourceType": rtype,
                "resourceUuid": rid,
                "code": code,
                "display": disp or code or "Medication",
                "status": status,
                "is_active": is_active,
                "date": m_date,
                "end_date": end_date,
                "dose_strs": dose_strs,
                "reasons": reasons,
                "setting": setting,
                "ingredient_key": ingredient,
                "source_org": resolve_source_org(res),
            }
        )

    med_conflicts = (
        detect_medication_conflicts([m for m in raw_meds if m["is_active"]])
        if include_conflict_annotations
        else {}
    )
    for m in raw_meds:
        rid = m["resourceUuid"]
        m_conflict = med_conflicts.get(rid)
        med_kb = resolve_medication_code(m["code"], m["display"])
        ing_grp = str(med_kb.get("ingredient") or m["ingredient_key"])
        t_class = str(med_kb.get("therapeutic_class") or "medication")
        quals = [f"status: {m['status']}", f"type: {m['setting']}"]
        if m["dose_strs"]:
            quals.append(f"dosage: {'; '.join(m['dose_strs'][:2])}")
        if m["reasons"]:
            quals.append(f"reason: {', '.join(dict.fromkeys(m['reasons']))}")
        if m["source_org"]:
            quals.append(f"source: {m['source_org']}")
        if m["code"]:
            quals.append(f"RxNorm: {m['code']}")
        suffix = (
            f" [ingredient_group: {ing_grp} ({t_class}); {m_conflict}]"
            if m_conflict
            else ""
        )
        items.append(
            ClinicalItem(
                resource_type=m["resourceType"],
                resource_uuid=rid,
                date=m["date"],
                clinical_date=m["date"],
                date_kind="clinical_event",
                section="med_active" if m["is_active"] else "med_historical",
                code=m["code"] or m["ingredient_key"],
                display=m["display"],
                text=f"Medication: {m['display']} ({', '.join(quals)}){suffix}",
                mandatory=m["is_active"],
                status=m["status"],
                end_date=m["end_date"],
                source_org=m["source_org"],
                conflict_note=m_conflict,
                ingredient_key=m["ingredient_key"],
                metadata={
                    "reasons": m["reasons"],
                    "dosage": m["dose_strs"],
                    "setting": m["setting"],
                    "ingredient_group": ing_grp,
                    "therapeutic_class": t_class,
                    "kb_resolution": med_kb,
                },
            )
        )

    # 5. Extract Immunization (with ACIP schedule & duplicate pre-annotation)
    raw_imms: List[Dict[str, Any]] = []
    for res in resources:
        if res.get("resourceType") != "Immunization":
            continue
        rid = str(res.get("id") or "")
        code, disp = _coding_pair(res.get("vaccineCode"))
        occ_date = _iso_date(res.get("occurrenceDateTime")) or reference_date
        status = str(res.get("status") or "completed").strip().lower()
        raw_imms.append(
            {
                "resourceUuid": rid,
                "code": code,
                "display": disp or code or "Vaccine",
                "date": occ_date,
                "status": status,
                "source_org": resolve_source_org(res),
                "group": classify_vaccine_group(code, disp),
            }
        )

    imm_anomalies = (
        annotate_immunization_anomalies(raw_imms, birth_date)
        if include_conflict_annotations
        else {}
    )
    imms_by_group: Dict[str, List[str]] = {}
    for imm in raw_imms:
        imms_by_group.setdefault(imm["group"], []).append(imm["resourceUuid"])

    for imm in raw_imms:
        rid = imm["resourceUuid"]
        anom = imm_anomalies.get(rid)
        quals = [f"status: {imm['status']}"]
        if imm["code"]:
            quals.append(f"CVX: {imm['code']}")
        if imm["source_org"]:
            quals.append(f"source: {imm['source_org']}")
        suffix = f" [{anom}]" if anom else ""
        # Attach all series UUIDs to ACIP schedule anomaly items so every dose UUID in the series
        # survives even in Tier 4 micro-capsules.
        series_members: Tuple[str, ...] = (
            tuple(imms_by_group.get(imm["group"], [rid]))
            if anom and anom.startswith("acip_schedule_note:")
            else ()
        )
        imm_kb = resolve_immunization_code(imm["code"], imm["display"])
        items.append(
            ClinicalItem(
                resource_type="Immunization",
                resource_uuid=rid,
                date=imm["date"],
                clinical_date=imm["date"],
                date_kind="clinical_event",
                section="immunization",
                code=imm["code"] or imm["group"],
                display=imm["display"],
                text=f"Immunization: {imm['display']} ({', '.join(quals)}){suffix}",
                mandatory=bool(anom),
                status=imm["status"],
                source_org=imm["source_org"],
                conflict_note=anom,
                ingredient_key=imm["group"],
                member_uuids=series_members,
                metadata={
                    "vaccine_group": imm_kb.get("vaccine_group", imm["group"]),
                    "kb_resolution": imm_kb,
                },
            )
        )

    # 6. Deduplicate Base64 Clinical Notes across DocumentReference & DiagnosticReport.presentedForm
    notes_by_hash: Dict[str, Dict[str, Any]] = {}
    dedup_note_count = 0

    for res in resources:
        rtype = str(res.get("resourceType") or "")
        rid = str(res.get("id") or "")
        if rtype == "DocumentReference":
            n_date = _iso_date(res.get("date") or (res.get("context") or {}).get("period", {}).get("start"))
            _, n_type = _coding_pair(res.get("type"))
            for content_item in res.get("content") or []:
                att = (content_item or {}).get("attachment") or {}
                b64_data = att.get("data")
                if isinstance(b64_data, str) and b64_data:
                    digest = hashlib.sha256(b64_data.strip().encode("ascii", errors="ignore")).hexdigest()
                    if digest in notes_by_hash:
                        notes_by_hash[digest]["member_uuids"].append(rid)
                        dedup_note_count += 1
                    else:
                        try:
                            decoded = base64.b64decode(b64_data).decode("utf-8", errors="replace")
                        except Exception:
                            decoded = ""
                        notes_by_hash[digest] = {
                            "primary_type": "DocumentReference",
                            "primary_uuid": rid,
                            "date": n_date or reference_date,
                            "title": n_type or "Clinical Note",
                            "decoded": decoded,
                            "source_org": resolve_source_org(res),
                            "member_uuids": [rid],
                        }
        elif rtype == "DiagnosticReport" and not (res.get("result") or []):
            # Note-wrapper DiagnosticReport with presentedForm[]
            n_date = _iso_date(res.get("effectiveDateTime") or res.get("issued"))
            _, n_type = _coding_pair(res.get("code"))
            for pf in res.get("presentedForm") or []:
                b64_data = (pf or {}).get("data")
                if isinstance(b64_data, str) and b64_data:
                    digest = hashlib.sha256(b64_data.strip().encode("ascii", errors="ignore")).hexdigest()
                    if digest in notes_by_hash:
                        notes_by_hash[digest]["member_uuids"].append(rid)
                        dedup_note_count += 1
                    else:
                        try:
                            decoded = base64.b64decode(b64_data).decode("utf-8", errors="replace")
                        except Exception:
                            decoded = ""
                        notes_by_hash[digest] = {
                            "primary_type": "DiagnosticReport",
                            "primary_uuid": rid,
                            "date": n_date or reference_date,
                            "title": n_type or "Clinical Note",
                            "decoded": decoded,
                            "source_org": resolve_source_org(res),
                            "member_uuids": [rid],
                        }

    for note_info in notes_by_hash.values():
        snippet = _extract_unique_note_narrative(
            note_info["decoded"],
            preserve_all_sections=preserve_all_note_sections,
        )
        if not snippet:
            continue
        members = tuple(dict.fromkeys(note_info["member_uuids"]))
        items.append(
            ClinicalItem(
                resource_type=note_info["primary_type"],
                resource_uuid=note_info["primary_uuid"],
                date=note_info["date"],
                clinical_date=note_info["date"],
                date_kind="clinical_event",
                section="clinical_note",
                code="clinical-note",
                display=note_info["title"],
                text=f"Clinical Note — {note_info['title']}: {snippet}",
                mandatory=False,
                status="current",
                source_org=note_info["source_org"],
                member_uuids=members,
                metadata={
                    "full_text": note_info["decoded"],
                    "duplicate_wrapper_count": len(members),
                },
            )
        )

    # 7. Extract DiagnosticReport Lab & Survey Panels (and track claimed child Observations)
    claimed_obs_ids: Set[str] = set()
    for res in resources:
        if res.get("resourceType") != "DiagnosticReport":
            continue
        result_refs = res.get("result") or []
        if not result_refs:
            continue
        rid = str(res.get("id") or "")
        code, disp = _coding_pair(res.get("code"))
        p_date = _iso_date(res.get("effectiveDateTime") or res.get("issued")) or reference_date
        is_survey = bool(_SURVEY_PANEL_PATTERNS.search(disp or ""))

        member_uuids: List[str] = [rid]
        child_parts: List[str] = []
        child_sensitivities: List[str] = []

        for ref_obj in result_refs:
            if not isinstance(ref_obj, Mapping):
                continue
            oid = _strip_urn(ref_obj.get("reference"))
            if not oid:
                continue
            claimed_obs_ids.add(oid)
            member_uuids.append(oid)
            child_obs = obs_by_id.get(oid)
            if not child_obs:
                r_disp = str(ref_obj.get("display") or "").strip()
                if r_disp:
                    child_parts.append(r_disp)
                continue
            o_code, o_disp = _coding_pair(child_obs.get("code"))
            val_str, num_val, unit = _format_obs_value(child_obs)
            o_cats = _categories(child_obs)
            o_sens = (
                classify_sensitivity(o_code, f"{o_disp} {val_str}", o_cats)
                if enable_privacy_classification
                else None
            )
            if o_sens:
                child_sensitivities.append(o_sens)
            short_label = o_disp or o_code or "item"
            if val_str:
                child_parts.append(f"{short_label}: {val_str}")
            if num_val is not None:
                o_date_raw = child_obs.get("effectiveDateTime") or child_obs.get("issued")
                o_date = (
                    (_iso_timestamp(o_date_raw) if preserve_intraday_timestamps else _iso_date(o_date_raw))
                    or p_date
                )
                if o_date:
                    numeric_points.append(
                        NumericPoint(
                            resource_uuid=oid,
                            date=o_date,
                            code=o_code or short_label,
                            concept=short_label,
                            value=num_val,
                            unit=unit,
                            category=o_cats[0] if o_cats else ("survey" if is_survey else "laboratory"),
                        )
                    )

        panel_sens = (
            (
                "SDOH/PRIVACY-SENSITIVE"
                if "SDOH/PRIVACY-SENSITIVE" in child_sensitivities
                else classify_sensitivity(code, disp)
            )
            if enable_privacy_classification
            else None
        )
        sens_suffix = " [sensitivity: SDOH/PRIVACY-SENSITIVE]" if panel_sens == "SDOH/PRIVACY-SENSITIVE" else ""
        joined_children = "; ".join(child_parts) if child_parts else "completed"
        panel_prefix = "Survey Panel" if is_survey else "Diagnostic Panel"
        panel_kb = resolve_lab_code(code, disp) if not is_survey else {}
        items.append(
            ClinicalItem(
                resource_type="DiagnosticReport",
                resource_uuid=rid,
                date=p_date,
                clinical_date=p_date,
                date_kind="clinical_event",
                section="survey_panel" if is_survey else "lab_panel",
                code=code or disp,
                display=disp or code or "Diagnostic Report",
                text=f"{panel_prefix} — {disp or code}: {joined_children}{sens_suffix}",
                mandatory=False,
                status=str(res.get("status") or "final").lower(),
                source_org=resolve_source_org(res),
                sensitivity=panel_sens,
                is_routine=is_survey,
                member_uuids=tuple(dict.fromkeys(member_uuids)),
                metadata={"kb_resolution": panel_kb} if panel_kb else {},
            )
        )

    # 8. Extract Standalone Observations (Vitals, Unbundled Labs, Social History, Survey answers)
    for res in resources:
        if res.get("resourceType") != "Observation":
            continue
        rid = str(res.get("id") or "")
        if rid in claimed_obs_ids:
            continue
        code, disp = _coding_pair(res.get("code"))
        raw_obs_dt = res.get("effectiveDateTime") or res.get("issued")
        o_date = _iso_date(raw_obs_dt) or reference_date
        o_ts = (_iso_timestamp(raw_obs_dt) if preserve_intraday_timestamps else None) or o_date
        cats = _categories(res)
        val_str, num_val, unit = _format_obs_value(res)
        sens = (
            classify_sensitivity(code, f"{disp} {val_str}", cats)
            if enable_privacy_classification
            else None
        )

        primary_cat = cats[0] if cats else "observation"
        has_components = bool(res.get("component"))
        if has_components and o_date:
            for comp in res.get("component") or []:
                if isinstance(comp, Mapping):
                    c_code, c_disp = _coding_pair(comp.get("code"))
                    vq = comp.get("valueQuantity")
                    if isinstance(vq, Mapping) and vq.get("value") is not None:
                        try:
                            c_num = float(vq["value"])
                            c_unit = str(vq.get("unit") or vq.get("code") or "").strip()
                            numeric_points.append(
                                NumericPoint(
                                    resource_uuid=rid,
                                    date=o_ts or o_date,
                                    code=c_code or c_disp,
                                    concept=c_disp or c_code,
                                    value=c_num,
                                    unit=c_unit,
                                    category=primary_cat,
                                )
                            )
                        except (TypeError, ValueError):
                            pass
        elif num_val is not None and o_date:
            numeric_points.append(
                NumericPoint(
                    resource_uuid=rid,
                    date=o_ts or o_date,
                    code=code or disp,
                    concept=disp or code or "Observation",
                    value=num_val,
                    unit=unit,
                    category=primary_cat,
                )
            )

        if "vital-signs" in cats:
            sec = "vital_sign"
            cls_label = "Vital Sign"
        elif "laboratory" in cats:
            sec = "lab_observation"
            cls_label = "Lab"
        elif "social-history" in cats or sens == "SDOH/PRIVACY-SENSITIVE":
            sec = "social_history"
            cls_label = "Social History"
        elif "survey" in cats or _SURVEY_PANEL_PATTERNS.search(disp or ""):
            sec = "survey_observation"
            cls_label = "Survey"
        else:
            sec = "observation"
            cls_label = "Finding"

        sens_suffix = " [sensitivity: SDOH/PRIVACY-SENSITIVE]" if sens == "SDOH/PRIVACY-SENSITIVE" else ""
        ts_suffix = f" (timestamp: {o_ts})" if (preserve_intraday_timestamps and o_ts and "T" in o_ts) else ""
        body_val = f": {val_str}" if val_str else ""
        obs_kb = resolve_lab_code(code, disp) if sec in {"vital_sign", "lab_observation"} else {}
        items.append(
            ClinicalItem(
                resource_type="Observation",
                resource_uuid=rid,
                date=o_date,
                clinical_date=o_date,
                date_kind="clinical_event",
                section=sec,
                code=code or disp,
                display=disp or code or "Observation",
                text=f"{cls_label} — {disp or code}{body_val}{ts_suffix}{sens_suffix}",
                mandatory=False,
                status=str(res.get("status") or "final").lower(),
                source_org=resolve_source_org(res),
                sensitivity=sens,
                is_routine=(sec in {"survey_observation", "vital_sign"}),
                metadata={
                    "value": val_str,
                    "numeric": num_val,
                    "unit": unit,
                    "category": primary_cat,
                    "timestamp": o_ts,
                    **({"kb_resolution": obs_kb} if obs_kb else {}),
                },
            )
        )

    # 9. Extract Procedures (Surgical/Therapeutic vs Routine Screening)
    for res in resources:
        if res.get("resourceType") != "Procedure":
            continue
        rid = str(res.get("id") or "")
        code, disp = _coding_pair(res.get("code"))
        p_date = _iso_date(
            res.get("performedDateTime")
            or (res.get("performedPeriod") or {}).get("start")
            or (res.get("performedPeriod") or {}).get("end")
        ) or reference_date
        status = str(res.get("status") or "completed").lower()
        is_routine = bool(_ROUTINE_SCREENING_PROCEDURE_PATTERNS.search(disp or ""))
        reasons: List[str] = []
        for rc in res.get("reasonCode") or []:
            _, r_disp = _coding_pair(rc)
            if r_disp:
                reasons.append(r_disp)
        for rr in res.get("reasonReference") or []:
            if isinstance(rr, Mapping):
                c_id = _strip_urn(rr.get("reference"))
                r_disp = str(rr.get("display") or conds_by_id.get(c_id) or "").strip()
                if r_disp:
                    reasons.append(r_disp)
        reason_str = f" (reason: {', '.join(dict.fromkeys(reasons))})" if reasons else ""
        items.append(
            ClinicalItem(
                resource_type="Procedure",
                resource_uuid=rid,
                date=p_date,
                clinical_date=p_date,
                date_kind="clinical_event",
                section="procedure_routine" if is_routine else "procedure_major",
                code=code or disp,
                display=disp or code or "Procedure",
                text=f"Procedure: {disp or code}{reason_str}",
                mandatory=False,
                status=status,
                source_org=resolve_source_org(res),
                is_routine=is_routine,
            )
        )

    # 10. Extract CarePlan, ImagingStudy, Patient-Carried Device, and Encounter
    for res in resources:
        rtype = str(res.get("resourceType") or "")
        rid = str(res.get("id") or "")
        if rtype == "CarePlan":
            status = str(res.get("status") or "active").lower()
            cp_date = _iso_date(
                (res.get("period") or {}).get("start") or res.get("created")
            ) or reference_date
            end_date = _iso_date((res.get("period") or {}).get("end"))
            cats: List[str] = []
            for c_item in res.get("category") or []:
                _, c_disp = _coding_pair(c_item)
                if c_disp and c_disp.lower() != "assess-plan":
                    cats.append(c_disp)
            acts: List[str] = []
            for act in res.get("activity") or []:
                detail = (act or {}).get("detail") or {}
                _, a_disp = _coding_pair(detail.get("code"))
                if a_disp:
                    acts.append(a_disp)
            label = ", ".join(cats) or "Care Plan"
            act_str = f" — activities: {', '.join(acts[:4])}" if acts else ""
            items.append(
                ClinicalItem(
                    resource_type="CarePlan",
                    resource_uuid=rid,
                    date=cp_date,
                    clinical_date=cp_date,
                    date_kind="clinical_event",
                    section="careplan_active" if status == "active" else "careplan_historical",
                    code=label,
                    display=label,
                    text=f"CarePlan: {label} (status: {status}{act_str})",
                    mandatory=False,
                    status=status,
                    end_date=end_date,
                    source_org=resolve_source_org(res),
                )
            )
        elif rtype == "ImagingStudy":
            im_date = _iso_date(res.get("started")) or reference_date
            _, proc_disp = _coding_pair((res.get("procedureCode") or [{}])[0] if res.get("procedureCode") else {})
            series_list = res.get("series") or []
            body_sites: List[str] = []
            modalities: List[str] = []
            for s in series_list:
                if isinstance(s, Mapping):
                    _, bs_disp = _coding_pair(s.get("bodySite"))
                    if bs_disp:
                        body_sites.append(bs_disp)
                    mod = s.get("modality") or {}
                    if isinstance(mod, Mapping) and mod.get("display"):
                        modalities.append(str(mod.get("display")))
            desc = proc_disp or ", ".join(dict.fromkeys(modalities)) or "Imaging Study"
            site_str = f" ({', '.join(dict.fromkeys(body_sites))})" if body_sites else ""
            items.append(
                ClinicalItem(
                    resource_type="ImagingStudy",
                    resource_uuid=rid,
                    date=im_date,
                    clinical_date=im_date,
                    date_kind="clinical_event",
                    section="imaging",
                    code=desc,
                    display=desc,
                    text=f"ImagingStudy: {desc}{site_str}",
                    mandatory=False,
                    status=str(res.get("status") or "available").lower(),
                    source_org=resolve_source_org(res),
                )
            )
        elif rtype == "Device":
            code, disp = _coding_pair(res.get("type"))
            if not disp:
                names = res.get("deviceName") or []
                if names and isinstance(names[0], Mapping):
                    disp = str(names[0].get("name") or "")
            is_room_equip = is_clinic_room_equipment(disp)
            if is_room_equip and filter_room_equipment_devices:
                excluded_counts["Device(clinic_equipment)"] = (
                    excluded_counts.get("Device(clinic_equipment)", 0) + 1
                )
                continue
            elif is_room_equip:
                excluded_counts["Device(clinic_equipment_retained)"] = (
                    excluded_counts.get("Device(clinic_equipment_retained)", 0) + 1
                )
            d_date = _iso_date(res.get("manufactureDate")) or reference_date
            status = str(res.get("status") or "active").lower()
            items.append(
                ClinicalItem(
                    resource_type="Device",
                    resource_uuid=rid,
                    date=d_date,
                    clinical_date=d_date,
                    date_kind="clinical_event",
                    section="device",
                    code=code or disp,
                    display=disp or code or "Medical Device",
                    text=f"Device: {disp or code} (status: {status})",
                    mandatory=(status == "active" and not is_room_equip),
                    status=status,
                    source_org=resolve_source_org(res),
                    is_routine=is_room_equip,
                    metadata={"is_room_equipment": is_room_equip},
                )
            )
        elif rtype == "Encounter":
            enc_period = res.get("period") or {}
            e_start = _iso_date(enc_period.get("start")) or reference_date
            e_end = _iso_date(enc_period.get("end"))
            enc_class = str((res.get("class") or {}).get("code") or "AMB").upper()
            types = res.get("type") or []
            _, t_disp = _coding_pair(types[0] if types else {})
            reasons: List[str] = []
            for rc in res.get("reasonCode") or []:
                _, r_disp = _coding_pair(rc)
                if r_disp:
                    reasons.append(r_disp)
            for rr in res.get("reasonReference") or []:
                if isinstance(rr, Mapping):
                    c_id = _strip_urn(rr.get("reference"))
                    r_disp = str(rr.get("display") or conds_by_id.get(c_id) or "").strip()
                    if r_disp:
                        reasons.append(r_disp)
            org = resolve_source_org(res)
            is_high_acuity = enc_class in {"IMP", "INPATIENT", "EMER", "EMERGENCY"} or any(
                w in (t_disp or "").lower() for w in ("inpatient", "emergency", "admission", "intensive")
            )
            quals = [f"class: {enc_class}"]
            if e_end and e_end != e_start:
                quals.append(f"discharged: {e_end}")
            if reasons:
                quals.append(f"reason: {', '.join(dict.fromkeys(reasons))}")
            if org:
                quals.append(f"facility: {org}")
            items.append(
                ClinicalItem(
                    resource_type="Encounter",
                    resource_uuid=rid,
                    date=e_end or e_start,
                    clinical_date=e_end or e_start,
                    date_kind="clinical_event",
                    section="encounter_acuity" if is_high_acuity else "encounter_routine",
                    code=enc_class,
                    display=t_disp or enc_class,
                    text=f"Encounter: {t_disp or enc_class} ({', '.join(quals)})",
                    mandatory=False,
                    status=str(res.get("status") or "finished").lower(),
                    end_date=e_end,
                    source_org=org,
                    is_routine=not is_high_acuity,
                    metadata={"start": e_start, "end": e_end, "class": enc_class},
                )
            )

    # 11. Detect free-text clinical note vs. structured active MedicationRequest contradictions
    if include_conflict_annotations:
        active_med_items = [it for it in items if it.section == "med_active"]
        note_items = [it for it in items if it.section in {"clinical_note", "note"}]
        hold_re = re.compile(
            r"\b(?:hold|stop|discontinue|held|stopped|do\s+not\s+take)\s+([a-z][a-z0-9-]{2,25})\b",
            re.IGNORECASE,
        )
        for n_item in note_items:
            for match in hold_re.finditer(n_item.text or ""):
                raw_drug = match.group(1)
                norm_ing = normalize_drug_ingredient(raw_drug)
                for m_item in active_med_items:
                    if m_item.ingredient_key == norm_ing or norm_ing in m_item.display.lower():
                        msg = (
                            f"note_medication_contradiction: Clinical note ({n_item.clinical_date or 'recent'}) "
                            f"instructs to hold/discontinue {m_item.display}, but structured MedicationRequest remains ACTIVE"
                        )
                        if not m_item.conflict_note:
                            m_item.conflict_note = msg
                            m_item.text += f" [{msg}]"
                        if n_item.resource_uuid not in m_item.member_uuids:
                            m_item.member_uuids = tuple(list(m_item.member_uuids) + [n_item.resource_uuid])
                        if not n_item.conflict_note:
                            n_item.conflict_note = msg
                            n_item.mandatory = True

    return ExtractedChart(
        patient_id=patient_id,
        patient_name=patient_name,
        birth_date=birth_date,
        reference_date=reference_date,
        items=items,
        numeric_points=numeric_points,
        excluded_resource_counts=excluded_counts,
        deduplicated_note_count=dedup_note_count,
        total_bundle_entries=len(resources),
    )


def _sanitize_raw_fhir_for_browser(obj: Any) -> Any:
    """Return a JSON-serializable copy of a FHIR resource with large Base64 payloads summarized."""
    if isinstance(obj, Mapping):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if k == "data" and isinstance(v, str) and len(v) > 120:
                out[k] = f"[Base64 payload ({len(v)} chars) — full decoded text shown above]"
            elif k == "div" and isinstance(v, str) and len(v) > 240:
                out[k] = v[:240] + "... [XHTML narrative truncated]"
            elif k == "extension" and isinstance(v, list) and len(v) > 3:
                out[k] = [_sanitize_raw_fhir_for_browser(x) for x in v[:3]] + [
                    {"note": f"... +{len(v) - 3} additional US Core extensions"}
                ]
            else:
                out[str(k)] = _sanitize_raw_fhir_for_browser(v)
        return out
    if isinstance(obj, list):
        if len(obj) > 25:
            return [_sanitize_raw_fhir_for_browser(x) for x in obj[:25]] + [
                f"... +{len(obj) - 25} more items"
            ]
        return [_sanitize_raw_fhir_for_browser(x) for x in obj]
    return obj


def build_fhir_browser_index(
    bundle: Mapping[str, Any],
    *,
    max_observations: int = 80,
    max_admin_claims: int = 10,
) -> Dict[str, Any]:
    """Flatten a FHIR R4 Bundle into a clean 1-level Core-Resource Browser index.

    Abstracts deep FHIR R4 nesting (`code.coding[0]`, `valueQuantity`, `dosageInstruction`,
    Base64 `DocumentReference.content[].attachment.data`, `Provenance` org links) into a
    single-level searchable table grouped by core clinical category, while preserving
    each resource's raw FHIR JSON structure and decoded note text for click-to-inspect.
    """
    entries = bundle.get("entry") or []
    raw_resources: List[Tuple[int, Mapping[str, Any]]] = []
    for idx, entry in enumerate(entries, start=1):
        if isinstance(entry, Mapping):
            res = entry.get("resource")
            if isinstance(res, Mapping) and res.get("resourceType"):
                raw_resources.append((idx, res))

    orgs_by_id: Dict[str, str] = {}
    pracs_by_id: Dict[str, str] = {}
    meds_by_id: Dict[str, Tuple[str, str]] = {}
    conds_by_id: Dict[str, str] = {}
    encounters_by_id: Dict[str, Mapping[str, Any]] = {}
    provenance_org_by_target: Dict[str, str] = {}
    b64_hash_counts: Dict[str, List[str]] = {}

    for _, res in raw_resources:
        rtype = str(res.get("resourceType") or "")
        rid = str(res.get("id") or "")
        if rtype == "Organization" and rid:
            orgs_by_id[rid] = str(res.get("name") or rid).strip()
        elif rtype == "Practitioner" and rid:
            names = res.get("name") or []
            if names and isinstance(names[0], Mapping):
                given = " ".join(names[0].get("given") or [])
                family = str(names[0].get("family") or "")
                pracs_by_id[rid] = f"{given} {family}".strip()
        elif rtype == "Medication" and rid:
            meds_by_id[rid] = _coding_pair(res.get("code"))
        elif rtype == "Condition" and rid:
            _, c_disp = _coding_pair(res.get("code"))
            if c_disp:
                conds_by_id[rid] = c_disp
        elif rtype == "Encounter" and rid:
            encounters_by_id[rid] = res
        elif rtype in {"DocumentReference", "DiagnosticReport"}:
            b64_list: List[str] = []
            for c_item in res.get("content") or []:
                b64_val = ((c_item or {}).get("attachment") or {}).get("data")
                if isinstance(b64_val, str) and b64_val:
                    b64_list.append(b64_val)
            for pf in res.get("presentedForm") or []:
                b64_val = (pf or {}).get("data")
                if isinstance(b64_val, str) and b64_val:
                    b64_list.append(b64_val)
            for b64_val in b64_list:
                digest = hashlib.sha256(b64_val.strip().encode("ascii", errors="ignore")).hexdigest()
                b64_hash_counts.setdefault(digest, []).append(rid)

    for _, res in raw_resources:
        if res.get("resourceType") != "Provenance":
            continue
        agent_name = ""
        for agent in res.get("agent") or []:
            if isinstance(agent, Mapping):
                who = agent.get("who") or {}
                on_behalf = agent.get("onBehalfOf") or {}
                org_id = _strip_urn(on_behalf.get("reference") or who.get("reference"))
                disp = str(on_behalf.get("display") or who.get("display") or orgs_by_id.get(org_id) or "").strip()
                if disp:
                    agent_name = disp
                    break
        if agent_name:
            for tgt in res.get("target") or []:
                if isinstance(tgt, Mapping):
                    tid = _strip_urn(tgt.get("reference"))
                    if tid and tid not in provenance_org_by_target:
                        provenance_org_by_target[tid] = agent_name

    def _resolve_org(res: Mapping[str, Any]) -> str:
        rid = str(res.get("id") or "")
        sp = res.get("serviceProvider")
        if isinstance(sp, Mapping):
            sp_disp = str(sp.get("display") or orgs_by_id.get(_strip_urn(sp.get("reference"))) or "").strip()
            if sp_disp:
                return sp_disp
        for perf in res.get("performer") or []:
            if isinstance(perf, Mapping):
                actor = perf.get("actor") or perf
                p_ref = _strip_urn(actor.get("reference"))
                p_disp = str(actor.get("display") or orgs_by_id.get(p_ref) or pracs_by_id.get(p_ref) or "").strip()
                if p_disp:
                    return p_disp
        enc_ref = _strip_urn((res.get("encounter") or {}).get("reference"))
        if enc_ref and enc_ref in encounters_by_id:
            enc_sp = encounters_by_id[enc_ref].get("serviceProvider")
            if isinstance(enc_sp, Mapping):
                sp_disp = str(
                    enc_sp.get("display") or orgs_by_id.get(_strip_urn(enc_sp.get("reference"))) or ""
                ).strip()
                if sp_disp:
                    return sp_disp
        return provenance_org_by_target.get(rid) or ""

    category_order = [
        "All Core Clinical",
        "Conditions",
        "Medications",
        "Allergies",
        "Labs & Vitals",
        "Clinical Notes",
        "Immunizations",
        "Visits & Procedures",
        "Devices",
        "Patient & Care Team",
        "Claims & Billing",
    ]
    total_category_counts: Dict[str, int] = {cat: 0 for cat in category_order}
    all_rows: List[Dict[str, Any]] = []

    for idx, res in raw_resources:
        rtype = str(res.get("resourceType") or "")
        rid = str(res.get("id") or f"entry-{idx}")
        org = _resolve_org(res)
        flags: List[str] = []
        decoded_note = ""
        is_core = True

        if rtype == "Patient":
            category = "Patient & Care Team"
            names = res.get("name") or []
            p_name = "Patient"
            if names and isinstance(names[0], Mapping):
                chosen = next((n for n in names if n.get("use") == "official"), names[0])
                p_name = f"{' '.join(chosen.get('given') or [])} {chosen.get('family') or ''}".strip() or "Patient"
            r_date = _iso_date(res.get("birthDate")) or ""
            display = p_name
            summary = f"DOB: {r_date or 'unknown'} | Gender: {res.get('gender') or 'unknown'}"
            status = "active"
            code_str = "FHIR Patient"
        elif rtype in {"Organization", "Practitioner", "Location", "Provenance"}:
            category = "Patient & Care Team"
            is_core = False
            r_date = _iso_date(res.get("recorded")) or ""
            if rtype == "Organization":
                display = str(res.get("name") or rid)
                summary = "Care organization / facility lookup resource"
            elif rtype == "Practitioner":
                display = pracs_by_id.get(rid) or rid
                summary = "Clinician / prescriber lookup resource"
            elif rtype == "Provenance":
                display = f"Provenance record ({org or 'author link'})"
                summary = f"Links {len(res.get('target') or [])} clinical resources to their source organization"
            else:
                display = str(res.get("name") or rtype)
                summary = f"{rtype} lookup metadata"
            status = "lookup"
            code_str = rtype
        elif rtype == "Condition":
            category = "Conditions"
            code, disp = _coding_pair(res.get("code"))
            c_status = _codeable_status(res.get("clinicalStatus")) or "active"
            onset = _iso_date(res.get("onsetDateTime") or res.get("recordedDate")) or ""
            abatement = _iso_date(res.get("abatementDateTime")) or ""
            r_date = onset or abatement
            display = disp or code or "Condition"
            summary = f"Onset: {onset or 'recorded'}" + (f" → Resolved: {abatement}" if abatement else " (Active on problem list)")
            status = c_status
            code_str = f"SNOMED {code}" if code else "Condition"
            sens = classify_sensitivity(code, disp, _categories(res))
            if sens == "SDOH/PRIVACY-SENSITIVE":
                flags.append("Sensitive / SDOH Record")
        elif rtype in {"MedicationRequest", "MedicationAdministration", "MedicationStatement", "MedicationDispense", "Medication"}:
            category = "Medications"
            if rtype == "Medication":
                is_core = False
                code, disp = _coding_pair(res.get("code"))
                r_date = ""
                display = disp or code or "Medication definition"
                summary = "Referenced medication code definition"
                status = str(res.get("status") or "active").lower()
                code_str = f"RxNorm {code}" if code else "Medication"
            else:
                code, disp = _coding_pair(res.get("medicationCodeableConcept"))
                if not disp and isinstance(res.get("medicationReference"), Mapping):
                    m_ref = _strip_urn(res["medicationReference"].get("reference"))
                    if m_ref in meds_by_id:
                        code, disp = meds_by_id[m_ref]
                    else:
                        disp = str(res["medicationReference"].get("display") or "")
                r_date = _iso_date(
                    res.get("authoredOn")
                    or res.get("effectiveDateTime")
                    or (res.get("effectivePeriod") or {}).get("start")
                ) or ""
                status = str(res.get("status") or "unknown").lower()
                doses = [
                    str(d.get("text") or "").strip()
                    for d in (res.get("dosageInstruction") or [])
                    if isinstance(d, Mapping) and d.get("text")
                ]
                reasons: List[str] = []
                for rc in res.get("reasonCode") or []:
                    _, r_disp = _coding_pair(rc)
                    if r_disp:
                        reasons.append(r_disp)
                for rr in res.get("reasonReference") or []:
                    if isinstance(rr, Mapping):
                        c_id = _strip_urn(rr.get("reference"))
                        r_disp = str(rr.get("display") or conds_by_id.get(c_id) or "").strip()
                        if r_disp:
                            reasons.append(r_disp)
                display = disp or code or rtype
                parts = []
                if doses:
                    parts.append(doses[0])
                if reasons:
                    parts.append(f"Reason: {', '.join(dict.fromkeys(reasons))}")
                summary = " | ".join(parts) if parts else f"{rtype} ({status})"
                code_str = f"RxNorm {code}" if code else rtype
        elif rtype == "AllergyIntolerance":
            category = "Allergies"
            code, disp = _coding_pair(res.get("code"))
            r_date = _iso_date(res.get("recordedDate") or res.get("onsetDateTime")) or ""
            status = _codeable_status(res.get("clinicalStatus")) or "active"
            crit = str(res.get("criticality") or "").strip()
            rxns: List[str] = []
            for rxn in res.get("reaction") or []:
                if isinstance(rxn, Mapping):
                    for manif in rxn.get("manifestation") or []:
                        _, m_disp = _coding_pair(manif)
                        if m_disp:
                            rxns.append(m_disp)
            display = disp or code or "Allergy"
            summary = f"Criticality: {crit or 'unspecified'}" + (f" | Reaction: {', '.join(rxns)}" if rxns else "")
            code_str = code or "Allergy"
        elif rtype == "Immunization":
            category = "Immunizations"
            code, disp = _coding_pair(res.get("vaccineCode"))
            r_date = _iso_date(res.get("occurrenceDateTime")) or ""
            status = str(res.get("status") or "completed").lower()
            display = disp or code or "Vaccine"
            summary = f"Vaccine group: {classify_vaccine_group(code, disp)}"
            code_str = f"CVX {code}" if code else "Immunization"
        elif rtype == "DocumentReference" or (rtype == "DiagnosticReport" and not (res.get("result") or [])):
            category = "Clinical Notes"
            r_date = _iso_date(
                res.get("date")
                or res.get("effectiveDateTime")
                or res.get("issued")
                or ((res.get("context") or {}).get("period") or {}).get("start")
            ) or ""
            code, disp = _coding_pair(res.get("type") if rtype == "DocumentReference" else res.get("code"))
            status = str(res.get("status") or "current").lower()
            b64_payload = ""
            for c_item in res.get("content") or []:
                b64_val = ((c_item or {}).get("attachment") or {}).get("data")
                if isinstance(b64_val, str) and b64_val:
                    b64_payload = b64_val
                    break
            if not b64_payload:
                for pf in res.get("presentedForm") or []:
                    b64_val = (pf or {}).get("data")
                    if isinstance(b64_val, str) and b64_val:
                        b64_payload = b64_val
                        break
            if b64_payload:
                digest = hashlib.sha256(b64_payload.strip().encode("ascii", errors="ignore")).hexdigest()
                dup_ids = b64_hash_counts.get(digest) or []
                if len(dup_ids) > 1:
                    flags.append(f"Byte-Identical Base64 Note ({len(dup_ids)} copies in bundle)")
                try:
                    decoded_note = base64.b64decode(b64_payload).decode("utf-8", errors="replace").strip()
                except Exception:
                    decoded_note = ""
            display = disp or "Clinical Note"
            preview = " ".join(decoded_note.splitlines()[:3]).strip()[:140]
            summary = f"{preview}..." if preview else f"{rtype} clinical narrative"
            code_str = f"LOINC {code}" if code else rtype
        elif rtype in {"Observation", "DiagnosticReport"}:
            category = "Labs & Vitals"
            code, disp = _coding_pair(res.get("code"))
            raw_dt = res.get("effectiveDateTime") or res.get("issued")
            r_date = _iso_timestamp(raw_dt) or _iso_date(raw_dt) or ""
            status = str(res.get("status") or "final").lower()
            display = disp or code or rtype
            if rtype == "Observation":
                val_str, _, _ = _format_obs_value(res)
                cats = _categories(res)
                summary = f"Value: {val_str}" if val_str else f"Category: {', '.join(cats) or 'observation'}"
                sens = classify_sensitivity(code, f"{disp} {val_str}", cats)
                if sens == "SDOH/PRIVACY-SENSITIVE":
                    flags.append("Sensitive / SDOH Observation")
            else:
                refs = res.get("result") or []
                summary = f"Panel grouping {len(refs)} child Observation result(s)"
            code_str = f"LOINC {code}" if code else rtype
        elif rtype == "Device":
            category = "Devices"
            code, disp = _coding_pair(res.get("type"))
            if not disp:
                names = res.get("deviceName") or []
                if names and isinstance(names[0], Mapping):
                    disp = str(names[0].get("name") or "")
            r_date = _iso_date(res.get("manufactureDate")) or ""
            status = str(res.get("status") or "active").lower()
            display = disp or code or "Device"
            is_room = is_clinic_room_equipment(disp)
            if is_room:
                flags.append("Non-Implantable Clinic Room Equipment Artifact")
                summary = "Facility/room equipment exported as a Device resource (not a patient implant)"
            else:
                summary = "Patient-carried / implanted medical device"
            code_str = f"SNOMED {code}" if code else "Device"
        elif rtype in {"Encounter", "Procedure", "CarePlan", "ImagingStudy"}:
            category = "Visits & Procedures"
            status = str(res.get("status") or "completed").lower()
            if rtype == "Encounter":
                enc_p = res.get("period") or {}
                r_date = _iso_date(enc_p.get("start") or enc_p.get("end")) or ""
                enc_class = str((res.get("class") or {}).get("code") or "AMB").upper()
                types = res.get("type") or []
                _, t_disp = _coding_pair(types[0] if types else {})
                display = t_disp or f"Encounter ({enc_class})"
                summary = f"Class: {enc_class}" + (f" | End: {_iso_date(enc_p.get('end'))}" if enc_p.get("end") else "")
                code_str = f"Encounter ({enc_class})"
            elif rtype == "Procedure":
                code, disp = _coding_pair(res.get("code"))
                r_date = _iso_date(
                    res.get("performedDateTime")
                    or (res.get("performedPeriod") or {}).get("start")
                ) or ""
                display = disp or code or "Procedure"
                summary = "Surgical / diagnostic / clinical procedure"
                code_str = f"SNOMED {code}" if code else "Procedure"
            elif rtype == "CarePlan":
                r_date = _iso_date((res.get("period") or {}).get("start") or res.get("created")) or ""
                cats_cp = [
                    _coding_pair(c)[1]
                    for c in (res.get("category") or [])
                    if _coding_pair(c)[1] and _coding_pair(c)[1].lower() != "assess-plan"
                ]
                display = ", ".join(cats_cp) or "Care Plan"
                summary = f"CarePlan ({status})"
                code_str = "CarePlan"
            else:
                r_date = _iso_date(res.get("started")) or ""
                display = "Imaging Study"
                summary = "Diagnostic imaging study"
                code_str = "ImagingStudy"
        else:
            category = "Claims & Billing"
            is_core = False
            r_date = _iso_date(res.get("created") or (res.get("billablePeriod") or {}).get("start")) or ""
            status = str(res.get("status") or "active").lower()
            display = f"{rtype} ({res.get('use') or 'billing'})"
            total_obj = res.get("total")
            if isinstance(total_obj, Mapping) and total_obj.get("value") is not None:
                summary = f"Billing / administrative record — Total: {total_obj.get('value')} {total_obj.get('currency') or 'USD'}"
            else:
                summary = "Non-clinical administrative / billing record"
            code_str = rtype
            flags.append("Non-Clinical Billing Resource")

        total_category_counts[category] = total_category_counts.get(category, 0) + 1
        if is_core:
            total_category_counts["All Core Clinical"] += 1

        all_rows.append(
            {
                "index": idx,
                "id": rid,
                "resourceType": rtype,
                "category": category,
                "isCoreClinical": is_core,
                "date": r_date,
                "display": display,
                "summary": summary,
                "status": status,
                "code": code_str,
                "sourceOrg": org,
                "flags": flags,
                "decodedNoteText": decoded_note[:3500] if decoded_note else "",
                "rawJson": _sanitize_raw_fhir_for_browser(res),
            }
        )

    # Keep 100% of Patient, Allergies, Conditions, Active Medications, Immunizations, and Devices,
    # while capping bulk historical renewals/encounters/notes/observations so the static browser payload stays <2 MB.
    kept_rows: List[Dict[str, Any]] = []
    obs_rows: List[Dict[str, Any]] = []
    admin_rows: List[Dict[str, Any]] = []
    hist_med_rows: List[Dict[str, Any]] = []
    visit_rows: List[Dict[str, Any]] = []
    note_rows: List[Dict[str, Any]] = []

    for row in all_rows:
        if row["resourceType"] in {"Observation", "DiagnosticReport"} and row["category"] == "Labs & Vitals":
            obs_rows.append(row)
        elif not row["isCoreClinical"]:
            admin_rows.append(row)
        elif row["category"] == "Medications" and row["status"] not in {"active", "in-progress", "on-hold"}:
            hist_med_rows.append(row)
        elif row["category"] == "Visits & Procedures":
            visit_rows.append(row)
        elif row["category"] == "Clinical Notes":
            note_rows.append(row)
        else:
            kept_rows.append(row)

    _by_date_desc = lambda r: (r["date"] or "", r["index"])
    kept_rows.extend(sorted(note_rows, key=_by_date_desc, reverse=True)[:20])
    kept_rows.extend(sorted(hist_med_rows, key=_by_date_desc, reverse=True)[:30])
    kept_rows.extend(sorted(visit_rows, key=_by_date_desc, reverse=True)[:35])
    kept_rows.extend(sorted(obs_rows, key=_by_date_desc, reverse=True)[:55])
    kept_rows.extend(admin_rows[:max_admin_claims])
    kept_rows.sort(key=lambda r: (r["isCoreClinical"] is False, -(int(r["date"][:4]) if r["date"] and r["date"][:4].isdigit() else 0), r["index"]))

    return {
        "total_bundle_entries": len(raw_resources),
        "category_counts": total_category_counts,
        "displayed_resource_count": len(kept_rows),
        "omitted_bulk_observation_count": max(0, len(obs_rows) - max_observations),
        "omitted_bulk_admin_count": max(0, len(admin_rows) - max_admin_claims),
        "resources": kept_rows,
    }

