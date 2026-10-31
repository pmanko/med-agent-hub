"""Simple 1-level Core-Resource FHIR R4 Browser index builder (`W15.1`).

Abstracts deep FHIR R4 nesting (`code.coding[0]`, `valueQuantity`, `dosageInstruction`,
Base64 `DocumentReference.content[].attachment.data`, `Provenance` org links) into a
single-level searchable table grouped by core clinical category, while preserving
each resource's raw FHIR JSON structure and decoded note text for click-to-inspect.
"""

from __future__ import annotations

import base64
import hashlib
from typing import Any, Dict, List, Mapping, Tuple

from .extractors import (
    _categories,
    _codeable_status,
    _coding_pair,
    _format_obs_value,
    _iso_date,
    _iso_timestamp,
    _strip_urn,
)
from .normalizers import (
    classify_sensitivity,
    classify_vaccine_group,
    is_clinic_room_equipment,
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
    """Flatten a FHIR R4 Bundle into a clean 1-level Core-Resource Browser index."""
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
                disp = str(
                    on_behalf.get("display") or who.get("display") or orgs_by_id.get(org_id) or ""
                ).strip()
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
            sp_disp = str(
                sp.get("display") or orgs_by_id.get(_strip_urn(sp.get("reference"))) or ""
            ).strip()
            if sp_disp:
                return sp_disp
        for perf in res.get("performer") or []:
            if isinstance(perf, Mapping):
                actor = perf.get("actor") or perf
                p_ref = _strip_urn(actor.get("reference"))
                p_disp = str(
                    actor.get("display") or orgs_by_id.get(p_ref) or pracs_by_id.get(p_ref) or ""
                ).strip()
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
                p_name = (
                    f"{' '.join(chosen.get('given') or [])} {chosen.get('family') or ''}".strip()
                    or "Patient"
                )
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
                summary = (
                    f"Links {len(res.get('target') or [])} clinical resources to their source organization"
                )
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
            summary = f"Onset: {onset or 'recorded'}" + (
                f" → Resolved: {abatement}" if abatement else " (Active on problem list)"
            )
            status = c_status
            code_str = f"SNOMED {code}" if code else "Condition"
            sens = classify_sensitivity(code, disp, _categories(res))
            if sens == "SDOH/PRIVACY-SENSITIVE":
                flags.append("Sensitive / SDOH Record")
        elif rtype in {
            "MedicationRequest",
            "MedicationAdministration",
            "MedicationStatement",
            "MedicationDispense",
            "Medication",
        }:
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
                r_date = (
                    _iso_date(
                        res.get("authoredOn")
                        or res.get("effectiveDateTime")
                        or (res.get("effectivePeriod") or {}).get("start")
                    )
                    or ""
                )
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
            summary = f"Criticality: {crit or 'unspecified'}" + (
                f" | Reaction: {', '.join(rxns)}" if rxns else ""
            )
            code_str = code or "Allergy"
        elif rtype == "Immunization":
            category = "Immunizations"
            code, disp = _coding_pair(res.get("vaccineCode"))
            r_date = _iso_date(res.get("occurrenceDateTime")) or ""
            status = str(res.get("status") or "completed").lower()
            display = disp or code or "Vaccine"
            summary = f"Vaccine group: {classify_vaccine_group(code, disp)}"
            code_str = f"CVX {code}" if code else "Immunization"
        elif rtype == "DocumentReference" or (
            rtype == "DiagnosticReport" and not (res.get("result") or [])
        ):
            category = "Clinical Notes"
            r_date = (
                _iso_date(
                    res.get("date")
                    or res.get("effectiveDateTime")
                    or res.get("issued")
                    or ((res.get("context") or {}).get("period") or {}).get("start")
                )
                or ""
            )
            code, disp = _coding_pair(
                res.get("type") if rtype == "DocumentReference" else res.get("code")
            )
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
                digest = hashlib.sha256(
                    b64_payload.strip().encode("ascii", errors="ignore")
                ).hexdigest()
                dup_ids = b64_hash_counts.get(digest) or []
                if len(dup_ids) > 1:
                    flags.append(f"Byte-Identical Base64 Note ({len(dup_ids)} copies in bundle)")
                try:
                    decoded_note = (
                        base64.b64decode(b64_payload).decode("utf-8", errors="replace").strip()
                    )
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
                summary = (
                    f"Value: {val_str}"
                    if val_str
                    else f"Category: {', '.join(cats) or 'observation'}"
                )
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
                summary = (
                    "Facility/room equipment exported as a Device resource (not a patient implant)"
                )
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
                summary = f"Class: {enc_class}" + (
                    f" | End: {_iso_date(enc_p.get('end'))}" if enc_p.get("end") else ""
                )
                code_str = f"Encounter ({enc_class})"
            elif rtype == "Procedure":
                code, disp = _coding_pair(res.get("code"))
                r_date = (
                    _iso_date(
                        res.get("performedDateTime")
                        or (res.get("performedPeriod") or {}).get("start")
                    )
                    or ""
                )
                display = disp or code or "Procedure"
                summary = "Surgical / diagnostic / clinical procedure"
                code_str = f"SNOMED {code}" if code else "Procedure"
            elif rtype == "CarePlan":
                r_date = (
                    _iso_date((res.get("period") or {}).get("start") or res.get("created")) or ""
                )
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
            r_date = (
                _iso_date(res.get("created") or (res.get("billablePeriod") or {}).get("start"))
                or ""
            )
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

    kept_rows: List[Dict[str, Any]] = []
    obs_rows: List[Dict[str, Any]] = []
    admin_rows: List[Dict[str, Any]] = []
    hist_med_rows: List[Dict[str, Any]] = []
    visit_rows: List[Dict[str, Any]] = []
    note_rows: List[Dict[str, Any]] = []

    for row in all_rows:
        if (
            row["resourceType"] in {"Observation", "DiagnosticReport"}
            and row["category"] == "Labs & Vitals"
        ):
            obs_rows.append(row)
        elif not row["isCoreClinical"]:
            admin_rows.append(row)
        elif row["category"] == "Medications" and row["status"] not in {
            "active",
            "in-progress",
            "on-hold",
        }:
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
    kept_rows.sort(
        key=lambda r: (
            r["isCoreClinical"] is False,
            -(int(r["date"][:4]) if r["date"] and r["date"][:4].isdigit() else 0),
            r["index"],
        )
    )

    return {
        "total_bundle_entries": len(raw_resources),
        "category_counts": total_category_counts,
        "displayed_resource_count": len(kept_rows),
        "omitted_bulk_observation_count": max(0, len(obs_rows) - max_observations),
        "omitted_bulk_admin_count": max(0, len(admin_rows) - max_admin_claims),
        "resources": kept_rows,
    }
