"""Thin HTTP client for the standalone clinical DDI & medication safety microservice."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import httpx

from . import drug_safety

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECONDS = 3.0


def ddi_service_url() -> str:
    """Return the configured DDI microservice base URL, or empty string if unset."""
    return (os.getenv("DDI_SERVICE_URL") or "").strip().rstrip("/")


def ddi_service_enabled() -> bool:
    """Return True when standalone DDI microservice integration is configured."""
    return bool(ddi_service_url())


def _timeout_seconds() -> float:
    raw = (os.getenv("DDI_SERVICE_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        parsed = float(raw)
        return parsed if parsed > 0 else _DEFAULT_TIMEOUT_SECONDS
    except ValueError:
        return _DEFAULT_TIMEOUT_SECONDS


@dataclass
class DdiPatientContext:
    """Resolved patient safety context from ``ddi-service`` with per-turn check caching."""

    raw_context: Dict[str, Any] = field(default_factory=dict)
    package: Dict[str, Any] = field(default_factory=dict)
    proactive_warnings: List[drug_safety.SafetyWarning] = field(default_factory=list)
    medication_safety_facts: str = ""
    available: bool = True
    issue: Optional[str] = None
    age_years: Optional[int] = None
    active_order_count: int = 0
    mapped_active_order_count: int = 0
    mapping_complete: bool = False
    exposure_complete: bool = False
    identity_confidence: str = "unavailable"
    active_drug_atc_codes: set[str] = field(default_factory=set)
    _check_cache: Dict[Tuple[str, str], drug_safety.SafetyCheckResult] = field(
        default_factory=dict, repr=False
    )


def _extract_warnings(raw_warnings: Any) -> List[drug_safety.SafetyWarning]:
    out: List[drug_safety.SafetyWarning] = []
    if not isinstance(raw_warnings, Sequence) or isinstance(raw_warnings, (str, bytes)):
        return out
    for item in raw_warnings:
        if not isinstance(item, Mapping):
            continue
        wtype = str(item.get("type") or "").strip()
        drug = str(item.get("drug") or "").strip()
        detail = str(item.get("detail") or "").strip()
        if wtype and detail:
            out.append(drug_safety.SafetyWarning(type=wtype, drug=drug, detail=detail))
    return out


def _apply_prepare_response(
    chart_text: str,
    mappings: List[Dict[str, Any]],
    payload: Mapping[str, Any],
    *,
    exposure_complete: bool,
) -> Tuple[str, List[Dict[str, Any]], DdiPatientContext]:
    raw_ctx = dict(payload.get("patient_context") or {})
    pkg = dict(payload.get("package") or {})
    proactive = _extract_warnings(payload.get("proactive_warnings"))
    safety_facts = str(payload.get("medication_safety_facts") or "").strip()

    ctx = DdiPatientContext(
        raw_context=raw_ctx,
        package=pkg,
        proactive_warnings=proactive,
        medication_safety_facts=safety_facts,
        available=True,
        age_years=raw_ctx.get("age_years"),
        active_order_count=int(raw_ctx.get("active_order_count") or 0),
        mapped_active_order_count=int(raw_ctx.get("mapped_active_order_count") or 0),
        mapping_complete=bool(raw_ctx.get("mapping_complete")),
        exposure_complete=bool(raw_ctx.get("exposure_complete", exposure_complete)),
        identity_confidence=str(raw_ctx.get("identity_confidence") or "unavailable"),
        active_drug_atc_codes={
            str(c).strip().upper()
            for c in (raw_ctx.get("active_drug_atc_codes") or ())
            if str(c).strip()
        },
    )

    refs = payload.get("drug_references") or ()
    if not refs or not chart_text:
        return chart_text, list(mappings), ctx

    new_mappings = list(mappings)
    appended_lines: List[str] = []
    seen_texts = {str(m.get("text") or "").strip() for m in new_mappings}
    next_index = len(new_mappings) + 1

    for raw_ref in refs:
        if not isinstance(raw_ref, Mapping):
            continue
        text = str(raw_ref.get("text") or "").strip()
        if not text or text in seen_texts:
            continue
        seen_texts.add(text)
        drug_label = str(raw_ref.get("drug") or "medication").strip()
        line_body = (
            text
            if text.lower().startswith("drug reference")
            else f"Drug reference ({drug_label}): {text}"
        )
        appended_lines.append(f"[{next_index}] {line_body}")
        provenance = dict(raw_ref.get("provenance") or {})
        if not provenance and pkg:
            provenance = {
                "authority": str(pkg.get("name") or "DDInter 2.0 / RxNorm / CIEL"),
                "version": str(pkg.get("version") or ""),
            }
        mapping_entry: Dict[str, Any] = {
            "index": next_index,
            "sourceId": str(raw_ref.get("sourceId") or f"ddi-reference:{next_index}"),
            "source": str(raw_ref.get("source") or "ddi-service"),
            "resourceType": "drug_reference",
            "group": "reference",
            "resourceUuid": raw_ref.get("resourceUuid"),
            "date": None,
            "title": str(raw_ref.get("title") or f"Medication reference: {drug_label}"),
            "text": line_body,
        }
        if provenance:
            mapping_entry["provenance"] = provenance
        if pkg:
            mapping_entry["package"] = pkg
        new_mappings.append(mapping_entry)
        next_index += 1

    if not appended_lines:
        return chart_text, new_mappings, ctx

    sep = "" if chart_text.endswith("\n") else "\n"
    return chart_text + sep + "\n".join(appended_lines) + "\n", new_mappings, ctx


def _unavailable_context(
    *,
    exposure_complete: bool = False,
    issue: str = "ddi_service_unavailable",
) -> DdiPatientContext:
    return DdiPatientContext(
        raw_context={"exposure_complete": exposure_complete},
        package={},
        available=False,
        issue=issue,
        exposure_complete=exposure_complete,
        identity_confidence="unavailable",
    )


def _unavailable_result(
    patient_context: Optional[DdiPatientContext],
    *,
    issue: str,
) -> drug_safety.SafetyCheckResult:
    active_count = patient_context.active_order_count if patient_context else 0
    mapped_count = patient_context.mapped_active_order_count if patient_context else 0
    mapping_complete = patient_context.mapping_complete if patient_context else False
    exposure_complete = patient_context.exposure_complete if patient_context else False
    package = dict(patient_context.package) if patient_context else {}
    return drug_safety.SafetyCheckResult(
        status=drug_safety.STATUS_UNAVAILABLE,
        warnings=[],
        package=package,
        coverage={
            "mapping_complete": mapping_complete,
            "exposure_complete": exposure_complete,
            "execution_complete": False,
            "active_order_count": active_count,
            "mapped_active_order_count": mapped_count,
        },
        identity_confidence=(
            patient_context.identity_confidence if patient_context else "unavailable"
        ),
        issues=[issue],
    )


def _parse_check_response(
    payload: Mapping[str, Any],
    patient_context: Optional[DdiPatientContext],
) -> drug_safety.SafetyCheckResult:
    status = str(payload.get("status") or drug_safety.STATUS_UNAVAILABLE).strip()
    if status not in {
        drug_safety.STATUS_CHECKED,
        drug_safety.STATUS_LIMITED,
        drug_safety.STATUS_UNAVAILABLE,
    }:
        status = drug_safety.STATUS_LIMITED
    warnings = _extract_warnings(payload.get("warnings"))
    package = dict(
        payload.get("package") or (patient_context.package if patient_context else {})
    )
    coverage = dict(payload.get("coverage") or {})
    identity_confidence = str(
        payload.get("identity_confidence")
        or (patient_context.identity_confidence if patient_context else "unavailable")
    )
    issues = [str(i) for i in (payload.get("issues") or ()) if str(i).strip()]
    return drug_safety.SafetyCheckResult(
        status=status,
        warnings=warnings,
        package=package,
        coverage=coverage,
        identity_confidence=identity_confidence,
        issues=issues,
    )


async def aprepare_ddi_context(
    chart_text: str,
    mappings: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
    question: str,
    reference_date: Optional[str],
    *,
    exposure_complete: bool = False,
    client: Optional[httpx.AsyncClient] = None,
) -> Tuple[str, List[Dict[str, Any]], DdiPatientContext]:
    """Call ``ddi-service`` ``/v1/prepare`` asynchronously; fail open on service unavailability."""
    base_url = ddi_service_url()
    if not base_url or not chart_text:
        return chart_text, list(mappings), _unavailable_context(
            exposure_complete=exposure_complete,
            issue="check_disabled" if not base_url else "empty_chart",
        )
    req_body = {
        "records": records or [],
        "reference_date": reference_date,
        "question": question or "",
        "exposure_complete": exposure_complete,
    }
    try:
        if client is not None:
            resp = await client.post(
                f"{base_url}/v1/prepare", json=req_body, timeout=_timeout_seconds()
            )
        else:
            async with httpx.AsyncClient(timeout=_timeout_seconds()) as owns_client:
                resp = await owns_client.post(f"{base_url}/v1/prepare", json=req_body)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, Mapping):
            raise ValueError("Invalid /v1/prepare payload from ddi-service")
        return _apply_prepare_response(
            chart_text, mappings, data, exposure_complete=exposure_complete
        )
    except Exception as exc:
        logger.warning("ddi-service /v1/prepare unavailable: %s", exc)
        return (
            chart_text,
            list(mappings),
            _unavailable_context(
                exposure_complete=exposure_complete, issue="ddi_service_unavailable"
            ),
        )


def prepare_ddi_context(
    chart_text: str,
    mappings: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
    question: str,
    reference_date: Optional[str],
    *,
    exposure_complete: bool = False,
) -> Tuple[str, List[Dict[str, Any]], DdiPatientContext]:
    """Call ``ddi-service`` ``/v1/prepare`` synchronously; fail open on unavailability."""
    base_url = ddi_service_url()
    if not base_url or not chart_text:
        return chart_text, list(mappings), _unavailable_context(
            exposure_complete=exposure_complete,
            issue="check_disabled" if not base_url else "empty_chart",
        )
    req_body = {
        "records": records or [],
        "reference_date": reference_date,
        "question": question or "",
        "exposure_complete": exposure_complete,
    }
    try:
        with httpx.Client(timeout=_timeout_seconds()) as client:
            resp = client.post(f"{base_url}/v1/prepare", json=req_body)
            resp.raise_for_status()
            data = resp.json()
        if not isinstance(data, Mapping):
            raise ValueError("Invalid /v1/prepare payload from ddi-service")
        return _apply_prepare_response(
            chart_text, mappings, data, exposure_complete=exposure_complete
        )
    except Exception as exc:
        logger.warning("ddi-service /v1/prepare unavailable: %s", exc)
        return (
            chart_text,
            list(mappings),
            _unavailable_context(
                exposure_complete=exposure_complete, issue="ddi_service_unavailable"
            ),
        )


def compute_ddi_safety_check(
    patient_context: Optional[DdiPatientContext],
    answer_text: str,
    question: str,
    enabled: bool,
) -> drug_safety.SafetyCheckResult:
    """Evaluate response drug safety via ``ddi-service`` ``/v1/check`` with per-turn caching."""
    if not enabled:
        return _unavailable_result(patient_context, issue="check_disabled")
    base_url = ddi_service_url()
    if not base_url:
        return _unavailable_result(patient_context, issue="ddi_service_unavailable")

    if patient_context is not None and not patient_context.available:
        return _unavailable_result(
            patient_context, issue=patient_context.issue or "ddi_service_unavailable"
        )

    cache_key = (answer_text or "", question or "")
    if patient_context is not None and cache_key in patient_context._check_cache:
        return patient_context._check_cache[cache_key]

    raw_ctx = (
        dict(patient_context.raw_context)
        if patient_context is not None
        else {
            "age_years": None,
            "active_drugs": [],
            "active_drug_atc_codes": [],
            "allergy_tokens": [],
            "condition_tokens": [],
            "weight_kg": None,
            "mapping_complete": False,
            "exposure_complete": False,
            "active_order_count": 0,
            "mapped_active_order_count": 0,
            "identity_confidence": "unavailable",
        }
    )
    req_body = {
        "patient_context": raw_ctx,
        "answer_text": answer_text or "",
        "question": question or "",
    }
    try:
        with httpx.Client(timeout=_timeout_seconds()) as client:
            resp = client.post(f"{base_url}/v1/check", json=req_body)
            resp.raise_for_status()
            data = resp.json()
        if not isinstance(data, Mapping):
            raise ValueError("Invalid /v1/check payload from ddi-service")
        result = _parse_check_response(data, patient_context)
    except Exception as exc:
        logger.warning("ddi-service /v1/check unavailable: %s", exc)
        result = _unavailable_result(patient_context, issue="ddi_service_unavailable")

    if patient_context is not None:
        patient_context._check_cache[cache_key] = result
    return result
