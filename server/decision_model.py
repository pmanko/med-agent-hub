"""Typed decision-model (Laya / Jev) verification for grounding and review labels.

Implements the Hub runtime slice of ``DECISION-LAYER-2026-10``:
- Refuses router products (``typesafe/jev-router``) at configuration and call time.
- Evaluates ``(source, statement)`` grounding and In-Depth attribution pairs with
  calibrated support probabilities mapped to three confidence bands:
  ``verified`` (green, p >= high_threshold), ``uncertain`` (yellow,
  low_threshold <= p < high_threshold), and ``unsupported`` (red, p < low_threshold).
- Verifies candidate Answer reviewer ``(wrong, chart)`` error pairs so false-positive
  reviewer flags are recorded under ``overruled_by_decision_model`` instead of
  degrading the Answer confidence badge.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import httpx

logger = logging.getLogger(__name__)

FORBIDDEN_DECISION_ROUTES = frozenset({"typesafe/jev-router", "jev-router"})

DEFAULT_LOW_THRESHOLD = 0.35
DEFAULT_HIGH_THRESHOLD = 0.75
DEFAULT_TEMPERATURE_SCALE = 1.0

_DECISION_ENTAILMENT_PROMPT = (
    "You are a calibrated clinical decision model. For each PAIR, decide whether the "
    "SOURCE record supports the STATEMENT and estimate the support probability in [0.0, 1.0]. "
    "Answer NO (low probability) if the statement describes a different person, is negated "
    "or only suspected when the source does not confirm it, or states a specific fact not "
    "present in the SOURCE text. Judge only from the SOURCE text. Return JSON: "
    '{"decisions": [{"verdict": "YES"|"NO", "probability": 0.0, "missing_facts": []}]}.'
)

_DECISION_ATTRIBUTION_PROMPT = (
    "You are a calibrated clinical attribution decision model. For each PAIR, identify the "
    "patient facts in STATEMENT (values, units, dates, drugs, results, diagnoses, statuses) "
    "and ignore clinical interpretation or general advice. Decide whether every patient fact "
    "is stated in SOURCE and estimate the support probability in [0.0, 1.0]. List any "
    "unmatched patient facts in missing_facts. Return JSON: "
    '{"decisions": [{"verdict": "YES"|"NO", "probability": 0.0, "missing_facts": ["..."]}]}.'
)

_DECISION_REVIEW_ERROR_PROMPT = (
    "You are a calibrated clinical contradiction verifier. For each PAIR, decide whether "
    "the CHART EVIDENCE genuinely contradicts the DRAFT SPAN from the answer and estimate "
    "the contradiction probability in [0.0, 1.0]. Return YES only when the chart text "
    "directly conflicts with the draft span. Return JSON: "
    '{"decisions": [{"verdict": "YES"|"NO", "probability": 0.0}]}.'
)

_DECISION_GROUNDING_RF: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "decision_grounding_verdicts",
        "schema": {
            "type": "object",
            "properties": {
                "decisions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "verdict": {"type": "string", "enum": ["YES", "NO"]},
                            "probability": {"type": "number"},
                            "missing_facts": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": ["verdict", "probability"],
                    },
                },
            },
            "required": ["decisions"],
        },
    },
}

_DECISION_REVIEW_ERROR_RF: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "decision_review_error_verdicts",
        "schema": {
            "type": "object",
            "properties": {
                "decisions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "verdict": {"type": "string", "enum": ["YES", "NO"]},
                            "probability": {"type": "number"},
                        },
                        "required": ["verdict", "probability"],
                    },
                },
            },
            "required": ["decisions"],
        },
    },
}


def validate_decision_model_id(model: str) -> str:
    """Refuse router products (`typesafe/jev-router`) per DECISION-LAYER-2026-10."""
    cleaned = str(model or "").strip()
    if not cleaned:
        raise ValueError("decision model identifier must be non-empty")
    lowered = cleaned.lower()
    if lowered in FORBIDDEN_DECISION_ROUTES or lowered.endswith("/jev-router"):
        raise ValueError(
            f"decision model {cleaned!r} is forbidden: typesafe/jev-router is an LLM router, not a decision model"
        )
    return cleaned


def apply_temperature_scaling(
    probability: float, temperature_scale: float = DEFAULT_TEMPERATURE_SCALE
) -> float:
    """Scale a raw probability in [0, 1] by dividing its logit by ``temperature_scale``."""
    p = min(1.0 - 1e-6, max(1e-6, float(probability)))
    temp = max(1e-3, float(temperature_scale))
    if abs(temp - 1.0) < 1e-6:
        return round(p, 4)
    logit = math.log(p / (1.0 - p)) / temp
    if logit >= 0:
        scaled = 1.0 / (1.0 + math.exp(-logit))
    else:
        exp_pos = math.exp(logit)
        scaled = exp_pos / (1.0 + exp_pos)
    return round(scaled, 4)


def classify_probability_band(
    probability: float,
    *,
    low_threshold: float = DEFAULT_LOW_THRESHOLD,
    high_threshold: float = DEFAULT_HIGH_THRESHOLD,
) -> Tuple[Optional[bool], str]:
    """Map a calibrated probability to ``(verdict, band)``.

    Returns:
      - ``(True, "verified")`` when ``probability >= high_threshold``
      - ``(False, "unsupported")`` when ``probability < low_threshold``
      - ``(None, "uncertain")`` when ``low_threshold <= probability < high_threshold``
    """
    p = float(probability)
    if p >= high_threshold:
        return True, "verified"
    if p < low_threshold:
        return False, "unsupported"
    return None, "uncertain"


def _extract_entry_probability(entry: Mapping[str, Any], verdict_str: str) -> Optional[float]:
    raw_prob = entry.get("probability")
    if raw_prob is None:
        raw_prob = entry.get("support_probability")
    if raw_prob is None:
        raw_prob = entry.get("confidence")
        if isinstance(raw_prob, (int, float)):
            conf = float(raw_prob)
            if 0.0 <= conf <= 1.0:
                return conf if verdict_str == "YES" else (1.0 - conf)
    if isinstance(raw_prob, (int, float)):
        val = float(raw_prob)
        if 0.0 <= val <= 1.0:
            return val
    if verdict_str == "YES":
        return 0.9
    if verdict_str == "NO":
        return 0.1
    return None


async def decide_grounding_pairs(
    client: httpx.AsyncClient,
    model: str,
    pairs: Sequence[Tuple[str, str]],
    *,
    chat_fn: Callable[..., Any],
    message_text_fn: Callable[[Mapping[str, Any]], str],
    attribution_only: bool = False,
    low_threshold: float = DEFAULT_LOW_THRESHOLD,
    high_threshold: float = DEFAULT_HIGH_THRESHOLD,
    temperature_scale: float = DEFAULT_TEMPERATURE_SCALE,
    reasons: Optional[List[Optional[str]]] = None,
    probabilities: Optional[List[Optional[float]]] = None,
    bands: Optional[List[Optional[str]]] = None,
) -> List[Optional[bool]]:
    """Score ``(source, statement)`` pairs with a Laya/Jev decision model."""
    validate_decision_model_id(model)
    if not pairs:
        return []
    body = "\n\n".join(
        f"PAIR {i + 1}:\nSOURCE: {source or '(none)'}\nSTATEMENT: {statement or '(none)'}"
        for i, (source, statement) in enumerate(pairs)
    )
    prompt = (
        _DECISION_ATTRIBUTION_PROMPT
        if attribution_only
        else _DECISION_ENTAILMENT_PROMPT
    )
    try:
        msg = await chat_fn(
            client,
            model,
            [{"role": "user", "content": prompt + "\n\n" + body}],
            response_format=_DECISION_GROUNDING_RF,
            temperature=0.0,
        )
        obj = json.loads(message_text_fn(msg))
        entries = obj.get("decisions") if isinstance(obj, Mapping) else None
        if not isinstance(entries, list):
            raise ValueError("decision response missing 'decisions' array")
    except Exception as exc:
        if exc.__class__.__name__ == "InsufficientContextError":
            raise
        logger.warning(
            "decision grounding[%s] call failed/unparseable -> all %d pair(s) unchecked",
            model,
            len(pairs),
        )
        if reasons is not None:
            reasons[:] = [None] * len(pairs)
        if probabilities is not None:
            probabilities[:] = [None] * len(pairs)
        if bands is not None:
            bands[:] = [None] * len(pairs)
        return [None] * len(pairs)

    out_verdicts: List[Optional[bool]] = []
    out_reasons: List[Optional[str]] = []
    out_probs: List[Optional[float]] = []
    out_bands: List[Optional[str]] = []

    for i in range(len(pairs)):
        entry = entries[i] if i < len(entries) and isinstance(entries[i], Mapping) else None
        if entry is None:
            out_verdicts.append(None)
            out_reasons.append(None)
            out_probs.append(None)
            out_bands.append(None)
            continue
        verdict_str = str(entry.get("verdict") or "").strip().upper()
        raw_prob = _extract_entry_probability(entry, verdict_str)
        if raw_prob is None:
            out_verdicts.append(None)
            out_reasons.append(None)
            out_probs.append(None)
            out_bands.append(None)
            continue
        scaled_prob = apply_temperature_scaling(raw_prob, temperature_scale)
        verdict, band = classify_probability_band(
            scaled_prob,
            low_threshold=low_threshold,
            high_threshold=high_threshold,
        )
        missing = [
            str(fact).strip()
            for fact in (entry.get("missing_facts") or [])
            if str(fact or "").strip()
        ]
        out_verdicts.append(verdict)
        out_reasons.append("; ".join(missing) or None)
        out_probs.append(scaled_prob)
        out_bands.append(band)

    if reasons is not None:
        reasons[:] = out_reasons
    if probabilities is not None:
        probabilities[:] = out_probs
    if bands is not None:
        bands[:] = out_bands
    return out_verdicts


async def verify_review_error_pairs(
    client: httpx.AsyncClient,
    model: str,
    errors: Sequence[Mapping[str, Any]],
    *,
    chat_fn: Callable[..., Any],
    message_text_fn: Callable[[Mapping[str, Any]], str],
    low_threshold: float = DEFAULT_LOW_THRESHOLD,
    high_threshold: float = DEFAULT_HIGH_THRESHOLD,
    temperature_scale: float = DEFAULT_TEMPERATURE_SCALE,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Verify candidate Answer reviewer ``(wrong, chart)`` error pairs (Pattern A).

    Returns ``(confirmed_errors, overruled_errors, decision_records)``.
    Fail-open on transport/parse failure: preserves original ``errors`` unchanged so a
    verifier outage never silently suppresses a reviewer flag.
    """
    validate_decision_model_id(model)
    normalized_errors = [dict(err) for err in errors if isinstance(err, Mapping)]
    if not normalized_errors:
        return [], [], []
    body = "\n\n".join(
        f"PAIR {i + 1}:\nCHART EVIDENCE: {str(err.get('chart') or '(none)')}\n"
        f"DRAFT SPAN: {str(err.get('wrong') or '(none)')}"
        for i, err in enumerate(normalized_errors)
    )
    try:
        msg = await chat_fn(
            client,
            model,
            [{"role": "user", "content": _DECISION_REVIEW_ERROR_PROMPT + "\n\n" + body}],
            response_format=_DECISION_REVIEW_ERROR_RF,
            temperature=0.0,
        )
        obj = json.loads(message_text_fn(msg))
        entries = obj.get("decisions") if isinstance(obj, Mapping) else None
        if not isinstance(entries, list) or len(entries) < len(normalized_errors):
            raise ValueError("decision review error response missing complete 'decisions'")
    except Exception:
        logger.warning(
            "decision review-verifier[%s] failed -> keeping all %d reviewer error(s)",
            model,
            len(normalized_errors),
        )
        return normalized_errors, [], []

    confirmed: List[Dict[str, Any]] = []
    overruled: List[Dict[str, Any]] = []
    records: List[Dict[str, Any]] = []
    for err, entry in zip(normalized_errors, entries):
        if not isinstance(entry, Mapping):
            confirmed.append(err)
            continue
        verdict_str = str(entry.get("verdict") or "").strip().upper()
        raw_prob = _extract_entry_probability(entry, verdict_str)
        if raw_prob is None:
            confirmed.append(err)
            continue
        scaled_prob = apply_temperature_scaling(raw_prob, temperature_scale)
        verdict, band = classify_probability_band(
            scaled_prob,
            low_threshold=low_threshold,
            high_threshold=high_threshold,
        )
        enriched = {
            **err,
            "decision_probability": scaled_prob,
            "decision_band": band,
        }
        records.append(
            {
                "wrong": err.get("wrong"),
                "chart": err.get("chart"),
                "probability": scaled_prob,
                "band": band,
                "confirmed": verdict is not False,
            }
        )
        if verdict is False:
            overruled.append(enriched)
        else:
            confirmed.append(enriched)
    return confirmed, overruled, records
