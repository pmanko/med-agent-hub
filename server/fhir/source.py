"""FHIR R4 Bundle ContextSource adapter and resilient token counter for med-agent-hub."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from ..context_sources import (
    ContextRequest,
    ContextSourceError,
    EvidenceLedger,
    EvidenceRecord,
    RouterTokenCounter,
    _is_mandatory_safety_record,
)
from .condenser import CondensedChartResult, condense_bundle_to_budget, estimate_tokens_local


class ResilientTokenCounter:
    """Token counter that uses `RouterTokenCounter` when available and falls back to
    `estimate_tokens_local` when `/tokenize` is not exposed (e.g. Ollama, LiteLLM, or offline runs)."""

    def __init__(self, router_counter: Optional[RouterTokenCounter] = None) -> None:
        self.router_counter = router_counter

    async def count(self, model: str, text: str) -> int:
        if self.router_counter is not None:
            try:
                return await self.router_counter.count(model, text)
            except Exception:
                pass
        return estimate_tokens_local(text)

    async def count_records(self, model: str, texts: Sequence[str]) -> Tuple[int, ...]:
        if self.router_counter is not None:
            try:
                return await self.router_counter.count_records(model, texts)
            except Exception:
                pass
        return tuple(estimate_tokens_local(t) for t in texts)

    async def count_chat(self, model: str, payload: Mapping[str, Any]) -> int:
        if self.router_counter is not None:
            try:
                return await self.router_counter.count_chat(model, payload)
            except Exception:
                pass
        parts: list[str] = []
        if payload.get("system"):
            parts.append(str(payload.get("system")))
        messages = payload.get("messages") or []
        for m in messages:
            if not isinstance(m, Mapping):
                continue
            content = m.get("content")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, Mapping):
                        parts.append(str(block.get("text") or block.get("content") or json.dumps(block)))
                    else:
                        parts.append(str(block))
            elif content is not None:
                parts.append(str(content))
            if m.get("tool_calls"):
                parts.append(json.dumps(m.get("tool_calls")))
        if payload.get("tools"):
            parts.append(json.dumps(payload.get("tools")))
        combined = "\n".join(parts)
        # Add 24-token chat-template framing allowance
        return estimate_tokens_local(combined) + 24


class FhirBundleContextSource:
    """Provider-neutral ContextSource (`source='fhir'`) over FHIR R4 Bundles."""

    name = "fhir"
    priority = 45
    supports_patient = True

    def __init__(
        self,
        *,
        bundles_by_patient: Optional[Mapping[str, Mapping[str, Any]]] = None,
        bundle_dir: Optional[str | Path] = None,
        target_tokens: int = 8192,
        mode: str = "full",
        sharing_posture: str = "patient_self",
        token_counter: Optional[Callable[[str], int]] = None,
    ) -> None:
        if not isinstance(target_tokens, int) or isinstance(target_tokens, bool) or target_tokens <= 0:
            raise ValueError("target_tokens must be a positive integer")
        if mode not in {"full", "reconcile"}:
            raise ValueError(f"Unsupported condensation mode: {mode!r}")
        if sharing_posture not in {"patient_self", "external_share"}:
            raise ValueError(f"Unsupported sharing_posture: {sharing_posture!r}")
        self.bundles_by_patient: Dict[str, Mapping[str, Any]] = dict(bundles_by_patient or {})
        self.bundle_dir = Path(bundle_dir) if bundle_dir else None
        self.target_tokens = target_tokens
        self.mode = mode
        self.sharing_posture = sharing_posture
        self.token_counter = token_counter or estimate_tokens_local

    def _resolve_bundle(self, request: ContextRequest) -> Mapping[str, Any]:
        if request.patient:
            if request.patient in self.bundles_by_patient:
                bundle = self.bundles_by_patient[request.patient]
                if not isinstance(bundle, Mapping) or bundle.get("resourceType") != "Bundle":
                    raise ContextSourceError(
                        "invalid_context_bundle",
                        f"Configured FHIR resource for patient {request.patient!r} is not a FHIR Bundle.",
                        source=self.name,
                    )
                return bundle
            if self.bundle_dir and self.bundle_dir.is_dir():
                candidate = self.bundle_dir / f"{request.patient}.json"
                if not candidate.exists():
                    matches = sorted(self.bundle_dir.glob(f"{request.patient}*.json"))
                    if matches:
                        candidate = matches[0]
                if candidate.exists():
                    try:
                        parsed_file = json.loads(candidate.read_text(encoding="utf-8"))
                    except Exception as exc:
                        raise ContextSourceError(
                            "invalid_context_bundle",
                            f"Failed to parse FHIR Bundle JSON for patient {request.patient!r}: {exc}",
                            source=self.name,
                        ) from exc
                    if not isinstance(parsed_file, Mapping) or parsed_file.get("resourceType") != "Bundle":
                        raise ContextSourceError(
                            "invalid_context_bundle",
                            f"File {candidate.name} for patient {request.patient!r} is not a FHIR Bundle.",
                            source=self.name,
                        )
                    return parsed_file

        # Check if an inline JSON FHIR Bundle was passed in request.messages
        for msg in request.messages:
            if msg.get("role") != "user":
                continue
            content = str(msg.get("content") or "").strip()
            if content.startswith("{") and '"resourceType"' in content:
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, Mapping) and parsed.get("resourceType") == "Bundle":
                        return parsed
                except Exception:
                    pass

        raise ContextSourceError(
            "context_source_unavailable",
            f"No FHIR Bundle found for patient {request.patient!r}.",
            source=self.name,
        )

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        bundle = self._resolve_bundle(request)
        condensed: CondensedChartResult = condense_bundle_to_budget(
            bundle,
            target_tokens=self.target_tokens,
            mode=self.mode,
            question=request.question,
            sharing_posture=self.sharing_posture,
            token_counter=self.token_counter,
        )
        records: list[EvidenceRecord] = []
        seen_ids: Dict[str, int] = {}
        recs_by_key: Dict[Tuple[str, str], list[Dict[str, Any]]] = {}
        for rec in condensed.records:
            r_key = (
                str(rec.get("resourceType") or "Record"),
                str(rec.get("resourceUuid") or ""),
            )
            recs_by_key.setdefault(r_key, []).append(rec)

        for mapping in condensed.mappings:
            rtype = str(mapping.get("resourceType") or "Record")
            ruuid = str(mapping.get("resourceUuid") or "")
            bucket = recs_by_key.get((rtype, ruuid)) or []
            rec = bucket.pop(0) if bucket else {}
            base_id = f"fhir:{rtype}:{ruuid}"
            count = seen_ids.get(base_id, 0) + 1
            seen_ids[base_id] = count
            stable_id = base_id if count == 1 else f"{base_id}#{count}"
            meta = dict(rec.get("metadata") or {})
            text = str(mapping.get("text") or "")
            records.append(
                EvidenceRecord(
                    stable_id=stable_id,
                    source=self.name,
                    source_priority=self.priority,
                    resource_type=rtype,
                    resource_uuid=ruuid,
                    date=mapping.get("date"),
                    text=text,
                    mandatory=_is_mandatory_safety_record(rtype, text, meta),
                    clinical_date=rec.get("clinicalDate"),
                    date_kind=rec.get("dateKind"),
                    metadata=meta,
                    raw=rec,
                )
            )
        source_metadata = {
            self.name: {
                "tier": condensed.tier,
                "tier_name": condensed.tier_name,
                "completeness_manifest": condensed.completeness_manifest,
                "redacted_for_privacy": condensed.redacted_for_privacy,
                "compression_stats": condensed.compression_stats,
            }
        }
        return EvidenceLedger(
            tuple(records),
            original_text=condensed.chart_text,
            source_metadata=source_metadata,
        )
