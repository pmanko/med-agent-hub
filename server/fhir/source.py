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
        messages = payload.get("messages") or []
        combined = "\n".join(
            str(m.get("content") or "") for m in messages if isinstance(m, Mapping)
        )
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
        self.bundles_by_patient: Dict[str, Mapping[str, Any]] = dict(bundles_by_patient or {})
        self.bundle_dir = Path(bundle_dir) if bundle_dir else None
        self.target_tokens = target_tokens
        self.mode = mode
        self.sharing_posture = sharing_posture
        self.token_counter = token_counter or estimate_tokens_local

    def _resolve_bundle(self, request: ContextRequest) -> Mapping[str, Any]:
        if request.patient:
            if request.patient in self.bundles_by_patient:
                return self.bundles_by_patient[request.patient]
            if self.bundle_dir and self.bundle_dir.is_dir():
                candidate = self.bundle_dir / f"{request.patient}.json"
                if not candidate.exists():
                    matches = sorted(self.bundle_dir.glob(f"{request.patient}*.json"))
                    if matches:
                        candidate = matches[0]
                if candidate.exists():
                    return json.loads(candidate.read_text(encoding="utf-8"))

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
        for rec, mapping in zip(condensed.records, condensed.mappings):
            rtype = str(mapping.get("resourceType") or "Record")
            ruuid = str(mapping.get("resourceUuid") or "")
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
