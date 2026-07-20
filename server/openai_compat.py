"""OpenAI-compatible profile discovery and execution surface."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Mapping
from dataclasses import replace
from typing import Annotated, Any, Dict, List, Literal, Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
import rfc8785
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .config import llm_config
from .catalyst_contracts import REQUEST_V2_ID, validator_for
from .catalyst_query import query_profile_evidence
from .context_sources import ContextSourceError
from .engine import ExecutionRequest, drain_profile, execute_profile
from .levels_loader import (
    ModelNotFoundError,
    Profile,
    get_profile,
    profile_ids,
    profile_metadata,
)

router = APIRouter()


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


class _StrictQueryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CatalystQueryMessage(_StrictQueryModel):
    role: Literal["user"]
    content: str = Field(..., min_length=1)


class CatalystQueryTarget(_StrictQueryModel):
    dataSource: str = Field(..., min_length=1)
    catalogVersion: str = Field(..., min_length=1)
    dialect: str = Field(..., min_length=1)


class CatalystQueryCatalogField(_StrictQueryModel):
    name: str = Field(..., pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    type: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    unit: Optional[str] = None

    @field_validator("unit", mode="before")
    @classmethod
    def reject_null_unit(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("unit must be omitted rather than null")
        return value


class CatalystQuerySemanticValue(_StrictQueryModel):
    canonical: str = Field(..., min_length=1)
    aliases: List[Annotated[str, Field(min_length=1)]] = Field(..., min_length=1)


class CatalystQuerySemanticDimension(_StrictQueryModel):
    field: str = Field(..., pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    semanticType: Literal["analyte"]
    values: List[CatalystQuerySemanticValue] = Field(..., min_length=1)


class CatalystQueryCatalogView(_StrictQueryModel):
    name: str = Field(..., pattern=r"^[A-Za-z_][A-Za-z0-9_.]*$")
    version: str = Field(..., min_length=1)
    grain: str = Field(..., min_length=1)
    fields: List[CatalystQueryCatalogField] = Field(..., min_length=1)
    relationships: Optional[List[Annotated[str, Field(min_length=1)]]] = None
    semanticDimensions: Optional[List[CatalystQuerySemanticDimension]] = None

    @field_validator("relationships", mode="before")
    @classmethod
    def reject_null_relationships(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("relationships must be omitted rather than null")
        return value

    @field_validator("semanticDimensions", mode="before")
    @classmethod
    def reject_null_semantic_dimensions(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("semanticDimensions must be omitted rather than null")
        return value


class CatalystQueryCatalog(_StrictQueryModel):
    contextSourceId: str = Field(..., min_length=1)
    views: List[CatalystQueryCatalogView] = Field(..., min_length=1)


class CatalystQueryPolicy(_StrictQueryModel):
    allowedOperation: Literal["select"]
    requirePreview: Literal[True]
    maxRows: int = Field(..., ge=1)
    statementTimeoutMs: int = Field(..., ge=1)


class CatalystQueryCorrelation(_StrictQueryModel):
    requestId: str = Field(..., min_length=1)
    traceId: str = Field(..., min_length=1)


class CatalystQueryContext(_StrictQueryModel):
    contractVersion: Literal["catalyst.query.request.v1"]
    target: CatalystQueryTarget
    catalog: CatalystQueryCatalog
    policy: CatalystQueryPolicy
    correlation: CatalystQueryCorrelation
    requiredOutputContract: Literal["catalyst.query.v1"]


class CatalystQueryCompletionRequest(_StrictQueryModel):
    model: str = Field(..., min_length=1, pattern=r"^catalyst-query-")
    stream: Literal[False]
    messages: List[CatalystQueryMessage] = Field(..., min_length=1, max_length=1)
    catalystQuery: CatalystQueryContext


class CatalystQueryCompletionRequestV2(_StrictQueryModel):
    """Strict follow-up request validated by the published offline contract."""

    model: str = Field(..., min_length=1, pattern=r"^catalyst-query-")
    stream: Literal[False]
    messages: List[CatalystQueryMessage] = Field(..., min_length=1, max_length=1)
    catalystQuery: Dict[str, Any]

    @model_validator(mode="after")
    def validate_revision_contract(self) -> "CatalystQueryCompletionRequestV2":
        payload = self.model_dump(exclude_none=True)
        errors = sorted(
            validator_for(REQUEST_V2_ID).iter_errors(payload),
            key=lambda error: [str(item) for item in error.absolute_path],
        )
        if errors:
            error = errors[0]
            location = ".".join(str(item) for item in error.absolute_path) or "<root>"
            raise ValueError(
                f"Catalyst v2 request failed at {location}: {error.message}"
            )

        revision = self.catalystQuery["revision"]
        instruction = self.messages[0].content
        if revision["currentInstruction"] != instruction:
            raise ValueError(
                "the sole user message must equal revision.currentInstruction"
            )
        expected_instruction_digest = hashlib.sha256(
            instruction.encode("utf-8")
        ).hexdigest()
        if revision["instructionDigest"] != expected_instruction_digest:
            raise ValueError(
                "revision.instructionDigest must be SHA-256 of the exact current "
                "instruction bytes"
            )
        history = revision["instructionHistory"]
        if history[0]["kind"] != "initial" or any(
            item["kind"] != "followup" for item in history[1:]
        ):
            raise ValueError(
                "revision history must contain the initial instruction followed by "
                "at most five follow-ups"
            )
        ordinals = [item["ordinal"] for item in history]
        if ordinals != sorted(ordinals) or len(ordinals) != len(set(ordinals)):
            raise ValueError("revision history ordinals must be unique and ordered")
        if any(
            item["instructionDigest"]
            != hashlib.sha256(item["instruction"].encode("utf-8")).hexdigest()
            for item in history
        ):
            raise ValueError(
                "every history instructionDigest must bind its exact UTF-8 bytes"
            )
        included = revision["selection"]["includedHistoryTurnIds"]
        if included != [item["turnId"] for item in history]:
            raise ValueError(
                "selection.includedHistoryTurnIds must exactly match instructionHistory"
            )
        editor = revision["editorSnapshot"]
        editor_content = {
            "sql": editor["sql"],
            "parameters": editor["parameters"],
            "expectedColumns": editor["expectedColumns"],
        }
        editor_digest = _canonical_digest(editor_content)
        if editor["editorDigest"] != editor_digest:
            raise ValueError(
                "revision.editorSnapshot.editorDigest does not bind the exact "
                "editor content"
            )

        classification = revision["baseClassification"]
        observed = revision["observedBase"]
        effective = revision["effectiveBaseVersion"]
        if effective is not None and effective["queryDigest"] != editor_digest:
            raise ValueError(
                "revision.effectiveBaseVersion must bind the editor snapshot"
            )
        if classification == "reused" and (observed is None or effective != observed):
            raise ValueError(
                "a reused base requires identical observed and effective versions"
            )
        if classification == "promoted_human" and effective is None:
            raise ValueError("a promoted human base requires an effective version")
        if classification == "unresolved" and effective is not None:
            raise ValueError("an unresolved base cannot have an effective version")

        selection = revision["selection"]
        omissions = selection["omissions"]
        omitted_history = omissions["omittedHistory"]
        if omissions["historyInstructionsOmitted"] != len(omitted_history):
            raise ValueError(
                "historyInstructionsOmitted must equal omittedHistory length"
            )
        if omissions["omittedHistoryDigest"] != _canonical_digest(omitted_history):
            raise ValueError(
                "omittedHistoryDigest must bind the ordered omitted references"
            )
        if any(item["turnId"] in set(included) for item in omitted_history):
            raise ValueError("included and omitted history turns must be disjoint")

        validation_context = revision["validationContext"]
        validation_ref = selection["validationRef"]
        expected_validation_ref = (
            None
            if validation_context is None
            else {
                key: validation_context[key]
                for key in ("validationId", "versionId", "queryDigest")
            }
        )
        if validation_ref != expected_validation_ref or (
            validation_context is not None
            and validation_context["queryDigest"] != editor_digest
        ):
            raise ValueError(
                "validationContext and selection.validationRef must match the base"
            )

        execution_context = revision["executionContext"]
        execution_ref = selection["executionRef"]
        expected_execution_ref = (
            None
            if execution_context is None
            else {
                key: execution_context[key]
                for key in ("executionId", "versionId", "queryDigest")
            }
        )
        if execution_ref != expected_execution_ref or (
            execution_context is not None
            and execution_context["queryDigest"] != editor_digest
        ):
            raise ValueError(
                "executionContext and selection.executionRef must match the base"
            )

        digestable_revision = dict(revision)
        digestable_revision.pop("contextDigest", None)
        if revision["contextDigest"] != _canonical_digest(digestable_revision):
            raise ValueError(
                "revision.contextDigest must bind the complete revision context"
            )
        return self


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[Dict[str, Any]] = Field(..., min_length=1)
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    context: Optional[Dict[str, Any]] = None
    patient: Optional[str] = None
    catalystQuery: Optional[Any] = None

    @model_validator(mode="before")
    @classmethod
    def validate_query_profile_request(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if (
            str(value.get("model", "")).startswith("catalyst-query-")
            or "catalystQuery" in value
        ):
            context = value.get("catalystQuery")
            contract_version = (
                context.get("contractVersion") if isinstance(context, dict) else None
            )
            request_model = (
                CatalystQueryCompletionRequestV2
                if contract_version == "catalyst.query.request.v2"
                else CatalystQueryCompletionRequest
            )
            return request_model.model_validate(value).model_dump(exclude_none=True)
        return value


_SENSITIVE_METADATA_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "authorization",
    "client_secret",
    "credential",
    "credentials",
    "password",
    "refresh_token",
    "secret",
}
_URL_METADATA_KEYS = {
    "download_url",
    "endpoint",
    "model_url",
    "uri",
    "url",
}


def _public_url(value: str) -> str:
    """Remove credentials, query parameters, and fragments from advertised URLs."""
    try:
        parsed = urlsplit(value)
        if not parsed.scheme or not parsed.hostname:
            return value
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path, "", ""))
    except ValueError:
        return "[redacted-invalid-url]"


def _sanitize_backend_metadata(value: Any, *, key: str = "") -> Any:
    """Preserve backend model metadata while removing conventional secrets."""
    normalized_key = key.strip().lower().replace("-", "_")
    if normalized_key in _SENSITIVE_METADATA_KEYS:
        return "[redacted]"
    if isinstance(value, Mapping):
        return {
            str(item_key): _sanitize_backend_metadata(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_backend_metadata(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_backend_metadata(item, key=key) for item in value]
    if isinstance(value, str) and normalized_key in _URL_METADATA_KEYS:
        return _public_url(value)
    return value


def _served_backend_model_metadata() -> Dict[str, Dict[str, Any]]:
    headers = {}
    if llm_config.api_key:
        headers["Authorization"] = f"Bearer {llm_config.api_key}"
    try:
        response = httpx.get(
            f"{llm_config.base_url.rstrip('/')}/v1/models",
            headers=headers,
            timeout=3.0,
        )
        response.raise_for_status()
        result: Dict[str, Dict[str, Any]] = {}
        for item in response.json().get("data") or []:
            if not isinstance(item, Mapping) or not item.get("id"):
                continue
            # The local llama.cpp router advertises configured aliases before
            # loading them and loads the selected alias on first inference.
            # Presence in the router catalog therefore means the model is
            # available; "unloaded" is lifecycle state, not an error.
            sanitized = _sanitize_backend_metadata(item)
            result[str(item["id"])] = dict(sanitized)
        return result
    except Exception:
        return {}


def _served_backend_models() -> set[str]:
    """Compatibility helper for callers that only need loaded backend aliases."""
    return set(_served_backend_model_metadata())


def _backend_discovery_metadata() -> Dict[str, str]:
    endpoint = _public_url(str(llm_config.base_url).rstrip("/"))
    return {
        "provider": str(getattr(llm_config, "provider", "openai-compatible")),
        "endpoint": endpoint,
        "models_endpoint": f"{endpoint}/v1/models",
    }


@router.get("/v1/models")
def list_models() -> Dict[str, Any]:
    created = int(time.time())
    backend_models = _served_backend_model_metadata()
    served = set(backend_models)
    backend = _backend_discovery_metadata()
    profiles = [get_profile(profile_id) for profile_id in profile_ids()]
    data = []
    for profile in profiles:
        missing = [
            model
            for model in sorted(set(profile.models.values()))
            if model not in served
        ]
        unavailable_reasons = (
            ("model_backend_unreachable",)
            if not served
            else tuple(f"model_not_loaded:{model}" for model in missing)
        )
        item = {
            **profile_metadata(
                profile,
                available=not missing,
                unavailable_reasons=unavailable_reasons,
            ),
            "object": "model",
            "created": created,
            "owned_by": "med-agent-hub",
            "backend": backend,
            "backend_model_metadata": {
                model: backend_models.get(model)
                for model in sorted(set(profile.models.values()))
            },
        }
        if profile.output_mode == "query" and not missing:
            item["profileEvidence"] = query_profile_evidence(profile)
        data.append(item)
    return {
        "object": "list",
        "data": data,
    }


def _request_for(req: ChatCompletionRequest, profile: Profile) -> ExecutionRequest:
    return ExecutionRequest(
        profile=profile,
        messages=req.messages,
        response_format=req.response_format,
        temperature=req.temperature,
        max_tokens=req.max_tokens,
        context=req.context,
        patient=req.patient,
        model_label=req.model,
        catalyst_query=(
            (
                req.catalystQuery.model_dump()
                if isinstance(req.catalystQuery, BaseModel)
                else dict(req.catalystQuery)
            )
            if req.catalystQuery is not None
            else None
        ),
    )


async def _content_for(req: ChatCompletionRequest) -> str:
    return await drain_profile(_request_for(req, get_profile(req.model)))


def _completion_envelope(
    model: str,
    content: str,
    *,
    completion_id: Optional[str] = None,
    extensions: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    envelope = {
        "id": completion_id or f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    if extensions:
        reserved = set(envelope).intersection(extensions)
        if reserved:
            raise ValueError(
                f"completion extensions overlap reserved fields: {sorted(reserved)}"
            )
        envelope.update(dict(extensions))
    return envelope


def _extract_query_evidence(content: str) -> tuple[str, Dict[str, Any]]:
    """Keep the query contract in message content and Hub evidence on the envelope."""
    try:
        payload = json.loads(content)
    except (TypeError, json.JSONDecodeError):
        return content, {}
    if not isinstance(payload, dict):
        return content, {}
    evidence = payload.pop("_hubEvidence", None)
    if not isinstance(evidence, Mapping):
        return content, {}
    return json.dumps(payload, separators=(",", ":")), dict(evidence)


def _sse_stream(model: str, content: str):
    """Emit one buffered OpenAI-compatible content delta."""
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())

    def chunk(delta: Dict[str, Any], finish: Optional[str]) -> str:
        body = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }
        return f"data: {json.dumps(body)}\n\n"

    yield chunk({"role": "assistant"}, None)
    yield chunk({"content": content}, None)
    yield chunk({}, "stop")
    yield "data: [DONE]\n\n"


_SSE_HEARTBEAT_INTERVAL_S = 10.0


def _named_sse(gen, interval_s: float = _SSE_HEARTBEAT_INTERVAL_S):
    """Frame stage events as SSE and propagate cancellation into the engine."""

    async def _stream():
        queue: asyncio.Queue = asyncio.Queue()

        async def _produce() -> None:
            try:
                async for item in gen:
                    await queue.put(("item", item))
            except ContextSourceError as error:
                await queue.put(("context_error", error))
            except BaseException as error:
                await queue.put(("error", error))
            finally:
                await queue.put(("done", None))

        producer = asyncio.create_task(_produce())
        try:
            while True:
                try:
                    kind, value = await asyncio.wait_for(
                        queue.get(), timeout=interval_s
                    )
                except asyncio.TimeoutError:
                    yield ": hb\n\n"
                    continue
                if kind == "context_error":
                    error = value
                    payload = json.dumps(
                        {
                            "code": error.code,
                            "source": error.source,
                            "message": str(error),
                        }
                    )
                    yield f"event: error\ndata: {payload}\n\n"
                    return
                if kind == "error":
                    raise value
                if kind == "done":
                    return
                name, data = value
                output = f"event: {name}\n"
                for line in (data or "").split("\n"):
                    output += f"data: {line}\n"
                output += "\n"
                yield output
        finally:
            if not producer.done():
                producer.cancel()
            try:
                await producer
            except BaseException:
                pass
            close = getattr(gen, "aclose", None)
            if close is not None:
                await close()

    return _stream()


def _model_error(error: ModelNotFoundError) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={
            "code": error.code,
            "model": error.model_id,
            "message": str(error),
            "configured_profiles": list(error.configured),
        },
    )


def _context_error(error: ContextSourceError) -> HTTPException:
    status = 422 if error.code == "insufficient_context" else 503
    return HTTPException(
        status_code=status,
        detail={
            "code": error.code,
            "source": error.source,
            "message": str(error),
        },
    )


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest, request: Request):
    try:
        profile = get_profile(req.model)
    except ModelNotFoundError as error:
        raise _model_error(error) from error

    catalyst_context = (
        req.catalystQuery.model_dump()
        if isinstance(req.catalystQuery, BaseModel)
        else req.catalystQuery
    )
    if (
        isinstance(catalyst_context, Mapping)
        and catalyst_context.get("contractVersion") == "catalyst.query.request.v2"
        and profile.policies.get("collaborative_review") is not True
    ):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "profile_not_revision_capable",
                "profileId": profile.id,
                "message": (
                    "follow-up requests require a configured different-family "
                    "writer/reviewer profile"
                ),
            },
        )

    execution = _request_for(req, profile)
    if req.stream and profile.staged:
        execution = replace(execution, is_disconnected=request.is_disconnected)
        return StreamingResponse(
            _named_sse(execute_profile(execution)),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    try:
        content = await drain_profile(execution)
    except ContextSourceError as error:
        raise _context_error(error) from error
    if req.stream:
        return StreamingResponse(
            _sse_stream(req.model, content),
            media_type="text/event-stream",
        )
    extensions: Dict[str, Any] = {}
    if profile.output_mode == "query":
        content, extensions = _extract_query_evidence(content)
    completion_id = (
        str((catalyst_context.get("correlation") or {}).get("traceId") or "")
        if isinstance(catalyst_context, Mapping)
        else None
    ) or None
    return _completion_envelope(
        req.model,
        content,
        completion_id=completion_id,
        extensions=extensions,
    )
