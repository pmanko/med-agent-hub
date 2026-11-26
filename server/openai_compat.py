"""OpenAI-compatible profile discovery and execution surface."""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .config import llm_config
from .context_sources import ContextSourceError
from .engine import ExecutionRequest, drain_profile, execute_profile
from .levels_loader import (
    OPENROUTER_MODEL_MAP,
    ModelNotFoundError,
    Profile,
    compile_execution_spec,
    execution_modes_metadata,
    get_profile,
    known_model_aliases,
    parse_model_map,
    profile_ids,
    resolve_backend_model,
)

router = APIRouter()


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[Dict[str, Any]] = Field(..., min_length=1)
    execution: Optional[Dict[str, Any]] = None
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    context: Optional[Dict[str, Any]] = None
    patient: Optional[str] = None


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
    "base_url",
    "download_url",
    "endpoint",
    "model_url",
    "uri",
    "url",
}


def _normalized_metadata_key(key: str) -> tuple[str, tuple[str, ...]]:
    snake_case = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key.strip())
    normalized = re.sub(r"[^a-z0-9]+", "_", snake_case.lower()).strip("_")
    return normalized, tuple(part for part in normalized.split("_") if part)


def _is_sensitive_metadata_key(normalized: str, parts: tuple[str, ...]) -> bool:
    if normalized in _SENSITIVE_METADATA_KEYS:
        return True
    if any(
        part in {"credential", "credentials", "password", "secret"} for part in parts
    ):
        return True
    pairs = set(zip(parts, parts[1:]))
    return bool(
        pairs
        & {
            ("access", "token"),
            ("api", "key"),
            ("auth", "token"),
            ("bearer", "token"),
            ("client", "secret"),
            ("refresh", "token"),
        }
    )


def _is_url_metadata_key(normalized: str, parts: tuple[str, ...]) -> bool:
    return normalized in _URL_METADATA_KEYS or bool(
        parts and parts[-1] in {"endpoint", "uri", "url"}
    )


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
    normalized_key, key_parts = _normalized_metadata_key(key)
    if _is_sensitive_metadata_key(normalized_key, key_parts):
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
    if isinstance(value, str):
        if _is_url_metadata_key(normalized_key, key_parts):
            return _public_url(value)
        try:
            parsed = urlsplit(value)
        except ValueError:
            return value
        if parsed.scheme in {"http", "https"} and parsed.hostname:
            return _public_url(value)
    return value


ROUTER_PROBE_TIMEOUT_SECONDS = 3.0


def _models_url(base_url: str) -> str:
    clean = str(base_url).rstrip("/")
    return f"{clean}/models" if clean.endswith("/v1") else f"{clean}/v1/models"


def _cfg_effective_base_url(cfg: Any) -> str:
    val = getattr(cfg, "effective_base_url", None)
    if val:
        return str(val)
    return str(getattr(cfg, "base_url", "http://localhost:8077"))


def _cfg_effective_api_key(cfg: Any) -> str:
    val = getattr(cfg, "effective_api_key", None)
    if val is not None:
        return str(val)
    return str(getattr(cfg, "api_key", "") or "")


def _cfg_is_openrouter(cfg: Any) -> bool:
    if hasattr(cfg, "is_openrouter"):
        return bool(cfg.is_openrouter)
    return str(getattr(cfg, "provider", "") or "").strip().lower() == "openrouter"


def _served_backend_model_metadata() -> Optional[Dict[str, Dict[str, Any]]]:
    """Return the router catalog's per-model metadata, or ``None`` when discovery itself fails."""
    headers = {}
    api_key = _cfg_effective_api_key(llm_config)
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        response = httpx.get(
            _models_url(_cfg_effective_base_url(llm_config)),
            headers=headers,
            timeout=ROUTER_PROBE_TIMEOUT_SECONDS,
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
        return None


def _served_backend_models() -> Optional[set[str]]:
    """Compatibility helper for callers that only need loaded backend aliases."""
    discovered = _served_backend_model_metadata()
    return None if discovered is None else set(discovered)


def _backend_discovery_metadata() -> Dict[str, str]:
    endpoint = _public_url(str(_cfg_effective_base_url(llm_config)).rstrip("/"))
    return {
        "provider": str(getattr(llm_config, "provider", "openai-compatible")),
        "endpoint": endpoint,
        "models_endpoint": _models_url(endpoint),
    }


@router.get("/v1/models")
def list_models() -> Dict[str, Any]:
    created = int(time.time())
    is_openrouter = _cfg_is_openrouter(llm_config)
    provider_name = str(getattr(llm_config, "provider", "openai-compatible"))
    model_map_raw = str(getattr(llm_config, "model_map_raw", "") or "")

    backend_models = None if is_openrouter else _served_backend_model_metadata()
    backend_reachable = backend_models is not None
    advertised = set(backend_models) if backend_models else set()
    backend = _backend_discovery_metadata()

    explicit_map = parse_model_map(model_map_raw)
    data = []
    if is_openrouter:
        openrouter_configured = bool(_cfg_effective_api_key(llm_config))
        backend_reachable = openrouter_configured
        advertised = set()
        for model_id in sorted(set(OPENROUTER_MODEL_MAP) | set(explicit_map)):
            resolved = resolve_backend_model(
                model_id,
                provider=provider_name,
                model_map_raw=model_map_raw,
            )
            available = openrouter_configured and bool(resolved)
            unavailable_reasons = (
                () if available else ("openrouter_api_key_not_configured",)
            )
            data.append(
                {
                    "id": model_id,
                    "object": "model",
                    "created": created,
                    "owned_by": "med-agent-hub",
                    "available": available,
                    "unavailable_reasons": list(unavailable_reasons),
                    "resolved_backend_model": resolved,
                    "backend": backend,
                    "backend_metadata": None,
                }
            )
    else:
        for model_id in sorted(advertised | set(explicit_map)):
            resolved = resolve_backend_model(
                model_id,
                provider=provider_name,
                model_map_raw=model_map_raw,
            )
            is_advertised = model_id in advertised or resolved in advertised
            if not backend_reachable:
                reasons: tuple[str, ...] = ("model_backend_unreachable",)
            elif not is_advertised:
                reasons = (f"model_not_advertised:{resolved}",)
            else:
                reasons = ()
            available = backend_reachable and is_advertised
            data.append(
                {
                    "id": model_id,
                    "object": "model",
                    "created": created,
                    "owned_by": "med-agent-hub",
                    "available": available,
                    "unavailable_reasons": list(reasons),
                    "resolved_backend_model": resolved,
                    "backend": backend,
                    "backend_metadata": (
                        (backend_models or {}).get(model_id)
                        or (backend_models or {}).get(resolved)
                    ),
                }
            )
    return {
        "object": "list",
        "data": data,
        "execution_modes": execution_modes_metadata(),
        "backend": {
            "contract_version": "med-agent-hub.backend-model-inventory.v1",
            **backend,
            "catalog_reachable": backend_reachable,
            "advertised_model_ids": sorted(advertised),
        },
    }


def _model_label(req: ChatCompletionRequest) -> str:
    if isinstance(req.execution, Mapping):
        preset_id = req.execution.get("preset_id")
        if isinstance(preset_id, str) and preset_id.strip():
            return preset_id.strip()
    return req.model


def _request_for(req: ChatCompletionRequest, profile: Profile) -> ExecutionRequest:
    return ExecutionRequest(
        profile=profile,
        messages=req.messages,
        response_format=req.response_format,
        temperature=req.temperature,
        max_tokens=req.max_tokens,
        context=req.context,
        patient=req.patient,
        model_label=_model_label(req),
    )


def _require_product_profile(req: ChatCompletionRequest, profile: Profile) -> None:
    """Keep product clients on complete, safety-enforced product-mode plans.

    Direct hub clients may intentionally use low-level legs or bare evaluation modes.
    Product relays mark their request explicitly so an arbitrary request cannot bypass
    product envelope checks or temporal enforcement.
    """
    context = req.context or {}
    product_request = bool(
        context.get("require_product_profile")
        or context.get("require_product_envelope")
    )
    if product_request and (profile.low_level_leg or profile.output_mode != "product"):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "product_profile_required",
                "model": req.model,
                "message": "This client requires a product-mode execution profile.",
            },
        )


def _completion_envelope(model: str, content: str) -> Dict[str, Any]:
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
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


def _heartbeat_interval() -> float:
    try:
        configured = float(os.environ.get("HUB_SSE_HEARTBEAT_SECONDS", "0.5"))
    except ValueError:
        configured = 0.5
    return max(0.1, configured)


_SSE_HEARTBEAT_INTERVAL_S = _heartbeat_interval()


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


def _invalid_execution_error(
    req: ChatCompletionRequest, error: ValueError
) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "code": "invalid_execution_spec",
            "model": req.model,
            "message": str(error),
        },
    )


def _is_known_backing_model(model_id: str) -> bool:
    clean = model_id.strip()
    if not clean:
        return False
    model_map_raw = str(getattr(llm_config, "model_map_raw", "") or "")
    is_openrouter = _cfg_is_openrouter(llm_config)
    if is_openrouter and "/" in clean:
        return True
    if clean in known_model_aliases(model_map_raw):
        return True
    if not is_openrouter:
        served = _served_backend_models()
        if served and clean in served:
            return True
    return False


def _resolve_chat_profile(req: ChatCompletionRequest) -> Profile:
    if req.execution is not None:
        if not _is_known_backing_model(req.model):
            raise _model_error(ModelNotFoundError(req.model, profile_ids()))
        try:
            profile = compile_execution_spec(req.model, req.execution)
        except ValueError as error:
            raise _invalid_execution_error(req, error) from error
        for role_model in profile.models.values():
            if not _is_known_backing_model(role_model):
                raise _model_error(ModelNotFoundError(role_model, profile_ids()))
        return profile
    try:
        profile = get_profile(req.model)
    except ValueError as error:
        raise _invalid_execution_error(req, error) from error
    except ModelNotFoundError as error:
        if _is_known_backing_model(req.model):
            try:
                return compile_execution_spec(req.model, {"mode": "standard"})
            except ValueError as spec_error:
                raise _invalid_execution_error(req, spec_error) from spec_error
        raise _model_error(error) from error
    if not profile.low_level_leg and req.model not in profile_ids():
        for role_model in profile.models.values():
            if not _is_known_backing_model(role_model):
                raise _model_error(ModelNotFoundError(req.model, profile_ids()))
    return profile


@router.post("/v1/chat/completions")
async def chat_completions(
    req: ChatCompletionRequest,
    request: Request,
):
    profile = _resolve_chat_profile(req)

    _require_product_profile(req, profile)
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
    model_label = execution.model_label or req.model
    if req.stream:
        return StreamingResponse(
            _sse_stream(model_label, content),
            media_type="text/event-stream",
        )
    return _completion_envelope(model_label, content)
