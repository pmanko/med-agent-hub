"""OpenAI-compatible profile discovery and execution surface."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import uuid
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
import rfc8785
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .config import llm_config
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


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[Dict[str, Any]] = Field(..., min_length=1)
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
        part in {"credential", "credentials", "password", "secret"}
        for part in parts
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
        parsed = urlsplit(value)
        if parsed.scheme in {"http", "https"} and parsed.hostname:
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
    completion_id = None
    return _completion_envelope(
        req.model,
        content,
        completion_id=completion_id,
        extensions=extensions,
    )
