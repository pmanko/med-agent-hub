"""OpenAI-compatible profile discovery and execution surface."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import replace
from typing import Any, Dict, List, Optional

import httpx
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


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[Dict[str, Any]] = Field(..., min_length=1)
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    context: Optional[Dict[str, Any]] = None
    patient: Optional[str] = None


def _served_backend_models() -> set[str]:
    try:
        response = httpx.get(
            f"{llm_config.base_url.rstrip('/')}/v1/models",
            timeout=3.0,
        )
        response.raise_for_status()
        return {
            str(item.get("id"))
            for item in (response.json().get("data") or [])
            if item.get("id")
        }
    except Exception:
        return set()


@router.get("/v1/models")
def list_models() -> Dict[str, Any]:
    created = int(time.time())
    served = _served_backend_models()
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
        data.append(
            {
                **profile_metadata(
                    profile,
                    available=not missing,
                    unavailable_reasons=unavailable_reasons,
                ),
                "object": "model",
                "created": created,
                "owned_by": "med-agent-hub",
            }
        )
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
    return _completion_envelope(req.model, content)
