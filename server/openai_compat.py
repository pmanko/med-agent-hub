"""
OpenAI-compatible bridge: the only consumer contract for med-agent-hub.

`POST /v1/chat/completions` accepts a standard OpenAI chat request from a
consumer (OpenMRS chartsearchai) and either runs the in-process Med Agent Team
or forwards straight to a single LM Studio model — the latter gives a team-vs-raw
A/B baseline and satisfies the picker's "needs >= 2 models" constraint.

`GET /v1/models` advertises the team id plus the underlying model ids. We do NOT
implement LM Studio's native `/api/v1/models`; letting it 404 tags this endpoint
as `generic-openai-compat` to the consumer's model-switch probe.
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .config import llm_config
from .team import run_team

logger = logging.getLogger(__name__)

router = APIRouter()

TEAM_MODEL_ID = "med-agent-team"


class ChatCompletionRequest(BaseModel):
    """The subset of the OpenAI chat-completions request we honor."""
    model: str
    messages: List[Dict[str, Any]] = Field(..., min_length=1)
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False


def _advertised_models() -> List[str]:
    """The team plus the raw backends, de-duplicated and order-preserving."""
    ids = [TEAM_MODEL_ID, llm_config.orchestrator_model, llm_config.med_model]
    seen: set = set()
    return [m for m in ids if m and not (m in seen or seen.add(m))]


@router.get("/v1/models")
def list_models() -> Dict[str, Any]:
    created = int(time.time())
    return {
        "object": "list",
        "data": [
            {"id": mid, "object": "model", "created": created, "owned_by": "med-agent-hub"}
            for mid in _advertised_models()
        ],
    }


async def _passthrough_content(req: ChatCompletionRequest) -> str:
    """Forward the request to a single LM Studio model and return its content."""
    payload: Dict[str, Any] = {
        "model": req.model,
        "messages": req.messages,
        "temperature": req.temperature if req.temperature is not None else llm_config.temperature,
    }
    if req.max_tokens is not None:
        payload["max_tokens"] = req.max_tokens
    if req.response_format is not None:
        payload["response_format"] = req.response_format
    headers = {"Content-Type": "application/json"}
    if llm_config.api_key:
        headers["Authorization"] = f"Bearer {llm_config.api_key}"
    url = f"{llm_config.base_url.rstrip('/')}/v1/chat/completions"
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload, headers=headers, timeout=180.0)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


async def _content_for(req: ChatCompletionRequest) -> str:
    """Team for the team id; raw passthrough for an advertised backend id."""
    if req.model == TEAM_MODEL_ID:
        return await run_team(
            req.messages,
            response_format=req.response_format,
            temperature=req.temperature,
            max_tokens=req.max_tokens,
        )
    return await _passthrough_content(req)


def _completion_envelope(model: str, content: str) -> Dict[str, Any]:
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def _sse_stream(model: str, content: str):
    """Buffer-then-emit: the structured envelope is parsed whole by the consumer,
    so we emit it as a single content delta rather than true token streaming."""
    cid = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())

    def chunk(delta: Dict[str, Any], finish: Optional[str]) -> str:
        body = {
            "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }
        return f"data: {json.dumps(body)}\n\n"

    yield chunk({"role": "assistant"}, None)
    yield chunk({"content": content}, None)
    yield chunk({}, "stop")
    yield "data: [DONE]\n\n"


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest):
    content = await _content_for(req)
    if req.stream:
        return StreamingResponse(_sse_stream(req.model, content), media_type="text/event-stream")
    return _completion_envelope(req.model, content)
