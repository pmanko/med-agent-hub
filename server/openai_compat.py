"""
OpenAI-compatible bridge: the only consumer contract for med-agent-hub.

`POST /v1/chat/completions` accepts a standard OpenAI chat request from a
consumer (OpenMRS chartsearchai). If `model` is one of the advertised team
presets it runs the in-process Med Agent Team for that level; any other `model`
is forwarded straight to a single LM Studio model (a raw team-vs-single baseline).

`GET /v1/models` advertises the three team presets (`med-agent-team-low/med/high`),
which is what the consumer's model picker lists. We do NOT implement LM Studio's
native `/api/v1/models`; letting it 404 tags this endpoint as
`generic-openai-compat` to the consumer's model-switch probe.
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
from .levels_loader import level_ids, get_level
from .prompt_loader import prompt_names

logger = logging.getLogger(__name__)

router = APIRouter()


class ChatCompletionRequest(BaseModel):
    """The subset of the OpenAI chat-completions request we honor."""
    model: str
    messages: List[Dict[str, Any]] = Field(..., min_length=1)
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    context: Optional[Dict[str, Any]] = None  # P1: context-spec override (temporal/kb/expert); used in P3
    patient: Optional[str] = None  # when set, the hub RETRIEVES this patient's chart from querystore (else uses the chart in messages)


def _advertised_models() -> List[str]:
    """Advertise the team levels (server/levels.yaml keys) so the UI picker and
    chartsearchai's exact-match served-model validation both accept them. Each id
    selects which model runs each role (orchestrator/synthesizer/expert) per request
    — one instance serves any config, no reboot. Raw backends stay callable via
    passthrough but aren't listed.

    Also advertise the generic two-call legs ``answer:<writer>`` and ``indepth-only:<writer>``
    for every model the router serves, so chartsearchai's exact-match validation accepts the
    dynamic levels get_level() resolves on the fly (no hand-authored per-writer level needed)."""
    ids = list(level_ids())
    try:
        import httpx
        resp = httpx.get(f"{llm_config.base_url.rstrip('/')}/v1/models", timeout=3.0)
        prompts = prompt_names()
        answer_prompts = [
            p for p in prompts
            if p.startswith("synthesis-")
            and p != "synthesis-indepth"
            and not p.endswith("-indepth")
            and not p.startswith("synthesis-indepth-")
        ]
        indepth_prompts = [
            p for p in prompts
            if p == "synthesis-indepth" or p.endswith("-indepth") or p.startswith("synthesis-indepth-")
        ]
        for m in resp.json().get("data", []):
            mid = m.get("id")
            if mid:
                ids.append(f"indepth-only:{mid}")
                ids.append(f"answer:{mid}")
                for prompt in answer_prompts:
                    ids.append(f"answer:{mid}@{prompt}")
                    for gate in ("off", "warn", "enforce"):
                        ids.append(f"answer:{mid}@{prompt}~{gate}")
                for prompt in indepth_prompts:
                    ids.append(f"indepth-only:{mid}@{prompt}")
    except Exception:
        pass  # router unreachable -> advertise levels only; direct-to-hub callers still work
    return list(dict.fromkeys(ids))


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
    """Run the team for an advertised level id; raw passthrough for any other model."""
    try:
        level = get_level(req.model)
    except KeyError:
        return await _passthrough_content(req)
    return await run_team(
        req.messages,
        response_format=req.response_format,
        temperature=req.temperature,
        max_tokens=req.max_tokens,
        orchestrator_model=level.orchestrator,
        synthesizer_model=level.synthesizer,
        expert_model=level.expert,
        orchestrator_prompt=level.orchestrator_prompt,
        synthesizer_prompt=level.synthesis_prompt,
        expert_prompt=level.expert_prompt,
        has_expert=level.has_expert,
        validator_model=level.validator,
        validator_prompt=level.validator_prompt,
        validator_max_loops=level.validator_max_loops,
        two_call=level.two_call,
        indepth_shared=level.indepth_shared,
        indepth_only=level.indepth_only,
        solo=level.solo,
        context=req.context,
        temporal_gate=level.temporal_gate,
        patient=req.patient,
        anchor=level.anchor,
        knobs=level.knobs,
        level_id=req.model,  # the advertised level id == the harness backend_id (trace correlation key)
    )


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
