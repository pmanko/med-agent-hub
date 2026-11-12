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
from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .config import llm_config
from .context_sources import ContextSourceError
from .engine import ExecutionRequest, drain_profile, execute_profile
from .fhir.clinical_graph import build_wahs_hub_enrichment_block
from .levels_loader import (
    ModelNotFoundError,
    Profile,
    get_profile,
    get_wahs_profile,
    is_wahs_profile_id,
    profile_ids,
    profile_metadata,
    resolve_backend_model,
    resolve_openrouter_model,
    resolve_wahs_model_context_spec,
    wahs_profile_ids,
    wahs_profile_metadata,
)
from .prompt_loader import load_prompt

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
def list_models(
    workflow: Optional[str] = None,
    x_openrouter_key: Optional[str] = Header(default=None, alias="X-OpenRouter-Key"),
) -> Dict[str, Any]:
    created = int(time.time())
    is_wahs = isinstance(workflow, str) and workflow.strip().lower() == "wahs"
    is_openrouter = _cfg_is_openrouter(llm_config)
    provider_name = str(getattr(llm_config, "provider", "openai-compatible"))
    model_map_raw = str(getattr(llm_config, "model_map_raw", "") or "")
    openrouter_api_key = str(getattr(llm_config, "openrouter_api_key", "") or "")
    openrouter_base_url = str(
        getattr(llm_config, "openrouter_base_url", "https://openrouter.ai/api/v1")
    )

    if not is_wahs and is_openrouter:
        backend_models = None
    else:
        backend_models = _served_backend_model_metadata()
    backend_reachable = backend_models is not None
    advertised = set(backend_models) if backend_models else set()
    backend = _backend_discovery_metadata()

    if is_wahs:
        header_key = (
            x_openrouter_key.strip() if isinstance(x_openrouter_key, str) else ""
        )
        openrouter_configured = bool(
            header_key or openrouter_api_key.strip()
        )
        wahs_profiles = [get_wahs_profile(pid) for pid in wahs_profile_ids()]
        wahs_data = []
        for profile in wahs_profiles:
            meta = wahs_profile_metadata(
                profile,
                backend_models=advertised if backend_reachable else None,
                openrouter_configured=openrouter_configured,
                effective_default=profile.default,
            )
            wahs_data.append(
                {
                    **meta,
                    "object": "model",
                    "created": created,
                    "owned_by": "med-agent-hub",
                    "backend": backend,
                }
            )
        return {
            "object": "list",
            "workflow": "wahs",
            "data": wahs_data,
            "backend": {
                "contract_version": "med-agent-hub.backend-model-inventory.v1",
                **backend,
                "catalog_reachable": backend_reachable,
                "advertised_model_ids": sorted(advertised),
                "openrouter_configured": openrouter_configured,
                "openrouter_base_url": _public_url(openrouter_base_url),
            },
        }

    profiles = [get_profile(profile_id) for profile_id in profile_ids()]
    readiness = []
    if is_openrouter:
        openrouter_configured = bool(_cfg_effective_api_key(llm_config))
        backend_reachable = openrouter_configured
        advertised = set()
        for profile in profiles:
            resolved_models = {
                resolve_backend_model(
                    model,
                    provider=provider_name,
                    model_map_raw=model_map_raw,
                )
                for model in profile.models.values()
            }
            if openrouter_configured and all(resolved_models):
                readiness.append((profile, True, ()))
            else:
                readiness.append(
                    (profile, False, ("openrouter_api_key_not_configured",))
                )
    else:
        for profile in profiles:
            missing = [
                model
                for model in sorted(set(profile.models.values()))
                if resolve_backend_model(
                    model,
                    provider=provider_name,
                    model_map_raw=model_map_raw,
                )
                not in advertised
            ]
            unavailable_reasons = (
                ("model_backend_unreachable",)
                if not backend_reachable
                else tuple(f"model_not_advertised:{model}" for model in missing)
            )
            readiness.append((profile, not missing, unavailable_reasons))

    available_products = [
        profile
        for profile, available, _reasons in readiness
        if available and profile.visibility == "product"
    ]
    configured_default = next(
        (profile for profile in available_products if profile.default), None
    )
    effective_default = configured_default or min(
        available_products,
        key=lambda profile: (profile.selection_priority, profile.id),
        default=None,
    )

    data = []
    for profile, available, unavailable_reasons in readiness:
        data.append(
            {
                **profile_metadata(
                    profile,
                    available=available,
                    unavailable_reasons=unavailable_reasons,
                    effective_default=(
                        effective_default is not None
                        and profile.id == effective_default.id
                    ),
                ),
                "object": "model",
                "created": created,
                "owned_by": "med-agent-hub",
                "backend": backend,
                "backend_model_metadata": {
                    model: (backend_models or {}).get(model)
                    or (backend_models or {}).get(
                        resolve_backend_model(
                            model,
                            provider=provider_name,
                            model_map_raw=model_map_raw,
                        )
                    )
                    for model in sorted(set(profile.models.values()))
                },
            }
        )
    return {
        "object": "list",
        "data": data,
        "backend": {
            "contract_version": "med-agent-hub.backend-model-inventory.v1",
            **backend,
            "catalog_reachable": backend_reachable,
            "advertised_model_ids": sorted(advertised),
        },
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


def _require_product_profile(req: ChatCompletionRequest, profile: Profile) -> None:
    """Keep product clients on complete, safety-enforced profile plans.

    Direct hub clients may intentionally use low-level legs. Product relays mark
    their request explicitly so an arbitrary client-supplied model id cannot turn a
    product Answer into an experimental ``off``/``warn`` execution.
    """
    product_request = bool((req.context or {}).get("require_product_profile"))
    if product_request and (
        profile.visibility != "product" or profile.output_mode != "product"
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "product_profile_required",
                "model": req.model,
                "message": "This client accepts configured product profiles only.",
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


async def _execute_wahs_chat_completion(
    req: ChatCompletionRequest,
    profile: Profile,
    request: Request,
    response: Optional[Response] = None,
):
    header_provider = (request.headers.get("x-llm-provider") or "").strip().lower()
    header_or_key = (request.headers.get("x-openrouter-key") or "").strip()
    provider = str(profile.policies.get("provider") or "local").lower()
    if header_provider == "openrouter" or header_or_key:
        provider = "openrouter"

    if provider == "openrouter":
        target_base_url = llm_config.openrouter_base_url.rstrip("/")
        api_key = header_or_key or (llm_config.openrouter_api_key or "").strip()
        if not api_key:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "openrouter_api_key_required",
                    "model": profile.id,
                    "message": (
                        "OpenRouter API key is required for OpenRouter connection "
                        "(set OPENROUTER_API_KEY or pass X-OpenRouter-Key header)."
                    ),
                },
            )
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://wahs-demo.openclinai.org",
            "X-Title": "OpenClinAI WAHS+ Explorer",
        }
    else:
        target_base_url = llm_config.base_url.rstrip("/")
        headers = {"Content-Type": "application/json"}
        if llm_config.api_key:
            headers["Authorization"] = f"Bearer {llm_config.api_key}"

    sys_prompt = load_prompt(str(profile.prompts["answer"])).strip()
    caller_sys_parts = [
        str(m.get("content") or "").strip()
        for m in req.messages
        if str(m.get("role") or "").lower() == "system" and str(m.get("content") or "").strip()
    ]
    combined_sys_prompt = (
        f"{sys_prompt}\n\n{chr(10).join(caller_sys_parts)}"
        if caller_sys_parts
        else sys_prompt
    )
    user_messages = [
        dict(m) for m in req.messages if str(m.get("role") or "").lower() != "system"
    ]
    enrichment_meta: Optional[Dict[str, Any]] = None
    hub_enrich_hdr = (request.headers.get("x-wahs-hub-enrichment") or "").strip().lower()
    enable_hub_enrichment = bool(
        profile.policies.get("include_hub_enrichment", profile.policies.get("include_kb_findings", True))
    ) and hub_enrich_hdr not in {"0", "false", "off", "no"}
    if enable_hub_enrichment and user_messages:
        enrichment_res = build_wahs_hub_enrichment_block(req.messages, max_tokens=800)
        enrichment_meta = enrichment_res.to_metadata_dict()
        for idx in range(len(user_messages) - 1, -1, -1):
            if str(user_messages[idx].get("role") or "").lower() == "user":
                orig_content = str(user_messages[idx].get("content") or "")
                if (
                    "[MED-AGENT-HUB ORCHESTRATION:" not in orig_content
                    and enrichment_res.enrichment_text
                ):
                    user_messages[idx]["content"] = (
                        f"{orig_content.rstrip()}\n\n{enrichment_res.enrichment_text}"
                    )
                break
    upstream_messages = [{"role": "system", "content": combined_sys_prompt}, *user_messages]

    answer_knobs = dict(profile.knobs.get("answer") or {})
    temperature = (
        req.temperature
        if req.temperature is not None
        else float(answer_knobs.get("temperature", 0.1))
    )
    max_tokens = (
        req.max_tokens
        if req.max_tokens is not None
        else int(answer_knobs.get("maxTokens", 4096))
    )
    raw_writer_model = str(profile.models["answer"])
    writer_model = (
        resolve_openrouter_model(raw_writer_model)
        if provider == "openrouter"
        else raw_writer_model
    )
    upstream_url = (
        f"{target_base_url}/chat/completions"
        if target_base_url.endswith("/v1")
        else f"{target_base_url}/v1/chat/completions"
    )

    upstream_payload: Dict[str, Any] = {
        "model": writer_model,
        "messages": upstream_messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": bool(req.stream),
    }
    if provider == "local":
        upstream_payload["reasoning_effort"] = "none"
        upstream_payload["chat_template_kwargs"] = {"enable_thinking": False}
        total_msg_chars = sum(
            len(str(m.get("content") or ""))
            for m in upstream_messages
            if isinstance(m, dict)
        )
        est_total_tokens = int(total_msg_chars / 3.2) + max_tokens + 256
        if est_total_tokens > 28672:
            ctx_max, _ = resolve_wahs_model_context_spec(raw_writer_model)
            if est_total_tokens <= 65536:
                req_ctx = 65536
            elif est_total_tokens <= 131072:
                req_ctx = 131072
            else:
                req_ctx = 262144
            upstream_payload["num_ctx"] = min(req_ctx, ctx_max)

    async def _run_review_stage(draft_answer: str) -> str:
        if "review" not in profile.stages or "review" not in profile.models:
            return ""
        rev_model = str(profile.models["review"])
        rev_prompt = load_prompt(str(profile.prompts["review"])).strip()
        rev_knobs = dict(profile.knobs.get("review") or {})
        last_user = next(
            (
                str(m.get("content") or "")
                for m in reversed(user_messages)
                if str(m.get("role") or "") == "user"
            ),
            "",
        )
        review_context = (
            f"{chr(10).join(caller_sys_parts)}\n\n{last_user}".strip()
            if caller_sys_parts
            else last_user
        )
        rev_payload = {
            "model": rev_model,
            "messages": [
                {"role": "system", "content": rev_prompt},
                {
                    "role": "user",
                    "content": (
                        f"CHART & QUESTION CONTEXT:\n{review_context}\n\n"
                        f"WRITER DRAFT ANSWER:\n{draft_answer}"
                    ),
                },
            ],
            "temperature": float(rev_knobs.get("temperature", 0.0)),
            "max_tokens": int(rev_knobs.get("maxTokens", 350)),
            "stream": False,
        }
        try:
            async with httpx.AsyncClient(
                timeout=llm_config.request_timeout_seconds
            ) as client:
                r = await client.post(upstream_url, headers=headers, json=rev_payload)
                r.raise_for_status()
                r_data = r.json()
                verdict = (
                    ((r_data.get("choices") or [{}])[0].get("message") or {}).get(
                        "content"
                    )
                    or ""
                ).strip()
                if verdict:
                    return f"\n\n---\n🔍 **Cross-Family Verifier (`{rev_model}`):**\n{verdict}"
        except Exception:
            return ""
        return ""

    if req.stream:
        completion_id = f"chatcmpl-{uuid.uuid4().hex}"
        created = int(time.time())

        async def _wahs_sse():
            from . import model_provisioner, team

            accumulated: List[str] = []
            first_chunk_emitted = False
            if provider == "local":
                await team._ROUTER_LOCK.acquire()
            try:
                if provider == "local":
                    await model_provisioner.ensure_local_model_provisioned(writer_model)
                async with httpx.AsyncClient(
                    timeout=llm_config.request_timeout_seconds
                ) as client:
                    async with client.stream(
                        "POST", upstream_url, headers=headers, json=upstream_payload
                    ) as resp:
                        resp.raise_for_status()
                        async for raw_line in resp.aiter_lines():
                            line = raw_line.strip()
                            if not line or not line.startswith("data:"):
                                continue
                            data_str = line[5:].strip()
                            if data_str == "[DONE]":
                                break
                            try:
                                chunk_obj = json.loads(data_str)
                                delta = (
                                    (chunk_obj.get("choices") or [{}])[0].get("delta")
                                    or {}
                                )
                                piece = delta.get("content") or ""
                                if piece:
                                    accumulated.append(piece)
                                chunk_obj["model"] = profile.id
                                if not first_chunk_emitted and enrichment_meta is not None:
                                    chunk_obj["hub_enrichment"] = enrichment_meta
                                    first_chunk_emitted = True
                                yield f"data: {json.dumps(chunk_obj)}\n\n"
                            except Exception:
                                yield f"{line}\n\n"
            finally:
                if provider == "local":
                    team._ROUTER_LOCK.release()
            review_appendix = await _run_review_stage("".join(accumulated))
            if review_appendix:
                rev_chunk = {
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": profile.id,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": review_appendix},
                            "finish_reason": None,
                        }
                    ],
                }
                yield f"data: {json.dumps(rev_chunk)}\n\n"
            yield "data: [DONE]\n\n"

        stream_headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        if enrichment_meta is not None:
            stream_headers["X-WAHS-Hub-Enrichment"] = json.dumps(enrichment_meta)
        return StreamingResponse(
            _wahs_sse(),
            media_type="text/event-stream",
            headers=stream_headers,
        )

    from . import model_provisioner, team
    import re

    t0 = time.perf_counter()
    if provider == "local":
        await team._ROUTER_LOCK.acquire()
    try:
        if provider == "local":
            await model_provisioner.ensure_local_model_provisioned(writer_model)
        async with httpx.AsyncClient(timeout=llm_config.request_timeout_seconds) as client:
            resp = await client.post(upstream_url, headers=headers, json=upstream_payload)
            resp.raise_for_status()
            body = resp.json()
    finally:
        if provider == "local":
            team._ROUTER_LOCK.release()
    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    content = (
        ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    )
    if "<think>" in content and "</think>" in content:
        content = re.sub(r"<think>.*?</think>\s*", "", content, flags=re.DOTALL).strip()
    if "<|channel>thought" in content and "<channel|>" in content:
        content = re.sub(
            r"<\|channel>thought.*?<channel\|>\s*", "", content, flags=re.DOTALL
        ).strip()
    if re.match(r"^\s*thought\b", content, flags=re.IGNORECASE):
        m_sum = re.search(
            r"(?:^|\n)\s*(?:\*\*|###\s*)?(?:SUMMARY|ABSTAIN)\s*:",
            content,
            flags=re.IGNORECASE,
        )
        if m_sum:
            content = content[m_sum.start() :].lstrip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z]*\s*", "", content)
        content = re.sub(r"\s*```\s*$", "", content).strip()
    if content.startswith(("{", "[")):
        try:
            parsed_json = json.loads(content)
            if isinstance(parsed_json, list) and parsed_json and isinstance(parsed_json[0], dict):
                parsed_json = parsed_json[0]
            if isinstance(parsed_json, dict):
                if isinstance(parsed_json.get("text"), str):
                    content = parsed_json["text"].strip()
                elif "summary" in parsed_json or "SUMMARY" in parsed_json:
                    s_txt = str(parsed_json.get("summary") or parsed_json.get("SUMMARY") or "").strip()
                    q_val = parsed_json.get("questions_for_doctor") or parsed_json.get("QUESTIONS_FOR_DOCTOR") or []
                    if isinstance(q_val, list):
                        q_txt = "\n".join(f"- {q}" for q in q_val if str(q).strip())
                    else:
                        q_txt = str(q_val).strip()
                    content = f"SUMMARY:\n{s_txt}" + (f"\n\nQUESTIONS_FOR_DOCTOR:\n{q_txt}" if q_txt else "")
        except Exception:
            pass
    review_appendix = await _run_review_stage(content)
    if review_appendix:
        content = f"{content}{review_appendix}"
    envelope = _completion_envelope(profile.id, content)
    if isinstance(body.get("usage"), dict):
        envelope["usage"] = dict(body["usage"])
    envelope["hub_profile"] = {
        "id": profile.id,
        "label": profile.label,
        "provider": provider,
        "model_family": str(profile.policies.get("model_family") or ""),
        "role_models": dict(profile.models),
        "policies": dict(profile.policies),
        "latency_ms": latency_ms,
    }
    if enrichment_meta is not None:
        envelope["hub_enrichment"] = enrichment_meta
        if response is not None:
            response.headers["X-WAHS-Hub-Enrichment"] = json.dumps(enrichment_meta)
    return envelope


@router.post("/v1/chat/completions")
async def chat_completions(
    req: ChatCompletionRequest,
    request: Request,
    response: Response,
):
    if is_wahs_profile_id(req.model):
        wahs_profile = get_wahs_profile(req.model)
        return await _execute_wahs_chat_completion(
            req, wahs_profile, request, response=response
        )

    try:
        profile = get_profile(req.model)
    except ModelNotFoundError as error:
        raise _model_error(error) from error

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
    if req.stream:
        return StreamingResponse(
            _sse_stream(req.model, content),
            media_type="text/event-stream",
        )
    return _completion_envelope(req.model, content)
