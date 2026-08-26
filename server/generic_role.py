"""Generic and Hub-configured structured single-shot role execution.

Clients own their own multi-step orchestration and prompts. They call this
endpoint once per role with a model, a message list, and an optional
``response_format``, and receive back the model's assistant content. The hub
contributes only what is genuinely shared infrastructure:

* the model-router connection and its single-slot serialization (``_ROUTER_LOCK``
  inside :func:`server.team._chat` — concurrent callers must not race the router),
* the provider / auth / timeout abstraction, and
* OpenAI-compatible structured-output pass-through.

The raw endpoint remains for generic Hub consumers. Catalyst uses the named
query-profile endpoints below: it cannot select a model, override model knobs,
or inject a system prompt into those requests.
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from typing import Any, Dict, List, Mapping, Optional

import httpx
import rfc8785
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from . import team
from .config import llm_config
from .levels_loader import (
    ModelNotFoundError,
    catalyst_query_profile_ids,
    catalyst_query_profile_metadata,
    get_catalyst_query_profile,
)
from .openai_compat import (
    ROUTER_PROBE_TIMEOUT_SECONDS,
    _backend_discovery_metadata,
    _served_backend_model_metadata,
)
from .prompt_loader import load_prompt

router = APIRouter()


class GenerateRequest(BaseModel):
    """One structured single-shot generation against a named model."""

    model: str = Field(min_length=1)
    messages: List[Dict[str, Any]] = Field(min_length=1)
    response_format: Optional[Dict[str, Any]] = None
    temperature: Optional[float] = None
    dry_multiplier: Optional[float] = None
    max_tokens: Optional[int] = None


class GenerateResponse(BaseModel):
    model: str
    content: str


class ProfileGenerateRequest(BaseModel):
    """Caller context for a configured Catalyst query role."""

    model_config = ConfigDict(extra="forbid")

    messages: List[Dict[str, Any]] = Field(min_length=1)
    response_format: Optional[Dict[str, Any]] = None


class ProfileGenerateResponse(GenerateResponse):
    profile_id: str
    role: str
    request_evidence: Dict[str, Any]
    # Exact token evidence for the fully rendered request, counted with the
    # model's own template and tokenizer before the call. The compatible field
    # is None unless all four legacy values are known; request_evidence records
    # each missing fact and reason without substituting an estimate.
    token_accounting: Optional[Dict[str, Any]] = None


def _backend_models() -> set[str] | None:
    discovered = _served_backend_model_metadata()
    return None if discovered is None else set(discovered)


def _context_window(model: str) -> Optional[int]:
    """The --ctx-size the router actually launched this model with."""
    metadata = _served_backend_model_metadata()
    if not isinstance(metadata, Mapping):
        return None
    entry = metadata.get(model)
    if not isinstance(entry, Mapping):
        return None
    status = entry.get("status")
    if not isinstance(status, Mapping):
        return None
    advertised_args = status.get("args")
    if not isinstance(advertised_args, (list, tuple)):
        return None
    args = list(advertised_args)
    for index, item in enumerate(args[:-1]):
        if item in ("--ctx-size", "-c"):
            try:
                value = int(args[index + 1])
                if value <= 0:
                    return None
                return value
            except (TypeError, ValueError):
                return None
    return None


async def _prompt_measurement(
    model: str, messages: List[Dict[str, Any]], output_reserve: int
) -> Dict[str, Any]:
    """Render and count the exact configured-role prompt when the router can.

    /apply-template renders the exact prompt the model will consume --
    special tokens included -- and /tokenize counts it with the model's own
    vocabulary. Missing router capabilities remain explicit; they are never
    replaced with character or approximate token counts.
    """

    window = _context_window(model)
    prompt: Optional[str] = None
    prompt_tokens: Optional[int] = None
    prompt_unavailable_reason: Optional[str] = None
    count_unavailable_reason: Optional[str] = None
    base = llm_config.base_url.rstrip("/")
    headers = {}
    if llm_config.api_key:
        headers["Authorization"] = f"Bearer {llm_config.api_key}"
    try:
        async with httpx.AsyncClient(
            headers=headers,
            timeout=ROUTER_PROBE_TIMEOUT_SECONDS,
        ) as client:
            try:
                rendered = await client.post(
                    f"{base}/apply-template",
                    json={"model": model, "messages": messages},
                )
                rendered.raise_for_status()
                candidate = rendered.json().get("prompt")
                if isinstance(candidate, str):
                    prompt = candidate
                else:
                    prompt_unavailable_reason = "prompt_rendering_unavailable"
            except Exception:
                prompt_unavailable_reason = "prompt_rendering_unavailable"

            if prompt is None:
                count_unavailable_reason = "rendered_prompt_unavailable"
            else:
                try:
                    tokenized = await client.post(
                        f"{base}/tokenize",
                        json={
                            "model": model,
                            "content": prompt,
                            # apply-template already emitted special tokens.
                            "add_special": False,
                            "parse_special": True,
                        },
                    )
                    tokenized.raise_for_status()
                    tokens = tokenized.json().get("tokens")
                    if isinstance(tokens, list):
                        prompt_tokens = len(tokens)
                    else:
                        count_unavailable_reason = "prompt_token_count_unavailable"
                except Exception:
                    count_unavailable_reason = "prompt_token_count_unavailable"
    except Exception:
        if prompt is None:
            prompt_unavailable_reason = "prompt_rendering_unavailable"
            count_unavailable_reason = "rendered_prompt_unavailable"
        elif prompt_tokens is None:
            count_unavailable_reason = "prompt_token_count_unavailable"

    prompt_evidence: Dict[str, Any] = {
        "renderedPrompt": prompt,
        "renderedPromptDigest": (
            hashlib.sha256(prompt.encode("utf-8")).hexdigest()
            if prompt is not None
            else None
        ),
    }
    if prompt is None:
        prompt_evidence["unavailableReason"] = (
            prompt_unavailable_reason or "prompt_rendering_unavailable"
        )

    required_tokens = (
        prompt_tokens + output_reserve if prompt_tokens is not None else None
    )
    fits = (
        required_tokens <= window
        if required_tokens is not None and window is not None
        else None
    )
    token_evidence: Dict[str, Any] = {
        "tokenizer": model,
        "contextWindow": window,
        "outputReserve": output_reserve,
        "promptTokens": prompt_tokens,
        "requiredTokens": required_tokens,
        "fits": fits,
    }
    if window is None:
        token_evidence["contextWindowUnavailableReason"] = "context_window_unavailable"
    if prompt_tokens is None:
        token_evidence["promptTokensUnavailableReason"] = (
            count_unavailable_reason or "prompt_token_count_unavailable"
        )
    return {"prompt": prompt_evidence, "tokens": token_evidence}


def _request_evidence(
    *,
    profile_id: str,
    role: str,
    model: str,
    messages: List[Dict[str, Any]],
    response_format: Optional[Dict[str, Any]],
    temperature: float,
    dry_multiplier: float,
    max_tokens: int,
    measurement: Dict[str, Any],
) -> Dict[str, Any]:
    """Bind exact call inputs and router measurements with canonical JSON."""

    request = {
        "profileId": profile_id,
        "role": role,
        "model": model,
        "messages": deepcopy(messages),
        "responseFormat": deepcopy(response_format),
        "config": {
            "temperature": temperature,
            "dryMultiplier": dry_multiplier,
            "maxTokens": max_tokens,
        },
    }
    return {
        "contractVersion": "med-agent-hub.catalyst-role-request-evidence.v1",
        "request": request,
        "requestDigest": hashlib.sha256(rfc8785.dumps(request)).hexdigest(),
        "prompt": deepcopy(measurement["prompt"]),
        "tokens": deepcopy(measurement["tokens"]),
    }


def _compatible_token_accounting(
    measurement: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Keep the existing successful token_accounting response shape."""

    tokens = measurement.get("tokens")
    if not isinstance(tokens, dict):
        return None
    required = ("tokenizer", "contextWindow", "outputReserve", "promptTokens")
    if any(tokens.get(key) is None for key in required):
        return None
    return {key: deepcopy(tokens[key]) for key in required}


def _profile_or_404(profile_id: str):
    try:
        return get_catalyst_query_profile(profile_id)
    except ModelNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


async def _chat_or_bad_gateway(
    *,
    model: str,
    messages: List[Dict[str, Any]],
    response_format: Optional[Dict[str, Any]],
    temperature: Optional[float],
    dry_multiplier: Optional[float],
    max_tokens: Optional[int],
) -> str:
    async with httpx.AsyncClient() as client:
        try:
            message = await team._chat(
                client,
                model,
                messages,
                response_format=response_format,
                temperature=temperature,
                dry_multiplier=dry_multiplier,
                max_tokens=max_tokens,
            )
        except httpx.HTTPStatusError as error:
            raise HTTPException(
                status_code=502,
                detail=f"model backend returned {error.response.status_code}",
            ) from error
        except httpx.HTTPError as error:
            raise HTTPException(
                status_code=502, detail=f"model backend request failed: {error}"
            ) from error
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise HTTPException(
            status_code=502,
            detail="model response contained no assistant content",
        )
    return content


@router.post("/v1/hub/generate", response_model=GenerateResponse)
async def generate(req: GenerateRequest) -> GenerateResponse:
    """Run one backend completion and return the assistant content verbatim."""
    content = await _chat_or_bad_gateway(
        model=req.model,
        messages=req.messages,
        response_format=req.response_format,
        temperature=req.temperature,
        dry_multiplier=req.dry_multiplier,
        max_tokens=req.max_tokens,
    )
    return GenerateResponse(model=req.model, content=content)


@router.get("/v1/hub/query-profiles")
def list_query_profiles() -> Dict[str, Any]:
    """Expose the Hub's configured query roles and real router availability."""
    backend_models = _backend_models()
    advertised = sorted(backend_models or set())
    return {
        "object": "list",
        "data": [
            catalyst_query_profile_metadata(
                get_catalyst_query_profile(profile_id), backend_models=backend_models
            )
            for profile_id in catalyst_query_profile_ids()
        ],
        "backend": {
            "contract_version": "med-agent-hub.backend-model-inventory.v1",
            **_backend_discovery_metadata(),
            "catalog_reachable": backend_models is not None,
            "advertised_model_ids": advertised,
        },
    }


@router.post(
    "/v1/hub/query-profiles/{profile_id}/roles/{role}/generate",
    response_model=ProfileGenerateResponse,
)
async def generate_query_role(
    profile_id: str, role: str, req: ProfileGenerateRequest
) -> ProfileGenerateResponse:
    """Execute a Hub-configured query role without caller-controlled model settings."""
    profile = _profile_or_404(profile_id)
    if role not in profile.models:
        raise HTTPException(
            status_code=404,
            detail=f"profile {profile_id} does not define query role {role}",
        )
    if any(message.get("role") == "system" for message in req.messages):
        raise HTTPException(
            status_code=422,
            detail="configured query roles do not accept caller-supplied system messages",
        )
    metadata = catalyst_query_profile_metadata(
        profile, backend_models=_backend_models()
    )
    if not metadata["available"]:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "profile_unavailable",
                "profileId": profile_id,
                "unavailableReasons": metadata["unavailable_reasons"],
            },
        )
    knobs = profile.knobs[role]
    model = profile.models[role]
    response_format = deepcopy(req.response_format)
    temperature = float(knobs["temperature"])
    dry_multiplier = float(knobs["dry"])
    max_tokens = int(knobs["maxTokens"])
    rendered_messages = [
        {"role": "system", "content": load_prompt(profile.prompts[role])},
        *deepcopy(req.messages),
    ]
    measurement = await _prompt_measurement(model, rendered_messages, max_tokens)
    request_evidence = _request_evidence(
        profile_id=profile_id,
        role=role,
        model=model,
        messages=rendered_messages,
        response_format=response_format,
        temperature=temperature,
        dry_multiplier=dry_multiplier,
        max_tokens=max_tokens,
        measurement=measurement,
    )
    if measurement["tokens"].get("fits") is False:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "context_window_exceeded",
                "message": (
                    "The exact rendered role request plus its configured output "
                    "reserve exceeds the model context window."
                ),
                "request_evidence": request_evidence,
            },
        )
    try:
        content = await _chat_or_bad_gateway(
            model=model,
            messages=rendered_messages,
            response_format=response_format,
            temperature=temperature,
            dry_multiplier=dry_multiplier,
            max_tokens=max_tokens,
        )
    except HTTPException as error:
        raise HTTPException(
            status_code=error.status_code,
            detail={
                "code": "model_request_failed",
                "message": str(error.detail),
                "request_evidence": request_evidence,
            },
            headers=error.headers,
        ) from error
    except Exception as error:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "model_request_failed",
                "message": (
                    "The model backend did not return a usable assistant response."
                ),
                "request_evidence": request_evidence,
            },
        ) from error
    return ProfileGenerateResponse(
        profile_id=profile_id,
        role=role,
        model=model,
        content=content,
        request_evidence=request_evidence,
        token_accounting=_compatible_token_accounting(measurement),
    )
