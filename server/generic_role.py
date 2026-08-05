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

from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from . import team
from .levels_loader import (
    ModelNotFoundError,
    catalyst_query_profile_metadata,
    catalyst_query_profile_ids,
    get_catalyst_query_profile,
)
from .openai_compat import _backend_discovery_metadata, _served_backend_model_metadata
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


def _backend_models() -> set[str] | None:
    discovered = _served_backend_model_metadata()
    return None if discovered is None else set(discovered)


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
    content = await _chat_or_bad_gateway(
        model=profile.models[role],
        messages=[
            {"role": "system", "content": load_prompt(profile.prompts[role])},
            *req.messages,
        ],
        response_format=req.response_format,
        temperature=float(knobs["temperature"]),
        dry_multiplier=float(knobs["dry"]),
        max_tokens=int(knobs["maxTokens"]),
    )
    return ProfileGenerateResponse(
        profile_id=profile_id,
        role=role,
        model=profile.models[role],
        content=content,
    )
