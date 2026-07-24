"""Generic structured single-shot completion — the hub as a domain-agnostic role executor.

Clients own their own multi-step orchestration and prompts. They call this
endpoint once per role with a model, a message list, and an optional
``response_format``, and receive back the model's assistant content. The hub
contributes only what is genuinely shared infrastructure:

* the model-router connection and its single-slot serialization (``_ROUTER_LOCK``
  inside :func:`server.team._chat` — concurrent callers must not race the router),
* the provider / auth / timeout abstraction, and
* OpenAI-compatible structured-output pass-through.

No domain logic lives here. The Catalyst gateway composes generate -> lint ->
review -> repair on top of this primitive; other clients can compose whatever
they need. This is the endpoint that survives once Catalyst-specific code is
removed from the hub.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from . import team

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


@router.post("/v1/hub/generate", response_model=GenerateResponse)
async def generate(req: GenerateRequest) -> GenerateResponse:
    """Run one backend completion and return the assistant content verbatim."""

    async with httpx.AsyncClient() as client:
        try:
            message = await team._chat(
                client,
                req.model,
                req.messages,
                response_format=req.response_format,
                temperature=req.temperature,
                dry_multiplier=req.dry_multiplier,
                max_tokens=req.max_tokens,
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
    return GenerateResponse(model=req.model, content=content.strip())
