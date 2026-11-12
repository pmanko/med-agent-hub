"""
med-agent-hub web facade.

Exposes observability and an OpenAI-compatible profile execution API for
ChartSearchAI, the validation harness, and direct clients.
"""

import asyncio
from contextlib import asynccontextmanager
import hmac
import logging
import os
import time

import psutil
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import model_provisioner
from .config import llm_config, validate_config
from .generic_role import router as generic_role_router
from .levels_loader import (
    validate_catalyst_query_profiles,
    validate_profiles,
    validate_wahs_profiles,
)
from .openai_compat import router as openai_router
from .wahs_demo import router as wahs_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_start_time = time.time()
validate_config()
validate_profiles()
validate_catalyst_query_profiles()
validate_wahs_profiles()

_PUBLIC_PATHS = frozenset({"/", "/health"})


async def _preload_local_models(aliases: list[str]) -> None:
    for alias in aliases:
        try:
            await model_provisioner.ensure_local_model_provisioned(alias)
        except Exception as exc:  # pragma: no cover - background warmup best-effort
            logger.warning("Background model preload failed for %s: %s", alias, exc)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    preload_raw = os.getenv("MED_AGENT_PRELOAD_MODELS", "")
    preload_aliases = model_provisioner.parse_preload_models(preload_raw)
    preload_task: asyncio.Task | None = None
    if preload_aliases and not llm_config.is_remote_openai:
        preload_task = asyncio.create_task(_preload_local_models(preload_aliases))
    try:
        yield
    finally:
        if preload_task is not None and not preload_task.done():
            preload_task.cancel()


app = FastAPI(
    title="med-agent-hub",
    description=(
        "Profile-driven clinical answer stages behind an OpenAI-compatible "
        "/v1/chat/completions and /v1/models surface."
    ),
    lifespan=_lifespan,
)

# Direct clients may call the hub; browser credentials remain disabled.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def enforce_optional_hub_api_key(request, call_next):
    expected_key = (
        os.getenv("HUB_API_KEY") or llm_config.hub_api_key or ""
    ).strip()
    if (
        expected_key
        and request.method != "OPTIONS"
        and request.url.path not in _PUBLIC_PATHS
    ):
        auth_header = (request.headers.get("authorization") or "").strip()
        prefix, _, provided = auth_header.partition(" ")
        if prefix.lower() != "bearer" or not hmac.compare_digest(
            provided.strip(), expected_key
        ):
            return JSONResponse(
                status_code=401,
                content={
                    "detail": {
                        "code": "unauthorized",
                        "message": "Valid Bearer token required.",
                    }
                },
            )
    return await call_next(request)


@app.middleware("http")
async def add_private_network_access_headers(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


@app.get("/")
def read_root():
    return {
        "status": "Server is running",
        "uptime_seconds": round(time.time() - server_start_time, 2),
        "model_backend": llm_config.effective_base_url,
    }


@app.get("/health")
def health_check():
    uptime = time.time() - server_start_time
    memory_info = {}
    try:
        process = psutil.Process()
        memory_info["process_memory_gb"] = round(process.memory_info().rss / 1024**3, 2)
        memory_info["process_memory_percent"] = round(process.memory_percent(), 1)
    except Exception:  # pragma: no cover — defensive against psutil failures
        pass
    return {
        "status": "healthy",
        "uptime_seconds": round(uptime, 2),
        "memory": memory_info,
        "timestamp": time.time(),
    }


app.include_router(openai_router)
app.include_router(generic_role_router)
app.include_router(wahs_router)
