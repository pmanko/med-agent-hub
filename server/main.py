"""
med-agent-hub web facade.

Exposes a small observability surface (`/`, `/manifest`, `/health`) and the
OpenAI-compat bridge (`/v1/chat/completions`, `/v1/models`, `/v1/agents`)
that lets external consumers (e.g. OpenMRS chartsearchai) treat med-agent-hub
as a drop-in LLM endpoint while the request is routed internally through the
A2A agent team (router → medical | clinical).

Legacy endpoints (`/generate/*`, `/chat`) and the bundled `client/`/`web/`
frontends were removed in feature 005 (med-agent-hub bridge). The
OpenAI-compat surface is the only consumer contract going forward.
"""

import logging
import time

import psutil
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import agent_config, llm_config, a2a_endpoints

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_start_time = time.time()

app = FastAPI(
    title="med-agent-hub",
    description=(
        "A2A-routed medical chat. OpenAI-compat /v1/chat/completions front; "
        "router agent dispatches to medical or clinical subagent."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "status": "Server is running",
        "uptime_seconds": round(time.time() - server_start_time, 2),
        "a2a_enabled": agent_config.enable_a2a,
        "subagent_models": {
            "orchestrator": llm_config.orchestrator_model,
            "medical": llm_config.med_model,
            "clinical": llm_config.clinical_research_model,
        },
    }


@app.get("/manifest")
def get_manifest():
    """
    Lightweight pointer to active A2A agent endpoints. Each agent serves its
    own /.well-known/agent-card.json — this endpoint is for quick diagnostics
    during deployment, not a load-bearing discovery contract. Consumers
    needing skill discovery should use /v1/agents (the OpenAI-compat bridge's
    skill enumeration, populated in feature 005 phase 1B).
    """
    return {
        "router_agent": a2a_endpoints.router_url,
        "medical_agent": getattr(a2a_endpoints, "medical_url", a2a_endpoints.medgemma_url),
        "clinical_agent": a2a_endpoints.clinical_url,
    }


@app.get("/health")
def health_check():
    uptime = time.time() - server_start_time
    memory_info = {}
    try:
        process = psutil.Process()
        memory_info["process_memory_gb"] = round(process.memory_info().rss / 1024 ** 3, 2)
        memory_info["process_memory_percent"] = round(process.memory_percent(), 1)
    except Exception:  # pragma: no cover — defensive against psutil failures
        pass
    return {
        "status": "healthy",
        "uptime_seconds": round(uptime, 2),
        "memory": memory_info,
        "timestamp": time.time(),
    }


# OpenAI-compat bridge (/v1/chat/completions, /v1/models, /v1/agents) is
# mounted here in feature 005 phase 1B once `server/openai_compat.py` lands:
#
#     from .openai_compat import router as openai_router
#     app.include_router(openai_router)
