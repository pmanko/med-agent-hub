"""
med-agent-hub web facade.

Exposes a small observability surface (`/`, `/health`) and the OpenAI-compat
bridge (`/v1/chat/completions`, `/v1/models`) so external consumers (e.g.
OpenMRS chartsearchai) can treat med-agent-hub as a drop-in LLM endpoint while
the request runs through the in-process Med Agent Team. The OpenAI-compat
surface is the only consumer contract.
"""

import logging
import time

import psutil
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import llm_config
from .openai_compat import router as openai_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_start_time = time.time()

app = FastAPI(
    title="med-agent-hub",
    description=(
        "In-process Med Agent Team behind an OpenAI-compat /v1/chat/completions "
        "+ /v1/models surface; one request runs the orchestrator → tools → "
        "synthesis loop in this process."
    ),
)

# Server-to-server only (the chartsearchai backend calls the hub); no browser
# credentials, so wildcard origins with credentials disabled is the valid combo.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "status": "Server is running",
        "uptime_seconds": round(time.time() - server_start_time, 2),
        "team_models": {
            "orchestrator": llm_config.orchestrator_model,
            "synthesizer": llm_config.synthesizer_model,
            "medical": llm_config.med_model,
        },
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


app.include_router(openai_router)
