"""
med-agent-hub web facade.

Exposes observability and an OpenAI-compatible profile execution API for
ChartSearchAI, the validation harness, and direct clients.
"""

import logging
import time

import psutil
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import llm_config, validate_config
from .levels_loader import validate_profiles
from .openai_compat import router as openai_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_start_time = time.time()
validate_config()
validate_profiles()

app = FastAPI(
    title="med-agent-hub",
    description=(
        "Profile-driven clinical answer stages behind an OpenAI-compatible "
        "/v1/chat/completions and /v1/models surface."
    ),
)

# Direct clients may call the hub; browser credentials remain disabled.
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
        "model_backend": llm_config.base_url,
    }


@app.get("/health")
def health_check():
    uptime = time.time() - server_start_time
    memory_info = {}
    try:
        process = psutil.Process()
        memory_info["process_memory_gb"] = round(
            process.memory_info().rss / 1024**3, 2
        )
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
