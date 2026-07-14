"""Runtime configuration for the med-agent-hub service."""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.getenv("UVICORN_ENV_FILE", ".env"))


@dataclass(frozen=True)
class LLMConfig:
    """OpenAI-compatible backend used by hub stages."""

    base_url: str = os.getenv("LLM_BASE_URL", "http://localhost:8077")
    api_key: str = os.getenv("LLM_API_KEY", "")
    temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "2000"))


@dataclass(frozen=True)
class QueryStoreConfig:
    """Optional patient-record source; inline context works without it."""

    base_url: str = os.getenv("QUERYSTORE_BASE_URL", "")
    username: str = os.getenv("QUERYSTORE_USERNAME", "")
    password: str = os.getenv("QUERYSTORE_PASSWORD", "")

    @property
    def enabled(self) -> bool:
        return all((self.base_url.strip(), self.username.strip(), self.password))

    @property
    def partially_configured(self) -> bool:
        values = (self.base_url.strip(), self.username.strip(), self.password)
        return any(values) and not all(values)


llm_config = LLMConfig()
querystore_config = QueryStoreConfig()


SYNTH_REPEAT_PENALTY = float(os.getenv("SYNTH_REPEAT_PENALTY", "1.15"))
ORCHESTRATOR_DRY_MULTIPLIER = float(os.getenv("ORCHESTRATOR_DRY_MULTIPLIER", "0.0"))
EXPERT_DRY_MULTIPLIER = float(os.getenv("EXPERT_DRY_MULTIPLIER", "0.8"))
SYNTH_DRY_MULTIPLIER = float(os.getenv("SYNTH_DRY_MULTIPLIER", "0.8"))

_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def resolve_hub_build_revision() -> str:
    """Return the exact source commit or an empty string when provenance is unavailable."""
    configured = os.getenv("HUB_BUILD_REVISION", "").strip().lower()
    if configured:
        return configured if _COMMIT_SHA_RE.fullmatch(configured) else ""
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip().lower()
    except (OSError, subprocess.CalledProcessError):
        return ""
    return revision if _COMMIT_SHA_RE.fullmatch(revision) else ""


def validate_config() -> None:
    if not llm_config.base_url:
        raise ValueError(
            "LLM_BASE_URL must identify the OpenAI-compatible model router."
        )
    if not resolve_hub_build_revision():
        raise ValueError(
            "HUB_BUILD_REVISION must be the 40-character Git commit for packaged deployments."
        )
    if querystore_config.partially_configured:
        raise ValueError(
            "QUERYSTORE_BASE_URL, QUERYSTORE_USERNAME, and QUERYSTORE_PASSWORD "
            "must be set together; leave all three empty to disable Querystore."
        )
