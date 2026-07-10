"""Runtime configuration for the med-agent-hub service."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.getenv("UVICORN_ENV_FILE", ".env"))


@dataclass(frozen=True)
class LLMConfig:
    """OpenAI-compatible backend used by hub stages."""

    base_url: str = os.getenv("LLM_BASE_URL", "http://localhost:8077")
    api_key: str = os.getenv("LLM_API_KEY", "")
    orchestrator_model: str = os.getenv("ORCHESTRATOR_MODEL", "google/gemma-4-e4b")
    synthesizer_model: str = os.getenv(
        "SYNTHESIZER_MODEL", os.getenv("ORCHESTRATOR_MODEL", "google/gemma-4-e4b")
    )
    med_model: str = os.getenv("MED_MODEL", "medgemma-1.5-4b-it")
    temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "2000"))


@dataclass(frozen=True)
class QueryStoreConfig:
    """Optional patient-record source; inline context works without it."""

    base_url: str = os.getenv("QUERYSTORE_BASE_URL", "")
    username: str = os.getenv(
        "QUERYSTORE_USERNAME", "admin" if os.getenv("QUERYSTORE_BASE_URL") else ""
    )
    password: str = os.getenv(
        "QUERYSTORE_PASSWORD", "Admin123" if os.getenv("QUERYSTORE_BASE_URL") else ""
    )

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)


llm_config = LLMConfig()
querystore_config = QueryStoreConfig()


SYNTH_REPEAT_PENALTY = float(os.getenv("SYNTH_REPEAT_PENALTY", "1.15"))
ORCHESTRATOR_DRY_MULTIPLIER = float(os.getenv("ORCHESTRATOR_DRY_MULTIPLIER", "0.0"))
EXPERT_DRY_MULTIPLIER = float(os.getenv("EXPERT_DRY_MULTIPLIER", "0.8"))
SYNTH_DRY_MULTIPLIER = float(os.getenv("SYNTH_DRY_MULTIPLIER", "0.8"))


def validate_config() -> None:
    if not llm_config.base_url:
        raise ValueError(
            "LLM_BASE_URL must identify the OpenAI-compatible model router."
        )
