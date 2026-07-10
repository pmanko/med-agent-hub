"""Focused configuration contracts for the consolidated hub runtime."""

import os
import subprocess
import sys

import pytest

from server import config


def test_llm_backend_is_openai_compatible_router_configuration():
    assert config.llm_config.base_url.startswith(("http://", "https://"))
    assert config.llm_config.orchestrator_model
    assert config.llm_config.synthesizer_model
    assert config.llm_config.med_model
    config.validate_config()


def test_querystore_is_optional():
    blank = config.QueryStoreConfig(base_url="", username="", password="")
    configured = config.QueryStoreConfig(
        base_url="http://openmrs", username="service", password="secret"
    )

    assert blank.enabled is False
    assert configured.enabled is True


@pytest.mark.parametrize(
    "partial",
    (
        config.QueryStoreConfig(
            base_url="http://openmrs", username="", password=""
        ),
        config.QueryStoreConfig(
            base_url="http://openmrs", username="service", password=""
        ),
        config.QueryStoreConfig(base_url="", username="service", password="secret"),
    ),
)
def test_partial_querystore_configuration_is_disabled_and_rejected(
    monkeypatch, partial
):
    monkeypatch.setattr(config, "querystore_config", partial)

    assert partial.enabled is False
    with pytest.raises(ValueError, match="must be set together"):
        config.validate_config()


def test_service_startup_does_not_invent_querystore_credentials():
    env = os.environ.copy()
    env["QUERYSTORE_BASE_URL"] = "http://openmrs"
    env.pop("QUERYSTORE_USERNAME", None)
    env.pop("QUERYSTORE_PASSWORD", None)

    result = subprocess.run(
        [sys.executable, "-c", "import server.main"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "must be set together" in result.stderr


def test_no_legacy_provider_or_agent_configuration_is_exported():
    removed = (
        "agent_config",
        "a2a_endpoints",
        "orchestrator_config",
        "openmrs_config",
        "spark_config",
        "local_config",
    )
    assert [name for name in removed if hasattr(config, name)] == []
