"""Focused configuration contracts for the consolidated hub runtime."""

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
