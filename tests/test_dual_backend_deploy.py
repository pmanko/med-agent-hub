"""Tests for dual-backend (llama-router + remote OpenAI/OpenRouter) deployment features."""

from __future__ import annotations

import asyncio

import httpx
from fastapi.testclient import TestClient

from server import (
    config,
    context_sources,
    engine,
    levels_loader,
    main,
    model_provisioner,
    openai_compat,
    team,
)


def _set_llm_config(monkeypatch, cfg: config.LLMConfig) -> None:
    for mod in (config, context_sources, engine, main, openai_compat, team):
        monkeypatch.setattr(mod, "llm_config", cfg)


def test_default_mode_preserves_router_token_counter(monkeypatch):
    _set_llm_config(monkeypatch, config.LLMConfig())
    assert config.llm_config.use_local_tokenizer is False
    assert isinstance(engine._default_token_counter(), context_sources.RouterTokenCounter)
    assert isinstance(context_sources.default_token_counter(), context_sources.RouterTokenCounter)


def test_openrouter_mode_models_endpoint_and_local_tokenizer(monkeypatch):
    cfg = config.LLMConfig(
        provider="openrouter",
        openrouter_base_url="https://openrouter.example/api/v1",
        openrouter_api_key="or-test-key",
        model_map_raw="gemma-e4b=google/gemini-2.5-flash",
    )
    _set_llm_config(monkeypatch, cfg)
    assert cfg.is_openrouter is True
    assert cfg.is_remote_openai is True
    assert cfg.use_local_tokenizer is True
    assert isinstance(
        engine._default_token_counter(), context_sources.LocalDeterministicTokenCounter
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("OpenRouter product catalog should not probe /v1/models")

    monkeypatch.setattr(openai_compat.httpx, "get", fail_if_called)

    client = TestClient(main.app)
    resp = client.get("/v1/models")
    assert resp.status_code == 200
    body = resp.json()
    by_id = {item["id"]: item for item in body["data"]}
    e4b = by_id["gemma-e4b"]
    assert e4b["available"] is True
    assert e4b["resolved_backend_model"] == "google/gemini-2.5-flash"
    assert by_id["gemma-4-12b"]["available"] is True
    assert {m["id"] for m in body["execution_modes"]} >= {
        "quick",
        "standard",
        "verified",
    }
    assert levels_loader.resolve_backend_model(
        "gemma-e4b", provider=cfg.provider, model_map_raw=cfg.model_map_raw
    ) == "google/gemini-2.5-flash"


def test_openrouter_chat_strips_llama_params_and_normalizes_v1_url(monkeypatch):
    cfg = config.LLMConfig(
        provider="openrouter",
        openrouter_base_url="https://openrouter.example/api/v1",
        openrouter_api_key="or-test-key",
        model_map_raw='{"gemma-e4b": "google/gemini-2.5-flash"}',
    )
    _set_llm_config(monkeypatch, cfg)

    captured = {}

    class FakeClient:
        async def post(self, url, *, json=None, headers=None, timeout=None):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {"message": {"role": "assistant", "content": '{"answer":"ok"}'}}
                    ]
                },
                request=httpx.Request("POST", url),
            )

    counter = context_sources.LocalDeterministicTokenCounter()
    policy = team.ChatBudgetPolicy(
        counter=counter, context_window=24576, reserved_output_tokens=4096
    )
    token = team.activate_chat_budget(policy)
    try:
        msg = asyncio.run(
            team._chat(
                FakeClient(),
                "gemma-e4b",
                [{"role": "user", "content": "Hello"}],
                repeat_penalty=1.15,
                dry_multiplier=0.8,
            )
        )
    finally:
        team.reset_chat_budget(token)

    assert msg["content"] == '{"answer":"ok"}'
    assert captured["url"] == "https://openrouter.example/api/v1/chat/completions"
    assert captured["json"]["model"] == "google/gemini-2.5-flash"
    assert "dry_multiplier" not in captured["json"]
    assert "repeat_penalty" not in captured["json"]
    assert captured["headers"]["Authorization"] == "Bearer or-test-key"
    assert counter.stats()["chat_count_mode"] == "local_deterministic"


def test_optional_hub_api_key_protects_api_but_keeps_health_public(monkeypatch):
    monkeypatch.setenv("HUB_API_KEY", "hub-secret-token")
    _set_llm_config(
        monkeypatch,
        config.LLMConfig(
            provider="openrouter",
            openrouter_api_key="or-test-key",
            hub_api_key="hub-secret-token",
        ),
    )

    client = TestClient(main.app)
    assert client.get("/health").status_code == 200
    assert client.get("/v1/models").status_code == 401
    assert (
        client.get(
            "/v1/models", headers={"Authorization": "Bearer wrong-token"}
        ).status_code
        == 401
    )
    ok = client.get(
        "/v1/models", headers={"Authorization": "Bearer hub-secret-token"}
    )
    assert ok.status_code == 200


def test_model_provisioner_falls_back_to_bundled_manifest(monkeypatch, tmp_path):
    monkeypatch.delenv("LLAMA_MODELS_MANIFEST", raising=False)
    monkeypatch.setenv("LLAMA_MODEL_DIR", str(tmp_path))

    path = model_provisioner._manifest_path()
    assert path is not None
    assert path.name == "llama-router.models.tsv"
    records = model_provisioner.load_pinned_manifest()
    assert "gemma-e4b" in records
    assert model_provisioner.parse_preload_models("gemma-e4b, gemma-e2b, gemma-e4b") == [
        "gemma-e4b",
        "gemma-e2b",
    ]
