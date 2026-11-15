"""Tests for dynamic byte-budget LRU GPU memory management on llama-router."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List

import httpx
import pytest

from server import config, context_sources, router_memory, team


@pytest.fixture(autouse=True)
def _reset_router_memory_state():
    router_memory.reset_state()
    context_sources.RouterTokenCounter.reset_capability_cache()
    yield
    router_memory.reset_state()
    context_sources.RouterTokenCounter.reset_capability_cache()


def _router_item(
    model_id: str,
    status_value: str,
    *,
    ctx_size: int = 24576,
    swa_full: bool = False,
) -> Dict[str, Any]:
    args = ["/app/llama-server", "--ctx-size", str(ctx_size)]
    if swa_full:
        args.append("--swa-full")
    return {
        "id": model_id,
        "object": "model",
        "owned_by": "llamacpp",
        "status": {"value": status_value, "args": args},
    }


def _patch_router_http(
    monkeypatch,
    state: Dict[str, Dict[str, Any]],
    calls: List[tuple[str, str, Any]],
    *,
    unload_polls_before_exit: int = 1,
):
    pending_unloads: Dict[str, int] = {}

    class FakeAsyncClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def aclose(self):
            return None

        async def get(self, url: str, *, headers=None, timeout=None):
            del headers, timeout
            calls.append(("GET", url, None))
            for victim in list(pending_unloads):
                pending_unloads[victim] -= 1
                if pending_unloads[victim] <= 0:
                    pending_unloads.pop(victim, None)
                    if victim in state:
                        state[victim]["status"]["value"] = "unloaded"
            return httpx.Response(
                200,
                json={"object": "list", "data": list(state.values())},
                request=httpx.Request("GET", url),
            )

        async def post(self, url: str, *, json=None, headers=None, timeout=None):
            del headers, timeout
            calls.append(("POST", url, json))
            if url.endswith("/models/unload"):
                victim = str((json or {}).get("model") or "")
                if victim in state and state[victim]["status"]["value"] in {
                    "loaded",
                    "loading",
                }:
                    pending_unloads[victim] = unload_polls_before_exit
                    return httpx.Response(
                        200,
                        json={"success": True},
                        request=httpx.Request("POST", url),
                    )
                return httpx.Response(
                    400,
                    json={"error": {"code": 400, "message": "model is not running"}},
                    request=httpx.Request("POST", url),
                )
            if url.endswith("/v1/chat/completions/input_tokens"):
                model = str((json or {}).get("model") or "")
                if model in state:
                    state[model]["status"]["value"] = "loaded"
                return httpx.Response(
                    200,
                    json={"input_tokens": 128},
                    request=httpx.Request("POST", url),
                )
            raise AssertionError(f"Unexpected POST url in test: {url}")

    monkeypatch.setattr(router_memory.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(context_sources.httpx, "AsyncClient", FakeAsyncClient)


def test_small_and_medium_models_co_reside_until_byte_budget_exceeded(monkeypatch):
    monkeypatch.setenv("MED_AGENT_ROUTER_VRAM_BUDGET_MIB", "40960")
    monkeypatch.setenv("LLAMA_MODEL_DIR", "/tmp/nonexistent-test-models")

    state = {
        "gemma-e4b": _router_item("gemma-e4b", "loaded", ctx_size=32768),
        "qwen3.5-9b": _router_item("qwen3.5-9b", "loaded", ctx_size=24576),
        "gemma-4-12b": _router_item(
            "gemma-4-12b", "unloaded", ctx_size=49152, swa_full=True
        ),
        "gemma-26b": _router_item(
            "gemma-26b", "unloaded", ctx_size=24576, swa_full=True
        ),
        "qwen3.6-35b": _router_item("qwen3.6-35b", "unloaded", ctx_size=24576),
    }
    calls: List[tuple[str, str, Any]] = []
    _patch_router_http(monkeypatch, state, calls, unload_polls_before_exit=2)

    base = "http://router.local:8077"
    router_memory.touch_model(base, "gemma-e4b", timestamp=100.0)
    router_memory.touch_model(base, "qwen3.5-9b", timestamp=200.0)

    # 1. Three small/medium models fit together (~33.8 GiB <= 40.0 GiB), so zero unloads.
    evicted_for_12b = asyncio.run(
        router_memory.ensure_router_capacity("gemma-4-12b", base_url=base)
    )
    assert evicted_for_12b == []
    assert not any(url.endswith("/models/unload") for _, url, _ in calls)

    # Simulate gemma-4-12b now loaded & recently used.
    state["gemma-4-12b"]["status"]["value"] = "loaded"
    router_memory.touch_model(base, "gemma-4-12b", timestamp=300.0)

    # 2. Requesting gemma-26b proactively evicts LRU gemma-e4b then qwen3.5-9b, keeping gemma-4-12b.
    calls.clear()
    evicted_for_26b = asyncio.run(
        router_memory.ensure_router_capacity("gemma-26b", base_url=base)
    )
    assert evicted_for_26b == ["gemma-e4b", "qwen3.5-9b"]
    unload_posts = [
        body["model"]
        for method, url, body in calls
        if method == "POST" and url.endswith("/models/unload")
    ]
    assert unload_posts == ["gemma-e4b", "qwen3.5-9b"]
    # Polling waited until both victims transitioned to unloaded before returning.
    assert state["gemma-e4b"]["status"]["value"] == "unloaded"
    assert state["qwen3.5-9b"]["status"]["value"] == "unloaded"
    assert state["gemma-4-12b"]["status"]["value"] == "loaded"
    get_polls = [url for method, url, _ in calls if method == "GET"]
    assert len(get_polls) >= 3  # initial /v1/models + >=2 poll checks

    # Simulate gemma-26b loaded, gemma-4-12b still warm.
    state["gemma-26b"]["status"]["value"] = "loaded"
    router_memory.touch_model(base, "gemma-26b", timestamp=400.0)

    # 3. Requesting qwen3.6-35b (~22.6 GiB) cannot coexist with gemma-26b (~18.3 GiB),
    # so both gemma-4-12b (oldest) and gemma-26b are unloaded down to 1 model.
    calls.clear()
    evicted_for_35b = asyncio.run(
        router_memory.ensure_router_capacity("qwen3.6-35b", base_url=base)
    )
    assert evicted_for_35b == ["gemma-4-12b", "gemma-26b"]


def test_tokenizer_count_chat_enforces_capacity_before_input_tokens(monkeypatch):
    monkeypatch.setenv("MED_AGENT_ROUTER_VRAM_BUDGET_MIB", "30000")
    monkeypatch.setenv("LLAMA_MODEL_DIR", "/tmp/nonexistent-test-models")
    monkeypatch.setattr(
        context_sources,
        "llm_config",
        config.LLMConfig(
            provider="openai-compatible", base_url="http://router.local:8077"
        ),
    )
    monkeypatch.setattr(
        context_sources.model_provisioner,
        "ensure_local_model_provisioned",
        lambda _alias: asyncio.sleep(0, result=True),
    )

    state = {
        "gemma-26b": _router_item("gemma-26b", "loaded", ctx_size=24576, swa_full=True),
        "gemma-4-12b": _router_item(
            "gemma-4-12b", "unloaded", ctx_size=32768, swa_full=True
        ),
    }
    calls: List[tuple[str, str, Any]] = []
    _patch_router_http(monkeypatch, state, calls, unload_polls_before_exit=1)
    router_memory.touch_model("http://router.local:8077", "gemma-26b", timestamp=50.0)

    async def exercise():
        counter = context_sources.RouterTokenCounter("http://router.local:8077")
        try:
            return await counter.count_chat(
                "gemma-4-12b",
                {
                    "messages": [
                        {"role": "user", "content": "What are my health concerns?"}
                    ]
                },
            )
        finally:
            await counter.aclose()

    count = asyncio.run(exercise())
    assert count == 128

    post_paths = [url for method, url, _ in calls if method == "POST"]
    assert post_paths == [
        "http://router.local:8077/models/unload",
        "http://router.local:8077/v1/chat/completions/input_tokens",
    ]
    assert state["gemma-26b"]["status"]["value"] == "unloaded"
    assert state["gemma-4-12b"]["status"]["value"] == "loaded"
    assert not team._ROUTER_LOCK.locked()


def test_skips_standard_openai_catalog_and_remote_mode(monkeypatch):
    monkeypatch.setenv("MED_AGENT_ROUTER_VRAM_BUDGET_MIB", "1024")
    monkeypatch.setenv("LLAMA_MODEL_DIR", "/tmp/nonexistent-test-models")
    calls: List[tuple[str, str, Any]] = []

    class StandardOpenAIClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def aclose(self):
            return None

        async def get(self, url: str, *, headers=None, timeout=None):
            del headers, timeout
            calls.append(("GET", url, None))
            return httpx.Response(
                200,
                json={"object": "list", "data": [{"id": "gpt-4o", "object": "model"}]},
                request=httpx.Request("GET", url),
            )

        async def post(self, url: str, *, json=None, headers=None, timeout=None):
            raise AssertionError(f"Unexpected unload POST to non-llama-router: {url}")

    monkeypatch.setattr(router_memory.httpx, "AsyncClient", StandardOpenAIClient)

    assert (
        asyncio.run(
            router_memory.ensure_router_capacity(
                "gemma-26b", base_url="https://api.example.com/v1", is_remote=True
            )
        )
        == []
    )
    assert calls == []

    # OpenAI-compatible server without llama.cpp `status.value` is a safe no-op.
    assert (
        asyncio.run(
            router_memory.ensure_router_capacity(
                "gpt-4o", base_url="http://vllm.local:8000/v1", is_remote=False
            )
        )
        == []
    )
    assert calls == [("GET", "http://vllm.local:8000/v1/models", None)]


def test_auto_detect_vram_budget_platforms_and_test_guard(monkeypatch):
    monkeypatch.delenv("MED_AGENT_ROUTER_VRAM_BUDGET_MIB", raising=False)
    # Under pytest without an explicit budget env var, capacity check stays inactive.
    assert router_memory.resolve_vram_budget_mib("http://localhost:8077") == 0

    monkeypatch.setattr(router_memory, "_nvidia_gpu_memory_mib", lambda: 24576)
    assert router_memory.auto_detect_vram_budget_mib("http://llama-router:8077") == int(
        24576 * 0.88
    )

    monkeypatch.setattr(router_memory, "_nvidia_gpu_memory_mib", lambda: None)
    monkeypatch.setattr(
        router_memory.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=65536 * 1024 * 1024),
    )
    monkeypatch.setattr(router_memory.sys, "platform", "darwin")
    assert router_memory.auto_detect_vram_budget_mib("http://localhost:8077") == int(
        65536 * 0.65
    )

    monkeypatch.setattr(
        router_memory.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=46146 * 1024 * 1024),
    )
    monkeypatch.setattr(router_memory.sys, "platform", "linux")
    assert router_memory.auto_detect_vram_budget_mib(
        "http://host.docker.internal:8077"
    ) == int(46146 * 0.85)
    assert router_memory.auto_detect_vram_budget_mib("http://llama-router:8077") == int(
        46146 * 0.75
    )


def test_reentrant_hold_router_lock_does_not_deadlock_or_double_count(monkeypatch):
    monkeypatch.setattr(team, "_ROUTER_LOCK", asyncio.Lock())
    evidence = team.RouterSlotEvidence()
    token = team.activate_router_slot_evidence(evidence)

    async def exercise():
        async with team.hold_router_lock(record_slot_evidence=True):
            assert team._ROUTER_LOCK.locked()
            async with team.hold_router_lock(record_slot_evidence=False):
                assert team._ROUTER_LOCK.locked()
            assert evidence.snapshot() == {
                "acquisitions": 1,
                "releases": 0,
                "active": 1,
            }
        assert evidence.fully_released
        assert not team._ROUTER_LOCK.locked()

    try:
        asyncio.run(exercise())
    finally:
        team.reset_router_slot_evidence(token)


def test_team_chat_and_stream_enforce_capacity_under_router_lock(monkeypatch):
    seen: List[tuple[str, bool]] = []

    async def fake_provision(_alias: str) -> bool:
        return True

    async def fake_capacity(
        model: str, *, base_url: str, api_key: str = "", is_remote: bool = False
    ):
        del base_url, api_key
        assert not is_remote
        seen.append((model, team._ROUTER_LOCK.locked()))
        return []

    monkeypatch.setattr(team, "_ROUTER_LOCK", asyncio.Lock())
    monkeypatch.setattr(
        team,
        "llm_config",
        config.LLMConfig(
            provider="openai-compatible", base_url="http://router.local:8077"
        ),
    )
    monkeypatch.setattr(
        team.model_provisioner, "ensure_local_model_provisioned", fake_provision
    )
    monkeypatch.setattr(team.router_memory, "ensure_router_capacity", fake_capacity)

    class FakeChatClient:
        async def post(self, url, *, json=None, headers=None, timeout=None):
            del url, json, headers, timeout
            return httpx.Response(
                200,
                json={"choices": [{"message": {"role": "assistant", "content": "ok"}}]},
                request=httpx.Request(
                    "POST", "http://router.local:8077/v1/chat/completions"
                ),
            )

        def stream(self, method, url, *, json=None, headers=None, timeout=None):
            del method, url, json, headers, timeout

            class _StreamResp:
                status_code = 200

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_args):
                    return None

                async def aiter_lines(self):
                    yield 'data: {"choices":[{"delta":{"content":"hi"}}]}'
                    yield "data: [DONE]"

            return _StreamResp()

    async def exercise():
        msg = await team._chat(
            FakeChatClient(), "gemma-4-12b", [{"role": "user", "content": "hi"}]
        )
        assert msg["content"] == "ok"
        chunks = [
            piece
            async for piece in team._stream_chat_chunks(
                FakeChatClient(), "gemma-26b", [{"role": "user", "content": "hi"}]
            )
        ]
        assert chunks == ["hi"]

    asyncio.run(exercise())
    assert seen == [("gemma-4-12b", True), ("gemma-26b", True)]
    assert not team._ROUTER_LOCK.locked()
