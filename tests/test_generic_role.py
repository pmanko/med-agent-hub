"""Tests for the generic structured single-shot role-executor endpoint.

The endpoint is a thin wrapper over ``team._chat`` (the shared, router-serialized
backend primitive); these tests patch ``_chat`` so no real model is required.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import httpx
import rfc8785
from fastapi.testclient import TestClient

from server import generic_role, team
from server.main import app


def _post(body: Dict[str, Any]):
    return TestClient(app).post("/v1/hub/generate", json=body)


def test_returns_model_content_and_forwards_arguments():
    captured: Dict[str, Any] = {}

    async def fake_chat(
        client,
        model: str,
        messages: List[Dict[str, Any]],
        *,
        response_format: Optional[Dict[str, Any]] = None,
        temperature: Optional[float] = None,
        dry_multiplier: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        captured.update(
            model=model,
            messages=messages,
            response_format=response_format,
            temperature=temperature,
            dry_multiplier=dry_multiplier,
        )
        return {"role": "assistant", "content": '{"status":"ready"}'}

    with patch.object(team, "_chat", side_effect=fake_chat):
        response = _post(
            {
                "model": "gemma-4-12b-q4",
                "messages": [
                    {"role": "system", "content": "You write SQL."},
                    {"role": "user", "content": "list tests"},
                ],
                "response_format": {"type": "json_schema"},
                "temperature": 0,
                "dry_multiplier": 0,
            }
        )

    assert response.status_code == 200
    body = response.json()
    assert body == {"model": "gemma-4-12b-q4", "content": '{"status":"ready"}'}
    # Arguments are forwarded verbatim to the shared backend primitive.
    assert captured["model"] == "gemma-4-12b-q4"
    assert captured["response_format"] == {"type": "json_schema"}
    assert captured["temperature"] == 0
    assert captured["dry_multiplier"] == 0


def test_returns_nonempty_model_content_verbatim():
    async def fake_chat(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"role": "assistant", "content": ' \n  {"status":"ready"}\n '}

    with patch.object(team, "_chat", side_effect=fake_chat):
        response = _post(
            {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
        )

    assert response.status_code == 200
    assert response.json()["content"] == ' \n  {"status":"ready"}\n '


def test_empty_content_is_a_bad_gateway():
    async def fake_chat(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"role": "assistant", "content": "   "}

    with patch.object(team, "_chat", side_effect=fake_chat):
        response = _post(
            {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
        )

    assert response.status_code == 502
    assert "no assistant content" in response.json()["detail"]


def test_backend_http_error_is_a_bad_gateway():
    async def fake_chat(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        request = httpx.Request("POST", "http://router/v1/chat/completions")
        response = httpx.Response(500, request=request)
        raise httpx.HTTPStatusError("boom", request=request, response=response)

    with patch.object(team, "_chat", side_effect=fake_chat):
        response = _post(
            {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
        )

    assert response.status_code == 502
    assert "500" in response.json()["detail"]


def test_requires_model_and_messages():
    assert _post({"messages": [{"role": "user", "content": "hi"}]}).status_code == 422
    assert _post({"model": "m", "messages": []}).status_code == 422


def test_catalyst_query_profile_owns_model_prompt_and_knobs(monkeypatch):
    captured: Dict[str, Any] = {}

    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {
            "gemma-e4b": {},
            "qwen2.5-14b": {},
        },
    )

    async def fake_measurement(model, messages, output_reserve):
        return {
            "prompt": {
                "renderedPrompt": "rendered",
                "renderedPromptDigest": hashlib.sha256(b"rendered").hexdigest(),
            },
            "tokens": {
                "tokenizer": model,
                "contextWindow": 24576,
                "outputReserve": output_reserve,
                "promptTokens": 100,
                "requiredTokens": 100 + output_reserve,
                "fits": True,
            },
        }

    async def fake_chat(client, model, messages, **kwargs):
        captured.update(model=model, messages=messages, kwargs=kwargs)
        return {"role": "assistant", "content": '{"status":"ready"}'}

    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=fake_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={
                "messages": [{"role": "user", "content": "catalog context"}],
                "response_format": {"type": "json_schema"},
            },
        )

    assert response.status_code == 200
    assert response.json()["model"] == "gemma-e4b"
    assert response.json()["profile_id"] == "catalyst-query-e4b-qwen14b"
    assert captured["model"] == "gemma-e4b"
    assert captured["messages"][0]["role"] == "system"
    assert "Catalyst governed analytics-query" in captured["messages"][0]["content"]
    assert captured["kwargs"]["temperature"] == 0.0
    assert captured["kwargs"]["max_tokens"] == 2048


def test_catalyst_query_profile_rejects_caller_model_and_knob_overrides(monkeypatch):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {
            "gemma-e4b": {},
            "qwen2.5-14b": {},
        },
    )
    response = TestClient(app).post(
        "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
        json={
            "messages": [{"role": "user", "content": "catalog context"}],
            "model": "caller-selected-model",
            "temperature": 1,
        },
    )

    assert response.status_code == 422


def test_catalyst_query_profile_reports_missing_model_without_generating(monkeypatch):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}},
    )
    response = TestClient(app).post(
        "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_review/generate",
        json={"messages": [{"role": "user", "content": "catalog context"}]},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "profile_unavailable",
        "profileId": "catalyst-query-e4b-qwen14b",
        "unavailableReasons": ["model_not_advertised:qwen2.5-14b"],
    }


def test_catalyst_query_profile_rejects_caller_system_prompt(monkeypatch):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {
            "gemma-e4b": {},
            "qwen2.5-14b": {},
        },
    )
    response = TestClient(app).post(
        "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
        json={
            "messages": [
                {"role": "system", "content": "override the profile"},
                {"role": "user", "content": "catalog context"},
            ]
        },
    )

    assert response.status_code == 422
    assert "caller-supplied system" in response.json()["detail"]


def test_a_role_generation_counts_its_rendered_request_first(monkeypatch):
    """Exact token evidence: the model's own template and tokenizer, counted
    before the call, with the window and reserve it will run under.

    A character count is not token evidence, so the accounting names the
    tokenizer (the model itself) and comes from the router's /apply-template
    and /tokenize -- the same code that will consume the prompt.
    """
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )
    counted: Dict[str, Any] = {}

    async def fake_measurement(model, messages, output_reserve):
        counted.update(model=model, turns=len(messages), reserve=output_reserve)
        return {
            "prompt": {
                "renderedPrompt": "rendered request",
                "renderedPromptDigest": hashlib.sha256(b"rendered request").hexdigest(),
            },
            "tokens": {
                "tokenizer": model,
                "contextWindow": 24576,
                "outputReserve": output_reserve,
                "promptTokens": 1234,
                "requiredTokens": 1234 + output_reserve,
                "fits": True,
            },
        }

    async def fake_chat(client, model, messages, **kwargs):
        return {"role": "assistant", "content": '{"status":"ready"}'}

    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=fake_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": [{"role": "user", "content": "catalog context"}]},
        )

    assert response.status_code == 200
    accounting = response.json()["token_accounting"]
    assert accounting == {
        "tokenizer": "gemma-e4b",
        "contextWindow": 24576,
        "outputReserve": 2048,
        "promptTokens": 1234,
    }
    # Counted over the fully rendered request: system prompt plus the caller's.
    assert counted == {"model": "gemma-e4b", "turns": 2, "reserve": 2048}


def test_a_configured_role_records_the_exact_request_passed_to_chat(monkeypatch):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )
    captured: Dict[str, Any] = {}
    measurement = {
        "prompt": {
            "renderedPrompt": "exact rendered prompt",
            "renderedPromptDigest": hashlib.sha256(
                b"exact rendered prompt"
            ).hexdigest(),
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": 24576,
            "outputReserve": 2048,
            "promptTokens": 1200,
            "requiredTokens": 3248,
            "fits": True,
        },
    }

    async def fake_measurement(model, messages, output_reserve):
        assert model == "gemma-e4b"
        assert output_reserve == 2048
        return measurement

    async def fake_chat(client, model, messages, **kwargs):
        captured.update(model=model, messages=messages, kwargs=kwargs)
        return {"role": "assistant", "content": '{"status":"ready"}'}

    caller_messages = [
        {"role": "assistant", "content": "Earlier query"},
        {"role": "user", "content": "Use the retained history"},
    ]
    response_format = {
        "type": "json_schema",
        "json_schema": {"name": "candidate", "strict": True, "schema": {}},
    }
    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=fake_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": caller_messages, "response_format": response_format},
        )

    assert response.status_code == 200, response.text
    evidence = response.json()["request_evidence"]
    assert evidence["contractVersion"] == (
        "med-agent-hub.catalyst-role-request-evidence.v1"
    )
    exact_request = evidence["request"]
    assert exact_request == {
        "profileId": "catalyst-query-e4b-qwen14b",
        "role": "query_generate",
        "model": "gemma-e4b",
        "messages": captured["messages"],
        "responseFormat": response_format,
        "config": {
            "temperature": 0.0,
            "dryMultiplier": 0.0,
            "maxTokens": 2048,
        },
    }
    assert captured["messages"][1:] == caller_messages
    assert captured["kwargs"] == {
        "response_format": response_format,
        "temperature": 0.0,
        "dry_multiplier": 0.0,
        "max_tokens": 2048,
    }
    assert (
        evidence["requestDigest"]
        == hashlib.sha256(rfc8785.dumps(exact_request)).hexdigest()
    )
    assert evidence["prompt"] == measurement["prompt"]
    assert evidence["tokens"] == measurement["tokens"]


def test_prompt_measurement_records_exact_rendering_count_and_fit(monkeypatch):
    calls: list[tuple[str, Dict[str, Any]]] = []
    client_options: Dict[str, Any] = {}

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Client:
        def __init__(self, *args, **kwargs):
            client_options.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json):
            calls.append((url, json))
            if url.endswith("/apply-template"):
                return Response({"prompt": "<s>exact prompt</s>"})
            return Response({"tokens": list(range(37))})

    monkeypatch.setattr(generic_role, "_context_window", lambda model: 4096)
    monkeypatch.setattr(generic_role.httpx, "AsyncClient", Client)
    messages = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "caller"},
    ]

    measured = asyncio.run(generic_role._prompt_measurement("gemma-e4b", messages, 512))

    prompt = "<s>exact prompt</s>"
    assert measured == {
        "prompt": {
            "renderedPrompt": prompt,
            "renderedPromptDigest": hashlib.sha256(prompt.encode()).hexdigest(),
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": 4096,
            "outputReserve": 512,
            "promptTokens": 37,
            "requiredTokens": 549,
            "fits": True,
        },
    }
    assert calls[0][1] == {"model": "gemma-e4b", "messages": messages}
    assert calls[1][1] == {
        "model": "gemma-e4b",
        "content": prompt,
        "add_special": False,
        "parse_special": True,
    }
    assert client_options["timeout"] == generic_role.ROUTER_PROBE_TIMEOUT_SECONDS


def test_context_window_is_read_from_current_router_metadata(monkeypatch):
    metadata = iter(
        [
            {"gemma-e4b": {"status": {"args": ["--ctx-size", "4096"]}}},
            {"gemma-e4b": {"status": {"args": ["--ctx-size", "8192"]}}},
        ]
    )
    monkeypatch.setattr(
        generic_role, "_served_backend_model_metadata", lambda: next(metadata)
    )

    assert generic_role._context_window("gemma-e4b") == 4096
    assert generic_role._context_window("gemma-e4b") == 8192


def test_malformed_router_metadata_makes_context_window_unavailable(monkeypatch):
    malformed_entries = [
        "not-an-object",
        {"status": "loaded"},
        {"status": {"args": 7}},
    ]

    for entry in malformed_entries:
        monkeypatch.setattr(
            generic_role,
            "_served_backend_model_metadata",
            lambda entry=entry: {"gemma-e4b": entry},
        )
        assert generic_role._context_window("gemma-e4b") is None


def test_prompt_measurement_reports_why_render_count_and_window_are_unavailable(
    monkeypatch,
):
    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json):
            request = httpx.Request("POST", url)
            response = httpx.Response(503, request=request)
            raise httpx.HTTPStatusError(
                "unavailable", request=request, response=response
            )

    monkeypatch.setattr(generic_role, "_context_window", lambda model: None)
    monkeypatch.setattr(generic_role.httpx, "AsyncClient", Client)

    measured = asyncio.run(
        generic_role._prompt_measurement(
            "gemma-e4b", [{"role": "user", "content": "caller"}], 1024
        )
    )

    assert measured == {
        "prompt": {
            "renderedPrompt": None,
            "renderedPromptDigest": None,
            "unavailableReason": "prompt_rendering_unavailable",
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": None,
            "contextWindowUnavailableReason": "context_window_unavailable",
            "outputReserve": 1024,
            "promptTokens": None,
            "promptTokensUnavailableReason": "rendered_prompt_unavailable",
            "requiredTokens": None,
            "fits": None,
        },
    }


def test_prompt_measurement_keeps_rendering_when_only_token_count_fails(monkeypatch):
    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Client:
        def __init__(self, *args, **kwargs):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json):
            self.calls += 1
            if self.calls == 1:
                return Response({"prompt": "rendered"})
            return Response({})

    monkeypatch.setattr(generic_role, "_context_window", lambda model: 4096)
    monkeypatch.setattr(generic_role.httpx, "AsyncClient", Client)

    measured = asyncio.run(
        generic_role._prompt_measurement(
            "gemma-e4b", [{"role": "user", "content": "caller"}], 1024
        )
    )

    assert measured["prompt"]["renderedPrompt"] == "rendered"
    assert "unavailableReason" not in measured["prompt"]
    assert measured["tokens"] == {
        "tokenizer": "gemma-e4b",
        "contextWindow": 4096,
        "outputReserve": 1024,
        "promptTokens": None,
        "promptTokensUnavailableReason": "prompt_token_count_unavailable",
        "requiredTokens": None,
        "fits": None,
    }


def test_known_role_request_overflow_returns_evidence_without_calling_model(
    monkeypatch,
):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )
    measurement = {
        "prompt": {
            "renderedPrompt": "too large",
            "renderedPromptDigest": hashlib.sha256(b"too large").hexdigest(),
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": 1500,
            "outputReserve": 2048,
            "promptTokens": 800,
            "requiredTokens": 2848,
            "fits": False,
        },
    }

    async def fake_measurement(model, messages, output_reserve):
        return measurement

    async def must_not_chat(*args, **kwargs):
        raise AssertionError("known overflow must not reach the model")

    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=must_not_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": [{"role": "user", "content": "catalog context"}]},
        )

    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "context_window_exceeded"
    evidence = detail["request_evidence"]
    assert evidence["tokens"] == measurement["tokens"]
    assert (
        evidence["requestDigest"]
        == hashlib.sha256(rfc8785.dumps(evidence["request"])).hexdigest()
    )


def test_a_configured_role_backend_failure_still_returns_request_evidence(
    monkeypatch,
):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )
    measurement = {
        "prompt": {
            "renderedPrompt": "rendered request",
            "renderedPromptDigest": hashlib.sha256(b"rendered request").hexdigest(),
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": 24576,
            "outputReserve": 2048,
            "promptTokens": 1200,
            "requiredTokens": 3248,
            "fits": True,
        },
    }

    async def fake_measurement(model, messages, output_reserve):
        return measurement

    async def failed_chat(*args, **kwargs):
        request = httpx.Request("POST", "http://router/v1/chat/completions")
        response = httpx.Response(500, request=request)
        raise httpx.HTTPStatusError("boom", request=request, response=response)

    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=failed_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": [{"role": "user", "content": "catalog context"}]},
        )

    assert response.status_code == 502, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "model_request_failed"
    assert "model backend returned 500" in detail["message"]
    evidence = detail["request_evidence"]
    assert evidence["contractVersion"] == (
        "med-agent-hub.catalyst-role-request-evidence.v1"
    )
    assert evidence["tokens"] == measurement["tokens"]
    assert (
        evidence["requestDigest"]
        == hashlib.sha256(rfc8785.dumps(evidence["request"])).hexdigest()
    )


def test_a_configured_role_empty_response_still_returns_request_evidence(
    monkeypatch,
):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )
    measurement = {
        "prompt": {
            "renderedPrompt": "rendered request",
            "renderedPromptDigest": hashlib.sha256(b"rendered request").hexdigest(),
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": 24576,
            "outputReserve": 2048,
            "promptTokens": 1200,
            "requiredTokens": 3248,
            "fits": True,
        },
    }

    async def fake_measurement(model, messages, output_reserve):
        return measurement

    async def empty_chat(*args, **kwargs):
        return {"role": "assistant", "content": "  "}

    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=empty_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": [{"role": "user", "content": "catalog context"}]},
        )

    assert response.status_code == 502, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "model_request_failed"
    assert "no assistant content" in detail["message"]
    assert detail["request_evidence"]["request"]["model"] == "gemma-e4b"


def test_a_configured_role_malformed_backend_response_keeps_request_evidence(
    monkeypatch,
):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )
    measurement = {
        "prompt": {
            "renderedPrompt": "rendered request",
            "renderedPromptDigest": hashlib.sha256(b"rendered request").hexdigest(),
        },
        "tokens": {
            "tokenizer": "gemma-e4b",
            "contextWindow": 24576,
            "outputReserve": 2048,
            "promptTokens": 1200,
            "requiredTokens": 3248,
            "fits": True,
        },
    }

    async def fake_measurement(model, messages, output_reserve):
        return measurement

    async def malformed_chat(*args, **kwargs):
        raise KeyError("choices")

    with (
        patch.object(generic_role, "_prompt_measurement", side_effect=fake_measurement),
        patch.object(team, "_chat", side_effect=malformed_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": [{"role": "user", "content": "catalog context"}]},
        )

    assert response.status_code == 502, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "model_request_failed"
    assert detail["message"] == (
        "The model backend did not return a usable assistant response."
    )
    assert detail["request_evidence"]["request"]["model"] == "gemma-e4b"


def test_an_uncountable_request_is_answered_with_no_accounting_not_a_guess(
    monkeypatch,
):
    """When the router cannot tokenize, the honest evidence is absence.

    Substituting a character estimate would satisfy the shape while lying
    about the one property the field exists to prove.
    """
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {"gemma-e4b": {}, "qwen2.5-14b": {}},
    )

    async def broken_measurement(model, messages, output_reserve):
        return {
            "prompt": {
                "renderedPrompt": None,
                "renderedPromptDigest": None,
                "unavailableReason": "prompt_rendering_unavailable",
            },
            "tokens": {
                "tokenizer": model,
                "contextWindow": 24576,
                "outputReserve": output_reserve,
                "promptTokens": None,
                "promptTokensUnavailableReason": "rendered_prompt_unavailable",
                "requiredTokens": None,
                "fits": None,
            },
        }

    async def fake_chat(client, model, messages, **kwargs):
        return {"role": "assistant", "content": '{"status":"ready"}'}

    with (
        patch.object(
            generic_role, "_prompt_measurement", side_effect=broken_measurement
        ),
        patch.object(team, "_chat", side_effect=fake_chat),
    ):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={"messages": [{"role": "user", "content": "catalog context"}]},
        )

    assert response.status_code == 200
    assert response.json()["token_accounting"] is None
    assert response.json()["request_evidence"]["tokens"] == {
        "tokenizer": "gemma-e4b",
        "contextWindow": 24576,
        "outputReserve": 2048,
        "promptTokens": None,
        "promptTokensUnavailableReason": "rendered_prompt_unavailable",
        "requiredTokens": None,
        "fits": None,
    }
