"""Tests for the generic structured single-shot role-executor endpoint.

The endpoint is a thin wrapper over ``team._chat`` (the shared, router-serialized
backend primitive); these tests patch ``_chat`` so no real model is required.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from server import team
from server import generic_role
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
            "google/gemma-4-e4b": {},
            "qwen2.5-14b-instruct-mlx": {},
        },
    )

    async def fake_chat(client, model, messages, **kwargs):
        captured.update(model=model, messages=messages, kwargs=kwargs)
        return {"role": "assistant", "content": '{"status":"ready"}'}

    with patch.object(team, "_chat", side_effect=fake_chat):
        response = TestClient(app).post(
            "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_generate/generate",
            json={
                "messages": [{"role": "user", "content": "catalog context"}],
                "response_format": {"type": "json_schema"},
            },
        )

    assert response.status_code == 200
    assert response.json()["model"] == "google/gemma-4-e4b"
    assert response.json()["profile_id"] == "catalyst-query-e4b-qwen14b"
    assert captured["model"] == "google/gemma-4-e4b"
    assert captured["messages"][0]["role"] == "system"
    assert "Catalyst governed analytics-query" in captured["messages"][0]["content"]
    assert captured["kwargs"]["temperature"] == 0.0
    assert captured["kwargs"]["max_tokens"] == 1024


def test_catalyst_query_profile_rejects_caller_model_and_knob_overrides(monkeypatch):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {
            "google/gemma-4-e4b": {},
            "qwen2.5-14b-instruct-mlx": {},
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
        lambda: {"google/gemma-4-e4b": {}},
    )
    response = TestClient(app).post(
        "/v1/hub/query-profiles/catalyst-query-e4b-qwen14b/roles/query_review/generate",
        json={"messages": [{"role": "user", "content": "catalog context"}]},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "profile_unavailable",
        "profileId": "catalyst-query-e4b-qwen14b",
        "unavailableReasons": ["model_not_advertised:qwen2.5-14b-instruct-mlx"],
    }


def test_catalyst_query_profile_rejects_caller_system_prompt(monkeypatch):
    monkeypatch.setattr(
        generic_role,
        "_served_backend_model_metadata",
        lambda: {
            "google/gemma-4-e4b": {},
            "qwen2.5-14b-instruct-mlx": {},
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
