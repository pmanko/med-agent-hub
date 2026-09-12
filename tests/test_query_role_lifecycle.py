"""The real named-role route must release its router call on interruption."""

import asyncio
import json
from dataclasses import replace

import httpx
import pytest
from fastapi.testclient import TestClient

from server import generic_role, team
from server.main import app


@pytest.mark.parametrize("queued", [False, True])
@pytest.mark.parametrize("interruption", ["disconnect", "deadline"])
def test_named_role_interruption_releases_work(monkeypatch, queued, interruption):
    async def scenario():
        monkeypatch.setattr(
            generic_role,
            "_served_backend_model_metadata",
            lambda: {
                "gemma-4-12b-q4": {},
            },
        )
        lock = asyncio.Lock()
        monkeypatch.setattr(team, "_ROUTER_LOCK", lock)
        reached = asyncio.Event()
        stopped = asyncio.Event()
        calls = []

        async def measured(*args):
            reached.set()
            return {"prompt": {}, "tokens": {"fits": True}}

        async def blocked_post(self, url, **kwargs):
            calls.append(url)
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        monkeypatch.setattr(generic_role, "_prompt_measurement", measured)
        monkeypatch.setattr(httpx.AsyncClient, "post", blocked_post)
        if queued:
            await lock.acquire()
        body_sent = False
        messages = []

        async def receive():
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {
                    "type": "http.request",
                    "body": json.dumps(
                        {
                            "messages": [{"role": "user", "content": "Count patients"}],
                        }
                    ).encode(),
                }
            await reached.wait()
            if interruption == "disconnect":
                # Let the model call acquire (or wait for) its actual router lock.
                await asyncio.sleep(0.01)
                return {"type": "http.disconnect"}
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)

        path = "/v1/hub/query-profiles/catalyst-query-gemma-4-12b/roles/query_generate/generate"
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [
                (b"content-type", b"application/json"),
                (
                    b"x-request-timeout-seconds",
                    b"0.05" if interruption == "deadline" else b"5",
                ),
            ],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        }
        task = asyncio.create_task(app(scope, receive, send))
        try:
            await asyncio.wait_for(reached.wait(), 1)
            done, _ = await asyncio.wait({task}, timeout=0.5)
            assert task in done, "Hub continued work after the caller stopped"
            await task
            assert messages[0]["status"] == (504 if interruption == "deadline" else 499)
            if queued:
                assert calls == []
                assert lock.locked(), "Cancellation released another request's lock"
                lock.release()
            else:
                assert len(calls) == 1
                assert stopped.is_set()
                assert not lock.locked()
            await asyncio.wait_for(lock.acquire(), 0.1)
            lock.release()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if lock.locked():
                lock.release()

    asyncio.run(scenario())


@pytest.mark.parametrize("budget", ["0", "-1", "nan", "inf", "invalid"])
def test_role_rejects_invalid_deadline_before_dispatch(monkeypatch, budget):
    def unexpected_discovery():
        pytest.fail("Invalid timeout reached profile/model discovery")

    monkeypatch.setattr(
        generic_role, "_served_backend_model_metadata", unexpected_discovery
    )
    response = TestClient(app).post(
        "/v1/hub/query-profiles/catalyst-query-gemma-4-12b/roles/query_generate/generate",
        json={"messages": [{"role": "user", "content": "Count patients"}]},
        headers={"X-Request-Timeout-Seconds": budget},
    )
    assert response.status_code == 422


def test_warm_route_does_not_inherit_the_interactive_deadline(monkeypatch):
    """Lifecycle warmup completes after the interactive request budget expires."""

    async def scenario():
        started = asyncio.Event()
        complete = asyncio.Event()
        stopped = asyncio.Event()
        messages = []

        async def warm_work(*_args):
            started.set()
            try:
                await complete.wait()
                return generic_role.ProfileGenerateResponse(
                    model="gemma-4-12b-q4",
                    content="ignored warmup output",
                    profile_id="catalyst-query-gemma-4-12b",
                    role="query_generate",
                    request_evidence={},
                )
            finally:
                stopped.set()

        monkeypatch.setattr(generic_role, "_generate_query_role", warm_work)
        monkeypatch.setattr(
            generic_role,
            "llm_config",
            replace(generic_role.llm_config, request_timeout_seconds=0.01),
        )
        body_sent = False

        async def receive():
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {
                    "type": "http.request",
                    "body": json.dumps(
                        {"messages": [{"role": "user", "content": "Warm up"}]}
                    ).encode(),
                }
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)

        path = "/v1/hub/query-profiles/catalyst-query-gemma-4-12b/roles/query_generate/warm"
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        }
        task = asyncio.create_task(app(scope, receive, send))
        try:
            await asyncio.wait_for(started.wait(), 1)
            await asyncio.sleep(0.05)
            assert not task.done(), "warmup inherited an interactive deadline"
            complete.set()
            await asyncio.wait_for(task, 1)
            assert messages[0]["status"] == 204
            assert stopped.is_set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
