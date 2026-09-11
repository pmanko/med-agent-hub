"""The real named-role route must release its router call on interruption."""

import asyncio
import json

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
