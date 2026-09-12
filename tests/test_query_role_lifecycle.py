"""The real named-role route must release its router call on interruption."""

import asyncio
import json
from dataclasses import replace

import httpx
import pytest

from server import generic_role, team
from server.main import app


@pytest.mark.parametrize("queued", [False, True])
def test_named_role_disconnect_releases_work(monkeypatch, queued):
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
            # Let the model call acquire (or wait for) its actual router lock.
            await asyncio.sleep(0.01)
            return {"type": "http.disconnect"}

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
            "headers": [(b"content-type", b"application/json")],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        }
        task = asyncio.create_task(app(scope, receive, send))
        try:
            await asyncio.wait_for(reached.wait(), 1)
            done, _ = await asyncio.wait({task}, timeout=0.5)
            assert task in done, "Hub continued work after the caller stopped"
            await task
            assert messages[0]["status"] == 499
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


@pytest.mark.parametrize(
    ("endpoint", "expected_status", "content"),
    [
        ("generate", 200, "Count patients"),
        ("warm", 204, "Warm up"),
    ],
)
def test_named_query_routes_do_not_inherit_the_model_deadline(
    monkeypatch, endpoint, expected_status, content
):
    """Named routes run until completion even when the shared default is tiny."""

    async def scenario():
        started = asyncio.Event()
        complete = asyncio.Event()
        stopped = asyncio.Event()
        messages = []
        observed = {}

        async def warm_work(*_args, **kwargs):
            observed.update(kwargs)
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
                        {"messages": [{"role": "user", "content": content}]}
                    ).encode(),
                }
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)

        path = (
            "/v1/hub/query-profiles/catalyst-query-gemma-4-12b/roles/"
            f"query_generate/{endpoint}"
        )
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
            assert not task.done(), "named route inherited an automatic deadline"
            complete.set()
            await asyncio.wait_for(task, 1)
            assert messages[0]["status"] == expected_status
            assert stopped.is_set()
            assert observed["request_timeout"] is None
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
