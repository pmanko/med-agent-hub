"""Loopback HTTP cancellation proof; the model endpoint is a blocking fixture."""

import asyncio
import socket
from dataclasses import replace

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request

from server import generic_role, team
from server.main import app


@pytest.mark.parametrize("interruption", ["disconnect", "deadline"])
def test_interruption_reaches_model_over_real_http(monkeypatch, interruption):
    async def scenario():
        model_app = FastAPI()
        model_started = asyncio.Event()
        model_disconnected = asyncio.Event()

        @model_app.post("/v1/chat/completions")
        async def blocked_model(request: Request):
            await request.body()
            model_started.set()
            while (await request.receive())["type"] != "http.disconnect":
                pass
            model_disconnected.set()
            return {}

        sockets = [socket.socket(), socket.socket()]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        model_url = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"
        hub_url = f"http://127.0.0.1:{sockets[1].getsockname()[1]}"
        monkeypatch.setattr(
            team, "llm_config", replace(team.llm_config, base_url=model_url, api_key="")
        )
        monkeypatch.setattr(team, "_ROUTER_LOCK", asyncio.Lock())
        monkeypatch.setattr(
            generic_role,
            "_served_backend_model_metadata",
            lambda: {"gemma-4-12b-q4": {}},
        )

        async def measured(*args):
            return {"prompt": {}, "tokens": {"fits": True}}

        monkeypatch.setattr(generic_role, "_prompt_measurement", measured)
        servers = [
            uvicorn.Server(
                uvicorn.Config(
                    application, lifespan="off", access_log=False, log_level="error"
                )
            )
            for application in (model_app, app)
        ]
        tasks = [
            asyncio.create_task(server.serve(sockets=[sock]))
            for server, sock in zip(servers, sockets)
        ]
        request_task = None
        try:

            async def ready():
                while not all(server.started for server in servers):
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(ready(), 3)
            async with httpx.AsyncClient() as client:
                request_task = asyncio.create_task(
                    client.post(
                        f"{hub_url}/v1/hub/query-profiles/"
                        "catalyst-query-gemma-4-12b/roles/query_generate/generate",
                        json={
                            "messages": [{"role": "user", "content": "Count patients"}]
                        },
                        headers={
                            "X-Request-Timeout-Seconds": "0.5"
                            if interruption == "deadline"
                            else "5"
                        },
                    )
                )
                await asyncio.wait_for(model_started.wait(), 2)
                if interruption == "disconnect":
                    request_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await request_task
                else:
                    response = await asyncio.wait_for(request_task, 2)
                    assert response.status_code == 504
                await asyncio.wait_for(model_disconnected.wait(), 2)
                assert not team._ROUTER_LOCK.locked()
        finally:
            if request_task is not None:
                request_task.cancel()
                await asyncio.gather(request_task, return_exceptions=True)
            for server in servers:
                server.should_exit = True
            await asyncio.wait_for(asyncio.gather(*tasks), 3)
            for sock in sockets:
                sock.close()

    asyncio.run(scenario())


def test_warm_route_ignores_internal_model_request_timeout(monkeypatch):
    """Lifecycle warmup is allowed to finish after the normal model deadline."""

    async def scenario():
        model_app = FastAPI()

        @model_app.post("/v1/chat/completions")
        async def delayed_model():
            await asyncio.sleep(0.05)
            return {"choices": [{"message": {"role": "assistant", "content": "ready"}}]}

        sockets = [socket.socket(), socket.socket()]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        model_url = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"
        hub_url = f"http://127.0.0.1:{sockets[1].getsockname()[1]}"
        monkeypatch.setattr(
            team,
            "llm_config",
            replace(
                team.llm_config,
                base_url=model_url,
                api_key="",
                request_timeout_seconds=0.01,
            ),
        )
        monkeypatch.setattr(team, "_ROUTER_LOCK", asyncio.Lock())
        monkeypatch.setattr(
            generic_role,
            "_served_backend_model_metadata",
            lambda: {"gemma-4-12b-q4": {}},
        )

        async def measured(*args):
            return {"prompt": {}, "tokens": {"fits": True}}

        monkeypatch.setattr(generic_role, "_prompt_measurement", measured)
        servers = [
            uvicorn.Server(
                uvicorn.Config(
                    application, lifespan="off", access_log=False, log_level="error"
                )
            )
            for application in (model_app, app)
        ]
        tasks = [
            asyncio.create_task(server.serve(sockets=[sock]))
            for server, sock in zip(servers, sockets)
        ]
        try:

            async def ready():
                while not all(server.started for server in servers):
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(ready(), 3)
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{hub_url}/v1/hub/query-profiles/"
                    "catalyst-query-gemma-4-12b/roles/query_generate/warm",
                    json={"messages": [{"role": "user", "content": "Warm up"}]},
                )
            assert response.status_code == 204
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.wait_for(asyncio.gather(*tasks), 3)
            for sock in sockets:
                sock.close()

    asyncio.run(scenario())
