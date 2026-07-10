from __future__ import annotations

import asyncio

from server import team
from server.levels_loader import get_profile


def test_blocking_adapter_drains_the_same_async_stage_engine(monkeypatch):
    request = team.ExecutionRequest(
        profile=get_profile("answer:gemma-4-12b"),
        messages=[{"role": "user", "content": "Question"}],
    )
    calls = []

    async def fake_engine(actual_request):
        calls.append(actual_request)
        yield "result", '{"answer":"ok","citations":[],"blocks":[]}'

    monkeypatch.setattr(team, "execute_profile", fake_engine)

    assert asyncio.run(team.drain_profile(request)) == (
        '{"answer":"ok","citations":[],"blocks":[]}'
    )
    assert calls == [request]


def test_duplicate_legacy_execution_entrypoints_are_removed():
    assert not hasattr(team, "run_team")
    assert not hasattr(team, "run_team_stream")
    assert not hasattr(team, "run_team_stage_drain")
