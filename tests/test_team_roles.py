"""Unit test for the team role split: the orchestrator tool loop and the final
synthesis use SEPARATE models — a fast small model runs the chatty loop while a
larger model composes the answer. Mocks the LM Studio boundary (`_chat`) and
asserts which model each call targets, exercising the real `run_team` routing
(not a reimplementation). Run: `pytest tests/test_team_roles.py`.
"""

import asyncio
import json

from server import team


def _fake_chat_factory(calls):
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None):
        calls.append((model, response_format is not None))
        if response_format is not None:  # synthesis turn (schema-bound)
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # loop turn: no tool calls -> break to synthesis
    return fake_chat


_MESSAGES = [
    {"role": "system", "content": "envelope system"},
    {"role": "user", "content": "patient chart"},
    {"role": "user", "content": "the question"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def test_synthesis_uses_synthesizer_model_loop_uses_orchestrator(monkeypatch):
    """Explicit per-call models: the loop runs on the orchestrator, synthesis on
    the synthesizer. RED on today's code (run_team has no synthesizer_model param)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _fake_chat_factory(calls))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH-MODEL", synthesizer_model="SYNTH-MODEL",
    ))
    loop_models = [m for (m, is_synth) in calls if not is_synth]
    synth_models = [m for (m, is_synth) in calls if is_synth]
    assert loop_models and all(m == "ORCH-MODEL" for m in loop_models), calls
    assert synth_models == ["SYNTH-MODEL"], calls
    assert json.loads(out)["answer"] == "ok"
