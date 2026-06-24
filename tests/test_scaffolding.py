"""P1: scaffolding (solo vs team) is DECOUPLED from context.

A `solo` arm runs ONE model (the writer) over the deterministic context (chart + temporal) — NO
orchestrator, no team. A `team` arm (solo=False) runs the orchestrator tool loop, as before. The
temporal context reaches BOTH (context is independent of scaffolding). Mocks the `_chat` boundary.
Run: pytest tests/test_scaffolding.py
"""
import asyncio
import json

from server import team


def _recording_chat(calls):
    async def fake_chat(client, model, messages, *, tools=None, response_format=None, **kwargs):
        calls.append({"model": model, "tools": bool(tools), "rf": response_format is not None,
                      "messages": messages})
        if response_format is not None:  # a synthesis turn
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # orchestrator turn: nothing to gather -> stop
    return fake_chat


_MSGS = [
    {"role": "system", "content": "s"},
    {"role": "user", "content": "[1] (2026-01-07) Finding — Weight (kg): 41.0 kg"},
    {"role": "user", "content": "When was the patient's last visit?"},
]
_RF = {"type": "json_schema", "json_schema": {}}


def test_solo_runs_one_model_no_orchestrator(monkeypatch):
    # solo scaffolding: the writer answers directly; NO orchestrator tool-loop turn at all.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    asyncio.run(team.run_team(_MSGS, response_format=_RF,
                              orchestrator_model="ORCH", synthesizer_model="SYNTH",
                              has_expert=False, two_call=False, solo=True))
    assert not any(c["tools"] for c in calls), calls            # no orchestrator/tool turns
    assert not any(c["model"] == "ORCH" for c in calls), calls  # orchestrator model never called
    assert any(c["model"] == "SYNTH" and c["rf"] for c in calls), calls  # the writer synthesized


def test_team_runs_orchestrator(monkeypatch):
    # team scaffolding (solo=False, the contrast): the orchestrator tool loop DOES run.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    asyncio.run(team.run_team(_MSGS, response_format=_RF,
                              orchestrator_model="ORCH", synthesizer_model="SYNTH",
                              has_expert=False, two_call=False, solo=False))
    assert any(c["model"] == "ORCH" and c["tools"] for c in calls), calls


def test_solo_still_gets_temporal_context(monkeypatch):
    # Context is independent of scaffolding: the deterministic temporal block reaches the solo writer.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    asyncio.run(team.run_team(_MSGS, response_format=_RF,
                              orchestrator_model="ORCH", synthesizer_model="SYNTH",
                              has_expert=False, two_call=False, solo=True))
    synth = next(c for c in calls if c["rf"])
    blob = json.dumps(synth["messages"])
    assert "2026-01-07" in blob and ("Most recent clinical visit" in blob or "Current date" in blob), \
        "the solo writer must receive the temporal context"
