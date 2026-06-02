"""Per-request team-model selection: the OpenAI `model` id selects which model runs
each role (orchestrator / synthesizer / expert), so ONE med-agent-hub serves any
advertised config per request — no reboot. Advertised presets pass chartsearchai's
exact-match served-model validation. Mocks the LM Studio boundary; exercises the
real run_team + team_config_for. Run: pytest tests/test_team_config.py
"""

import asyncio
import json

from server import team


def test_team_config_for_maps_preset_ids():
    # bare id -> no overrides (run_team falls back to llm_config defaults)
    assert team.team_config_for("med-agent-team") == {}
    # the big rung: a4b synthesizer + 27b expert (e4b orchestrator stays default)
    big = team.team_config_for("med-agent-team-a4b-27b")
    assert big.get("synthesizer_model") == "google/gemma-4-26b-a4b"
    assert big.get("expert_model") == "medgemma-27b-text-it-mlx"
    # mid rung: a4b synth, default (small) expert
    assert team.team_config_for("med-agent-team-a4b") == {"synthesizer_model": "google/gemma-4-26b-a4b"}
    # the clean Qwen synth flavors (the gemma-4-collapse fix): non-gemma-4 synthesizer
    qwen = team.team_config_for("med-agent-team-qwen")
    assert qwen.get("synthesizer_model") == "qwen3.6-35b-a3b-mlx"
    assert qwen.get("expert_model") == "medgemma-27b-text-it-mlx"
    assert team.team_config_for("med-agent-team-qwen-low") == {"synthesizer_model": "qwen3-14b-mlx"}
    # an unadvertised id never crashes -> empty overrides (defaults)
    assert team.team_config_for("med-agent-team-bogus") == {}


def _stateful_fake(calls):
    """Orchestrator calls medical_expert on its first turn, then stops; synthesis
    is the response_format turn. Lets us see which model each role uses."""
    state = {"n": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, frequency_penalty=None):
        calls.append((model, bool(tools), response_format is not None))
        state["n"] += 1
        if response_format is not None:                      # synthesis turn
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        if state["n"] == 1:                                  # 1st orchestrator turn -> call expert
            return {"content": "", "tool_calls": [
                {"id": "c1", "function": {"name": "medical_expert",
                                          "arguments": json.dumps({"query": "interpret"})}}]}
        return {"content": "", "tool_calls": None}           # later orchestrator turn -> stop
    return fake_chat


def test_run_team_routes_expert_model(monkeypatch):
    """The expert (medical_expert) call uses expert_model — the 3rd per-call role."""
    calls = []
    monkeypatch.setattr(team, "_chat", _stateful_fake(calls))
    asyncio.run(team.run_team(
        [{"role": "system", "content": "s"}, {"role": "user", "content": "chart"},
         {"role": "user", "content": "q"}],
        response_format={"type": "json_schema", "json_schema": {}},
        orchestrator_model="ORCH", synthesizer_model="SYNTH", expert_model="EXPERT",
    ))
    orch = [m for (m, t, rf) in calls if t]                    # tool turns = orchestrator
    expert = [m for (m, t, rf) in calls if not t and not rf]   # no-tools, no-rf = expert
    synth = [m for (m, t, rf) in calls if not t and rf]        # no-tools, rf = synthesis
    assert orch and all(m == "ORCH" for m in orch), calls
    assert expert == ["EXPERT"], calls
    assert synth == ["SYNTH"], calls
