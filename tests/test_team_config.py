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
    from server import config as cfg
    # med-agent-hub publishes EXACTLY three levels: low / med / high.
    assert set(team.TEAM_PRESETS) == {"med-agent-team-low", "med-agent-team-med", "med-agent-team-high"}
    # high: biggest synth + big expert (orchestrator stays the default e4b).
    high = team.team_config_for("med-agent-team-high")
    assert high["synthesizer_model"] == cfg.SYNTH_MODEL_HIGH
    assert high["expert_model"] == cfg.EXPERT_MODEL_HIGH
    # med: distinct mid-size synth + big expert.
    med = team.team_config_for("med-agent-team-med")
    assert med["synthesizer_model"] == cfg.SYNTH_MODEL_MED
    assert med["expert_model"] == cfg.EXPERT_MODEL_MED
    # low: small synth + small expert + an optimized synthesis prompt for that model class.
    low = team.team_config_for("med-agent-team-low")
    assert low["synthesizer_model"] == cfg.SYNTH_MODEL_LOW
    assert low["expert_model"] == cfg.EXPERT_MODEL_LOW
    assert low["synthesizer_prompt"] == "synthesis-low"
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
