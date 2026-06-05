"""Team levels config: the OpenAI `model` id selects a level (server/levels.yaml),
which fixes the per-role models (orchestrator / synthesizer / expert) + prompts, so
ONE med-agent-hub serves any tier per request — no reboot. `expert: null` drops the
medical_expert tool + role. Mocks the LM Studio boundary; exercises the real
levels_loader + run_team. Run: pytest tests/test_team_config.py
"""

import asyncio
import json

import pytest

from server import team, levels_loader


def test_level_ids_advertises_the_configured_levels():
    # med-agent-hub publishes the keys in levels.yaml: the core tiers are always
    # advertised, with no duplicates (extra experiment lanes may be added over time).
    ids = levels_loader.level_ids()
    assert len(ids) == len(set(ids))  # no duplicate ids
    for tier in ("med-agent-team-low", "med-agent-team-med", "med-agent-team-high"):
        assert tier in ids, ids


def test_get_level_resolves_models_and_prompts():
    low = levels_loader.get_level("med-agent-team-low")
    assert low.orchestrator and low.synthesizer        # required roles present
    # synthesis_prompt is the BASE name; two-call resolves <base>-answer / <base>-indepth.
    assert low.synthesis_prompt == "synthesis"         # tier-agnostic two-call prompts (default base)
    high = levels_loader.get_level("med-agent-team-high")
    assert high.expert and high.expert != low.expert   # high steps up to a bigger expert
    assert high.synthesis_prompt == "synthesis"        # default prompt name


def test_unknown_level_fails_loud():
    with pytest.raises(KeyError):
        levels_loader.get_level("med-agent-team-bogus")


def test_expert_toggle_drops_the_medical_expert_tool():
    # The whole point of the toggle: a level with no expert is offered no
    # medical_expert tool. Red against the old hardcoded two-tool list.
    names_with = [t["function"]["name"] for t in team._tool_definitions(has_expert=True)]
    names_without = [t["function"]["name"] for t in team._tool_definitions(has_expert=False)]
    assert "kb_search" in names_with and "medical_expert" in names_with
    assert "kb_search" in names_without and "medical_expert" not in names_without


def test_level_with_null_expert_reports_no_expert():
    # A Level with expert unset reports has_expert False (drives the tool toggle).
    assert levels_loader.Level(id="x", orchestrator="o", synthesizer="s", expert=None).has_expert is False
    assert levels_loader.Level(id="y", orchestrator="o", synthesizer="s", expert="medgemma").has_expert is True


def _stateful_fake(calls):
    """Orchestrator calls medical_expert on its first turn, then stops; synthesis
    is the response_format turn. Lets us see which model each role uses."""
    state = {"n": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
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
    # Two-call synthesis (Answer + In-Depth) — both on the synthesizer model.
    assert synth and all(m == "SYNTH" for m in synth), calls
