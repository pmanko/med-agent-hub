"""Per-lane knobs: a level can set sampling knobs (temperature / repeat_penalty / dry)
PER ROLE, so any arm is fully reproducible from its level block. Unset roles fall back
to today's global defaults. Mocks the LM Studio boundary (`_chat`) and asserts each
role's call carries its configured knobs. Run: pytest tests/test_knobs.py
"""

import asyncio
import json

from server import team

_MESSAGES = [
    {"role": "system", "content": "s"},
    {"role": "user", "content": "chart"},
    {"role": "user", "content": "q"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def test_per_lane_knobs_route_to_each_role(monkeypatch):
    """Per-role knobs reach the matching role's _chat call (synthesizer + validator +
    orchestrator). RED on today's code: run_team has no `knobs` parameter."""
    seen = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        seen.append({"model": model, "temperature": temperature,
                     "repeat_penalty": repeat_penalty, "dry": dry_multiplier})
        if model == "VAL":
            return {"content": json.dumps({"answer_ok": False, "answer_issues": "x", "indepth_drop": [], "indepth_issues": ""})}
        if response_format is not None:
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)
    knobs = {
        "synthesizer": {"temperature": 0.7, "repeat_penalty": 1.3, "dry": 0.5},
        "validator": {"temperature": 0.9},
        "orchestrator": {"temperature": 0.15, "dry": 0.0},
    }
    asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VAL", validator_max_loops=1, knobs=knobs))

    synth = [c for c in seen if c["model"] == "SYNTH"]
    val = [c for c in seen if c["model"] == "VAL"]
    orch = [c for c in seen if c["model"] == "ORCH"]
    # synthesizer (original + the validator-triggered re-synth) both carry the lane's synth knobs
    assert synth, seen
    assert all(c["temperature"] == 0.7 and c["repeat_penalty"] == 1.3 and c["dry"] == 0.5
               for c in synth), seen
    assert val and val[0]["temperature"] == 0.9, seen
    assert orch and orch[0]["temperature"] == 0.15 and orch[0]["dry"] == 0.0, seen


def test_unset_role_knobs_keep_defaults(monkeypatch):
    """With no knobs block, the synthesizer keeps its anti-degeneration defaults
    (temperature floor + repeat_penalty) — nothing changes vs today."""
    seen = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        seen.append({"model": model, "temperature": temperature,
                     "repeat_penalty": repeat_penalty, "dry": dry_multiplier})
        if response_format is not None:
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)
    asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, temperature=0.0,
        orchestrator_model="ORCH", synthesizer_model="SYNTH"))
    synth = [c for c in seen if c["model"] == "SYNTH"][0]
    assert synth["temperature"] >= team._SYNTH_MIN_TEMPERATURE, synth
    assert synth["repeat_penalty"] == team.SYNTH_REPEAT_PENALTY, synth
    assert synth["dry"] == team.SYNTH_DRY_MULTIPLIER, synth
