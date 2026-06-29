"""Per-lane knobs: a level can set sampling knobs (temperature / repeat_penalty / dry)
PER ROLE, so any arm is fully reproducible from its level block. Unset roles fall back
to today's global defaults. Mocks the LM Studio boundary (`_chat`) and asserts each
role's call carries its configured knobs — including the two-call generation flow
(Answer synth + In-Depth synth) and the two validators (answer_verdict + indepth_verdict).
Run: pytest tests/test_knobs.py
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


def _make_chat(seen):
    """A _chat mock that records each call's knobs and branches on the response_format
    json_schema name so the synth calls (chart_answer + in_depth) and the validator calls
    (answer_verdict + indepth_verdict) are all served — like production's four call sites."""
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        seen.append({"model": model, "schema": name, "temperature": temperature,
                     "repeat_penalty": repeat_penalty, "dry": dry_multiplier})
        if name == "chart_answer":
            return {"content": json.dumps({"answer": "ans", "citations": [], "blocks": []})}
        if name == "in_depth":
            return {"content": json.dumps({"claims": ["c1", "c2"]})}
        if name == "answer_verdict":
            return {"content": json.dumps({"answer_ok": True, "answer_issues": ""})}
        if name == "indepth_verdict":
            return {"content": json.dumps({"drop": []})}
        return {"content": "", "tool_calls": None}
    return fake_chat


def test_per_lane_knobs_route_to_each_role(monkeypatch):
    """Per-role knobs reach the matching role's _chat calls: BOTH synthesis calls (Answer +
    In-Depth) carry the synth knobs, BOTH validator calls carry the validator knobs, and the
    orchestrator loop carries the orchestrator knobs."""
    seen = []
    monkeypatch.setattr(team, "_chat", _make_chat(seen))
    knobs = {
        "synthesizer": {"temperature": 0.7, "repeat_penalty": 1.3, "dry": 0.5},
        "validator": {"temperature": 0.9, "repeat_penalty": 1.1, "dry": 0.2},
        "orchestrator": {"temperature": 0.15, "dry": 0.0},
    }
    asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VAL", validator_max_loops=1, knobs=knobs))

    synth = [c for c in seen if c["schema"] in ("chart_answer", "in_depth")]
    val = [c for c in seen if c["schema"] in ("rewrite_verdict", "indepth_verdict")]
    orch = [c for c in seen if c["schema"] is None]

    # Both the Answer synth (chart_answer) and the In-Depth synth (in_depth) fire and carry the knobs.
    assert {c["schema"] for c in synth} == {"chart_answer", "in_depth"}, seen
    assert all(c["model"] == "SYNTH" for c in synth), seen
    assert all(c["temperature"] == 0.7 and c["repeat_penalty"] == 1.3 and c["dry"] == 0.5
               for c in synth), seen

    # Both validators (answer rewrite + in-depth) fire and carry the validator knobs.
    assert {c["schema"] for c in val} == {"rewrite_verdict", "indepth_verdict"}, seen
    assert all(c["model"] == "VAL" for c in val), seen
    assert all(c["temperature"] == 0.9 and c["repeat_penalty"] == 1.1 and c["dry"] == 0.2
               for c in val), seen

    assert orch and all(c["model"] == "ORCH" for c in orch), seen
    assert orch[0]["temperature"] == 0.15 and orch[0]["dry"] == 0.0, seen


def test_unset_role_knobs_keep_defaults(monkeypatch):
    """With no knobs block, the synthesizer keeps its anti-degeneration defaults
    (temperature floor + repeat_penalty + dry) on BOTH synth calls — nothing changes vs today."""
    seen = []
    monkeypatch.setattr(team, "_chat", _make_chat(seen))
    asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, temperature=0.0,
        orchestrator_model="ORCH", synthesizer_model="SYNTH"))
    synth = [c for c in seen if c["schema"] in ("chart_answer", "in_depth")]
    assert {c["schema"] for c in synth} == {"chart_answer", "in_depth"}, seen
    for c in synth:
        assert c["temperature"] >= team._SYNTH_MIN_TEMPERATURE, c
        assert c["repeat_penalty"] == team.SYNTH_REPEAT_PENALTY, c
        assert c["dry"] == team.SYNTH_DRY_MULTIPLIER, c
