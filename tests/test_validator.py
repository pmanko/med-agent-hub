"""Validator role: an optional post-synthesis audit round. When a level sets a
`validator` model, run_team audits the synthesized envelope (the main answer) AND
the gathered context separately; on a flag it appends specific feedback and
re-synthesizes, up to `validator_max_loops` cycles. `validator=None` skips it
entirely (today's behaviour). Mocks the LM Studio boundary (`_chat`) and asserts
the routing + loop-back, exercising the real run_team. Run: pytest tests/test_validator.py
"""

import asyncio
import json

from server import team

_MESSAGES = [
    {"role": "system", "content": "envelope system"},
    {"role": "user", "content": "patient chart"},
    {"role": "user", "content": "the question"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def _factory(calls, validator_ok_seq):
    """Mock _chat. Orchestrator loop -> no tools (break to synth). Synth -> a versioned
    envelope so re-synth is distinguishable. Validator (model='VALIDATOR') -> a verdict
    pulled from validator_ok_seq."""
    state = {"synth": 0, "val": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))
        if model == "VALIDATOR":
            ok = validator_ok_seq[min(state["val"], len(validator_ok_seq) - 1)]
            state["val"] += 1
            return {"content": json.dumps(
                {"ok": ok,
                 "answer_issues": "" if ok else "trend asserted from a single data point",
                 "context_issues": ""})}
        if response_format is not None:  # synthesis (schema-bound)
            v = state["synth"]
            state["synth"] += 1
            return {"content": json.dumps({"answer": f"answer-v{v}", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # loop turn

    return fake_chat


def _counts(calls):
    val = [c for c in calls if c[0] == "VALIDATOR"]
    synth = [c for c in calls if c[0] == "SYNTH"]
    return len(val), len(synth)


def test_validator_none_skips_audit(monkeypatch):
    """No validator configured -> no audit call, single synthesis (today's behaviour)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [True]))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH"))
    val, synth = _counts(calls)
    assert val == 0 and synth == 1, calls
    assert json.loads(out)["answer"] == "answer-v0"


def test_validator_clean_answer_no_resynth(monkeypatch):
    """Validator passes the draft -> audited once, NOT re-synthesized."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [True]))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VALIDATOR"))
    val, synth = _counts(calls)
    assert val == 1 and synth == 1, calls
    assert json.loads(out)["answer"] == "answer-v0"


def test_validator_flag_loops_back_then_passes(monkeypatch):
    """Flag -> re-synthesize -> re-audit -> pass. Two synth calls, two audits; the
    returned answer is the corrected re-synthesis (v1)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [False, True]))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VALIDATOR", validator_max_loops=2))
    val, synth = _counts(calls)
    assert synth == 2 and val == 2, calls
    assert json.loads(out)["answer"] == "answer-v1"


def test_validator_respects_max_loops(monkeypatch):
    """Persistent flag with max_loops=1 -> exactly one audit + one re-synth, then stop
    (return the best effort rather than loop forever)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [False, False, False]))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VALIDATOR", validator_max_loops=1))
    val, synth = _counts(calls)
    assert val == 1 and synth == 2, calls
    assert json.loads(out)["answer"] == "answer-v1"
