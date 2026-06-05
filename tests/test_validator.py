"""Validator role: an optional post-synthesis audit that judges the **Answer** and
**In Depth** sections SEPARATELY and acts granularly — ship a clean draft; if only the
In Depth is flagged keep the Answer and drop the In Depth; if the Answer is flagged
re-synthesize, adopt the fix or (if still wrong) abstain. Never ships a wrong direct
answer. Mocks the LM Studio boundary (`_chat`) and exercises the real run_team.
Run: pytest tests/test_validator.py
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


def _factory(calls, verdicts):
    """Mock _chat. Orchestrator loop -> no tools (break to synth). Synth -> a versioned
    two-section envelope. Validator (schema 'validator_verdict') -> a per-section verdict
    {answer_ok, indepth_ok} pulled from `verdicts`."""
    state = {"synth": 0, "val": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))
        if name == "validator_verdict":
            v = verdicts[min(state["val"], len(verdicts) - 1)]
            state["val"] += 1
            return {"content": json.dumps({
                "answer_ok": v["answer_ok"], "answer_issues": "" if v["answer_ok"] else "wrong value",
                "indepth_ok": v["indepth_ok"], "indepth_issues": "" if v["indepth_ok"] else "fabricated trend"})}
        if response_format is not None:  # synthesis (chart_answer schema)
            n = state["synth"]
            state["synth"] += 1
            return {"content": json.dumps(
                {"answer": f"**Answer** ans-v{n} **In Depth** depth-v{n}", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # loop turn

    return fake_chat


def _counts(calls):
    val = sum(1 for m, n in calls if n == "validator_verdict")
    synth = sum(1 for m, n in calls if n == "chart_answer")
    return val, synth


def _run(verdicts, **kw):
    calls = []
    import server.team as t
    t._chat = _factory(calls, verdicts)
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH",
        synthesizer_model="SYNTH", **kw))
    return calls, json.loads(out)["answer"]


def test_validator_none_skips_audit(monkeypatch):
    monkeypatch.setattr(team, "_chat", _factory([], []))
    out = asyncio.run(team.run_team(_MESSAGES, response_format=_RF,
                                    orchestrator_model="ORCH", synthesizer_model="SYNTH"))
    assert "ans-v0" in json.loads(out)["answer"]


def test_validator_both_sections_clean_ships(monkeypatch):
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [{"answer_ok": True, "indepth_ok": True}]))
    out = asyncio.run(team.run_team(_MESSAGES, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", validator_model="VALIDATOR"))
    val, synth = _counts(calls)
    assert val == 1 and synth == 1, calls
    a = json.loads(out)["answer"]
    assert "ans-v0" in a and "depth-v0" in a, a  # full two-section answer shipped


def test_validator_indepth_flagged_keeps_answer_drops_indepth(monkeypatch):
    """GRANULAR: Answer ok, In Depth flagged -> ship the Answer, drop the In Depth. RED on
    the old all-or-nothing code (which abstained the whole turn)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [{"answer_ok": True, "indepth_ok": False}]))
    out = asyncio.run(team.run_team(_MESSAGES, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", validator_model="VALIDATOR"))
    val, synth = _counts(calls)
    assert val == 1 and synth == 1, calls          # audited once, NOT re-synthesized
    a = json.loads(out)["answer"]
    assert "ans-v0" in a, a                         # the grounded Answer is kept
    assert "depth-v0" not in a, a                   # the flagged In Depth is dropped
    assert "could not produce" not in a.lower(), a  # NOT a full abstain


def test_validator_answer_flagged_resynth_fixes(monkeypatch):
    """Answer flagged -> re-synthesize -> Answer now ok -> adopt the revision."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(
        calls, [{"answer_ok": False, "indepth_ok": False}, {"answer_ok": True, "indepth_ok": True}]))
    out = asyncio.run(team.run_team(_MESSAGES, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", validator_model="VALIDATOR", validator_max_loops=1))
    val, synth = _counts(calls)
    assert val == 2 and synth == 2, calls
    assert "ans-v1" in json.loads(out)["answer"]


def test_validator_answer_flagged_persists_abstains(monkeypatch):
    """Answer flagged + re-synth still flagged -> abstain (never ship a wrong direct answer)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(
        calls, [{"answer_ok": False, "indepth_ok": False}, {"answer_ok": False, "indepth_ok": False}]))
    out = asyncio.run(team.run_team(_MESSAGES, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", validator_model="VALIDATOR", validator_max_loops=1))
    val, synth = _counts(calls)
    assert val == 2 and synth == 2, calls
    assert "could not produce" in json.loads(out)["answer"].lower()
