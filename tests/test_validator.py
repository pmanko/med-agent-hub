"""Validator role: an optional post-synthesis audit that drives TWO INDEPENDENT remediation
paths — the **Answer** (strict: re-synthesize, else abstain) and the **In Depth** (advisory,
claim-level: block/strip exactly the claims the validator flags, Answer untouched, never abstain).
The In Depth is emitted as an enumerated claim list so each claim is individually addressable.
Mocks the LM Studio boundary (`_chat`) and exercises the real run_team.
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
    """Mock _chat. Orchestrator loop -> no tools (break to synth). Synth -> a versioned envelope
    whose answer has an **Answer** + a two-bullet **In Depth**. Validator -> a verdict
    {answer_ok, indepth_drop} pulled from `verdicts` (the real _run_validator clamps the drop)."""
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
                "indepth_drop": v.get("indepth_drop", []), "indepth_issues": ""})}
        if response_format is not None:  # synthesis (chart_answer schema)
            n = state["synth"]
            state["synth"] += 1
            return {"content": json.dumps({
                "answer": f"**Answer** ans-v{n}\n\n**In Depth**\n- claim-A-v{n} [1]\n- claim-B-v{n} per WHO",
                "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # loop turn

    return fake_chat


def _counts(calls):
    val = sum(1 for m, n in calls if n == "validator_verdict")
    synth = sum(1 for m, n in calls if n == "chart_answer")
    return val, synth


def _run(verdicts, **kw):
    calls = []
    monkey = team
    monkey._chat = _factory(calls, verdicts)
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH",
        synthesizer_model="SYNTH", validator_model="VALIDATOR", **kw))
    return calls, json.loads(out)["answer"]


# ---- pure helpers (claim addressability) -----------------------------------

def test_split_answer_indepth_parses_bullets():
    ans = "**Answer** the answer [3]\n\n**In Depth**\n- first claim [3]\n- second claim per WHO"
    answer_part, claims = team._split_answer_indepth(ans)
    assert "the answer" in answer_part and "**In Depth**" not in answer_part
    assert claims == ["first claim [3]", "second claim per WHO"]


def test_split_answer_indepth_no_section():
    answer_part, claims = team._split_answer_indepth("**Answer** not documented.")
    assert claims == []


def test_strip_indepth_claims_removes_only_flagged():
    env = json.dumps({"answer": "**Answer** A\n\n**In Depth**\n- keep me\n- drop me\n- keep me too",
                      "citations": [], "blocks": []})
    out = json.loads(team._strip_indepth_claims(env, [2]))["answer"]
    assert "keep me" in out and "keep me too" in out
    assert "drop me" not in out          # the flagged claim is gone
    assert "**Answer** A" in out         # the Answer is untouched


def test_strip_all_claims_keeps_answer_with_note():
    env = json.dumps({"answer": "**Answer** A\n\n**In Depth**\n- one\n- two", "citations": [], "blocks": []})
    out = json.loads(team._strip_indepth_claims(env, [1, 2]))["answer"]
    assert "**Answer** A" in out and "one" not in out and "two" not in out
    assert "withheld" in out.lower()


# ---- end-to-end through run_team --------------------------------------------

def test_validator_none_skips_audit(monkeypatch):
    monkeypatch.setattr(team, "_chat", _factory([], []))
    out = asyncio.run(team.run_team(_MESSAGES, response_format=_RF,
                                    orchestrator_model="ORCH", synthesizer_model="SYNTH"))
    assert "ans-v0" in json.loads(out)["answer"]


def test_validator_both_clean_ships_full():
    calls, ans = _run([{"answer_ok": True, "indepth_drop": []}])
    val, synth = _counts(calls)
    assert val == 1 and synth == 1, calls
    assert "ans-v0" in ans and "claim-A-v0" in ans and "claim-B-v0" in ans


def test_validator_indepth_claim_stripped_keeps_answer():
    """GRANULAR claim-level: drop claim #2 only — keep the Answer + claim #1, no re-synth, no abstain."""
    calls, ans = _run([{"answer_ok": True, "indepth_drop": [2]}])
    val, synth = _counts(calls)
    assert val == 1 and synth == 1, calls       # audited once, NOT re-synthesized
    assert "ans-v0" in ans                       # Answer kept
    assert "claim-A-v0" in ans                   # claim #1 kept
    assert "claim-B-v0" not in ans               # claim #2 stripped
    assert "could not produce" not in ans.lower()  # NOT a full abstain


def test_validator_all_indepth_dropped_keeps_answer():
    calls, ans = _run([{"answer_ok": True, "indepth_drop": [1, 2]}])
    val, synth = _counts(calls)
    assert val == 1 and synth == 1, calls
    assert "ans-v0" in ans and "claim-A-v0" not in ans and "claim-B-v0" not in ans
    assert "could not produce" not in ans.lower()  # In-Depth wipe never abstains the turn


def test_validator_answer_flagged_resynth_fixes():
    """ANSWER path is independent: flagged Answer -> re-synthesize -> now ok -> adopt."""
    calls, ans = _run([{"answer_ok": False, "indepth_drop": []},
                       {"answer_ok": True, "indepth_drop": []}], validator_max_loops=1)
    val, synth = _counts(calls)
    assert val == 2 and synth == 2, calls
    assert "ans-v1" in ans


def test_validator_answer_flagged_persists_abstains():
    calls, ans = _run([{"answer_ok": False, "indepth_drop": []},
                       {"answer_ok": False, "indepth_drop": []}], validator_max_loops=1)
    val, synth = _counts(calls)
    assert val == 2 and synth == 2, calls
    assert "could not produce" in ans.lower()
