"""Two-call generation + two independent validators. From generation onward the **Answer**
and the **In Depth** are DISTINCT: a separate Answer synthesis (bound to chartsearchai's
{answer, citations, blocks} schema) and a separate In-Depth synthesis (a list of claim strings),
each audited by its own validator, combined into one markdown body ("**Answer**\\n...\\n\\n**In
Depth**\\n- ...") only at the chartsearchai handoff.

  ANSWER (strict): a flagged-with-a-reason Answer -> re-synthesize, else ABSTAIN the turn.
  IN DEPTH (advisory, claim-level): drop exactly the claims the validator flags; never abstains.

Mocks only the LM Studio boundary (`_chat`) and exercises the real run_team end-to-end (asserting
the final combined answer string). The mock branches on the response_format json_schema name so the
four distinct call sites (chart_answer synth, in_depth synth, answer_verdict, indepth_verdict) are
served independently — exactly how the production code distinguishes them.
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
# chartsearchai's envelope response_format; its json_schema name is what the Answer synth call
# carries, so the mock recognizes that call site by this name.
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def _factory(calls, verdicts):
    """Mock _chat that branches on the response_format json_schema name, mirroring the four
    distinct call sites in team.run_team:

      "chart_answer"   (Answer synthesis)   -> a versioned {answer, citations, blocks} envelope;
      "in_depth"       (In-Depth synthesis) -> two versioned claim strings;
      "answer_verdict" (Answer validator)   -> {answer_ok, answer_issues} pulled from `verdicts`;
      "indepth_verdict"(In-Depth validator) -> {drop} pulled from `verdicts`;
      no response_format (orchestrator loop) -> no tools, no content (break to synthesis).

    `verdicts` is the per-Answer-audit-attempt list; the In-Depth `drop` is read from the FIRST
    verdict (the In-Depth validator runs once)."""
    state = {"answer_synth": 0, "indepth_synth": 0, "answer_val": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))

        if name == "chart_answer":  # Answer synthesis (chartsearchai envelope schema)
            n = state["answer_synth"]
            state["answer_synth"] += 1
            return {"content": json.dumps({
                "answer": f"ans-v{n}", "citations": [], "blocks": []})}

        if name == "in_depth":  # In-Depth synthesis (claim list)
            n = state["indepth_synth"]
            state["indepth_synth"] += 1
            return {"content": json.dumps({
                "claims": [f"claim-A-v{n}", f"claim-B-v{n}"]})}

        if name == "answer_verdict":  # Answer validator
            v = verdicts[min(state["answer_val"], len(verdicts) - 1)]
            state["answer_val"] += 1
            return {"content": json.dumps({
                "answer_ok": v["answer_ok"], "answer_issues": v.get("answer_issues", "")})}

        if name == "indepth_verdict":  # In-Depth validator
            drop = verdicts[0].get("drop", []) if verdicts else []
            return {"content": json.dumps({"drop": drop, "issues": ""})}

        return {"content": "", "tool_calls": None}  # orchestrator loop turn

    return fake_chat


def _counts(calls):
    answer_synth = sum(1 for _m, n in calls if n == "chart_answer")
    indepth_synth = sum(1 for _m, n in calls if n == "in_depth")
    answer_val = sum(1 for _m, n in calls if n == "answer_verdict")
    indepth_val = sum(1 for _m, n in calls if n == "indepth_verdict")
    return answer_synth, indepth_synth, answer_val, indepth_val


def _run(verdicts, **kw):
    calls = []
    team._chat = _factory(calls, verdicts)
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH",
        synthesizer_model="SYNTH", validator_model="VALIDATOR", **kw))
    return calls, json.loads(out)["answer"]


def test_no_validator_ships_answer_and_both_claims(monkeypatch):
    """No validator -> ship the Answer + both In-Depth claim bullets, no audit calls."""
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, []))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH", synthesizer_model="SYNTH"))
    ans = json.loads(out)["answer"]
    assert "**Answer**" in ans and "ans-v0" in ans
    assert "claim-A-v0" in ans and "claim-B-v0" in ans
    _as, _is, av, iv = _counts(calls)
    assert av == 0 and iv == 0, calls  # no validator was configured


def test_both_clean_ships_full_body():
    """Both validators pass -> FULL combined body: **Answer** + both In-Depth claims."""
    calls, ans = _run([{"answer_ok": True, "drop": []}])
    answer_synth, indepth_synth, answer_val, indepth_val = _counts(calls)
    assert answer_synth == 1 and indepth_synth == 1, calls
    assert answer_val == 1 and indepth_val == 1, calls
    assert "**Answer**" in ans and "ans-v0" in ans
    assert "**In Depth**" in ans
    assert "claim-A-v0" in ans and "claim-B-v0" in ans


def test_indepth_drop_strips_only_flagged_claim_no_resynth():
    """In-Depth drop=[2]: keep the Answer + claim-A, strip claim-B. NOT an abstain, and the
    In-Depth synth runs ONCE (the Answer is never re-synthesized for an In-Depth drop)."""
    calls, ans = _run([{"answer_ok": True, "drop": [2]}])
    answer_synth, indepth_synth, answer_val, indepth_val = _counts(calls)
    assert answer_synth == 1, calls          # Answer synthesized once (no re-synth)
    assert indepth_synth == 1, calls         # In-Depth synthesized once
    assert answer_val == 1 and indepth_val == 1, calls
    assert "ans-v0" in ans                    # Answer kept
    assert "claim-A-v0" in ans               # claim #1 kept
    assert "claim-B-v0" not in ans           # claim #2 dropped
    assert "could not produce" not in ans.lower()  # NOT an abstain


def test_answer_flagged_with_reason_resynth_fixes_adopts_v1():
    """ANSWER path: flagged WITH a reason -> re-synthesize -> now ok -> adopt the revision (v1)."""
    calls, ans = _run([{"answer_ok": False, "answer_issues": "wrong dose"},
                       {"answer_ok": True}], validator_max_loops=1)
    answer_synth, indepth_synth, answer_val, indepth_val = _counts(calls)
    assert answer_synth == 2 and answer_val == 2, calls  # re-synth + re-audit
    assert "ans-v1" in ans                                # adopted the revision
    assert indepth_synth == 1, calls                      # In-Depth still runs once afterward


def test_answer_flagged_with_reason_persists_abstains_gracefully():
    """ANSWER path: flagged WITH a reason and STILL flagged -> GRACEFUL abstain: a nuanced message
    that surfaces what the review found (answer_issues), AND the In-Depth KB context is still shipped."""
    calls, ans = _run([{"answer_ok": False, "answer_issues": "wrong dose"},
                       {"answer_ok": False, "answer_issues": "still wrong"}], validator_max_loops=1)
    answer_synth, indepth_synth, answer_val, indepth_val = _counts(calls)
    assert answer_synth == 2 and answer_val == 2, calls
    assert "could not confirm" in ans.lower()             # graceful abstain (nuanced wording)
    assert "still wrong" in ans                           # surfaces the specific review finding
    assert "ans-v" not in ans                             # the flagged draft answer is NOT shipped
    assert indepth_synth == 1, calls                      # In-Depth KB context IS still generated
    assert "claim-A-v0" in ans                            # ...and shipped alongside the abstain


def test_answer_flagged_without_reason_ships_noise_guard():
    """NOISE GUARD: answer_ok=False with EMPTY answer_issues is a reasonless flag -> treat as PASS,
    ship the answer (NOT an abstain), and do not re-synthesize."""
    calls, ans = _run([{"answer_ok": False, "answer_issues": ""}])
    answer_synth, indepth_synth, answer_val, indepth_val = _counts(calls)
    assert answer_synth == 1 and answer_val == 1, calls   # single synth + single audit, no re-synth
    assert "ans-v0" in ans                                # shipped the answer
    assert "could not produce" not in ans.lower()         # NOT an abstain
    assert "claim-A-v0" in ans                            # In-Depth still flows through


def test_reasoning_trace_is_written_with_steps_and_disposition(tmp_path, monkeypatch):
    """The hub appends a per-turn reasoning trace: the ordered steps (answer_synth, answer_validator,
    indepth_synth, indepth_validator) + the disposition + the level_id correlation key."""
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, [{"answer_ok": True, "drop": []}]))
    asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VALIDATOR", level_id="med-agent-team-low-validated"))
    lines = (tmp_path / "trace.jsonl").read_text().splitlines()
    assert len(lines) == 1, lines
    entry = json.loads(lines[0])
    assert entry["level_id"] == "med-agent-team-low-validated"
    assert entry["disposition"] == "full"
    roles = [s["role"] for s in entry["steps"]]
    assert "answer_synth" in roles and "answer_validator" in roles
    assert "indepth_synth" in roles and "indepth_validator" in roles
    assert entry["ts"] and entry["models"]["synthesizer"] == "SYNTH"
