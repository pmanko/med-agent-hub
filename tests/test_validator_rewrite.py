"""Rewrite-mode answer validator: the validator LOCALIZES each chart contradiction and returns the
corrected answer; the refine loop ADOPTS that fix (not a from-scratch regeneration) and keeps the BEST
(fewest-errors) version seen, NEVER regressing below the original draft (the over-correction guard).

Selected per-arm by validator_prompt="validation-rewrite". Mocks only the LM Studio boundary (`_chat`)
and exercises the real run_team end-to-end, branching on the response_format json_schema name so the
Answer synth, the rewrite_verdict validator, and the In-Depth calls are served independently.

Run: pytest tests/test_validator_rewrite.py
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


def _factory(calls, rewrite_verdicts):
    """Mock _chat. Branches on the response_format json_schema name:
      "chart_answer"     (Answer synth)    -> {answer: ans-vN} (N = synth call index);
      "in_depth"         (In-Depth synth)  -> {claims:[...]};
      "rewrite_verdict"  (rewrite validator) -> the Nth entry of `rewrite_verdicts`, verbatim;
      "indepth_verdict"  -> {drop:[]} (no-op here);
      no response_format (orchestrator)    -> no tools (break to synthesis)."""
    state = {"answer_synth": 0, "indepth_synth": 0, "rw": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))
        if name == "chart_answer":
            n = state["answer_synth"]; state["answer_synth"] += 1
            return {"content": json.dumps({"answer": f"ans-v{n}", "citations": [], "blocks": []})}
        if name == "in_depth":
            n = state["indepth_synth"]; state["indepth_synth"] += 1
            return {"content": json.dumps({"claims": [f"claim-A-v{n}"]})}
        if name == "rewrite_verdict":
            v = rewrite_verdicts[min(state["rw"], len(rewrite_verdicts) - 1)] if rewrite_verdicts else {"answer_ok": True}
            state["rw"] += 1
            return {"content": json.dumps(v)}
        if name == "indepth_verdict":
            return {"content": json.dumps({"drop": [], "issues": ""})}
        return {"content": "", "tool_calls": None}

    return fake_chat


def _counts(calls):
    return (sum(1 for _m, n in calls if n == "chart_answer"),
            sum(1 for _m, n in calls if n == "rewrite_verdict"))


def _run(rewrite_verdicts, **kw):
    calls = []
    team._chat = _factory(calls, rewrite_verdicts)
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VALIDATOR", validator_prompt="validation-rewrite", **kw))
    return calls, json.loads(out)


def _err(wrong, chart, fix):
    return {"wrong": wrong, "chart": chart, "fix": fix}


def _factory_answers(calls, answers, rewrite_verdicts):
    """Like _factory but the writer returns answers[N] (so a test can force a non-substantive draft)."""
    state = {"answer_synth": 0, "indepth_synth": 0, "rw": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None,
                        dry_multiplier=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))
        if name == "chart_answer":
            n = state["answer_synth"]; state["answer_synth"] += 1
            ans = answers[min(n, len(answers) - 1)]
            return {"content": json.dumps({"answer": ans, "citations": [], "blocks": []})}
        if name == "in_depth":
            n = state["indepth_synth"]; state["indepth_synth"] += 1
            return {"content": json.dumps({"claims": [f"claim-A-v{n}"]})}
        if name == "rewrite_verdict":
            v = rewrite_verdicts[min(state["rw"], len(rewrite_verdicts) - 1)] if rewrite_verdicts else {"answer_ok": True}
            state["rw"] += 1
            return {"content": json.dumps(v)}
        if name == "indepth_verdict":
            return {"content": json.dumps({"drop": [], "issues": ""})}
        return {"content": "", "tool_calls": None}

    return fake_chat


def test_nonsubstantive_answer_ships_red_not_green():
    """THE SUBSTANCE GATE: a "." answer the LLM validator can't fault (no claim to contradict) must NOT
    ship green. After retries that stay non-substantive, the hub ships the fallback with RED confidence."""
    calls = []
    # writer always emits "."; validator finds nothing to localize (clean) -> w/o the gate this is green.
    team._chat = _factory_answers(calls, ["."], [{"answer_ok": True, "errors": [], "corrected_answer": ""}])
    env = json.loads(asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, orchestrator_model="ORCH", synthesizer_model="SYNTH",
        validator_model="VALIDATOR", validator_prompt="validation-rewrite")))
    assert env["confidence"]["answer"]["level"] == "red", env["confidence"]
    assert env["answer"].strip() != ".", env["answer"]
    assert any(ch.isalnum() for ch in env["answer"]), env["answer"]  # fallback prose, not punctuation


def test_clean_pass_is_green_and_untouched():
    """No localized contradiction -> green, the original answer ships, validator runs ONCE, no re-synth."""
    calls, env = _run([{"answer_ok": True, "errors": [], "corrected_answer": ""}])
    synth, rw = _counts(calls)
    assert synth == 1 and rw == 1, calls            # one synth, one audit, NO regeneration
    assert env["confidence"]["answer"]["level"] == "green"
    assert "ans-v0" in env["answer"]


def test_adopts_corrected_answer_without_regenerating():
    """Flagged with a fix that clears on re-audit -> yellow, the VALIDATOR's corrected text ships, and
    the writer is NOT asked to regenerate (chart_answer synth stays at 1 — the rewrite is adopted, not
    re-generated). This is the core regenerate->rewrite behavioral change."""
    calls, env = _run([
        {"answer_ok": False, "errors": [_err("65 kg", "weight 71 kg on 2026-01-26 [15]", "71 kg")],
         "corrected_answer": "The patient weighs 71 kg [15]."},
        {"answer_ok": True, "errors": [], "corrected_answer": ""},
    ])
    synth, rw = _counts(calls)
    assert synth == 1, f"answer must NOT be regenerated, got {synth} synth calls: {calls}"
    assert rw == 2, calls                            # audit draft + re-audit the adopted fix
    assert env["confidence"]["answer"]["level"] == "yellow"
    assert "71 kg" in env["answer"] and "ans-v0" not in env["answer"]   # the corrected text replaced the draft
    assert "[15]" in env["answer"]                   # citation carried from the corrected text


def test_never_regress_reverts_to_original_when_rewrite_is_worse():
    """THE OVER-CORRECTION GUARD: the rewrite re-audits with MORE errors than the original draft ->
    REVERT to the original answer (never ship a worse rewrite). Level red, original text preserved."""
    calls, env = _run([
        {"answer_ok": False, "errors": [_err("65 kg", "weight 71 kg [15]", "71 kg")],
         "corrected_answer": "The patient weighs 71 kg but the HbA1c is 12 and BP 200/130."},
        # re-audit of the rewrite: it introduced NEW errors (2 > the original's 1)
        {"answer_ok": False, "errors": [_err("HbA1c 12", "no HbA1c in chart", "remove"),
                                        _err("BP 200/130", "no BP in chart", "remove")],
         "corrected_answer": "even worse"},
    ], validator_max_loops=1)
    assert env["confidence"]["answer"]["level"] == "red"
    assert "ans-v0" in env["answer"], "must REVERT to the original draft, not keep the worse rewrite"
    assert "200/130" not in env["answer"], "the worse rewrite must NOT ship"


def test_adopts_improved_but_imperfect_rewrite_as_red():
    """Rewrite re-audits with FEWER errors (1 < 2) but not clean -> adopt the improved rewrite, red.
    Strictly-better is kept; the guard only blocks regressions, not partial improvements."""
    calls, env = _run([
        {"answer_ok": False, "errors": [_err("a", "x [1]", "a2"), _err("b", "y [2]", "b2")],
         "corrected_answer": "improved answer [1] [2]"},
        {"answer_ok": False, "errors": [_err("b", "y [2]", "b2")],
         "corrected_answer": "improved answer [1] [2]"},
    ], validator_max_loops=1)
    assert env["confidence"]["answer"]["level"] == "red"
    assert "improved answer" in env["answer"], "a strictly-fewer-errors rewrite must be adopted"


def test_flag_without_a_rewrite_keeps_original_red():
    """Validator flags an error but offers NO corrected_answer -> keep the original draft, red caveat
    (nothing usable to adopt; never blank the answer)."""
    calls, env = _run([
        {"answer_ok": False, "errors": [_err("65 kg", "weight 71 kg [15]", "71 kg")],
         "corrected_answer": ""},
    ], validator_max_loops=1)
    assert env["confidence"]["answer"]["level"] == "red"
    assert "ans-v0" in env["answer"]
