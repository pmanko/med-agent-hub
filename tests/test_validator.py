"""Two-call generation + two independent validators, each with a re-synth cycle that yields a
per-section CONFIDENCE label (green / yellow / red). The Answer and the In-Depth are distinct from
generation onward and combined into one body only at the chartsearchai handoff.

  Confidence (per section): green = passed review first pass; yellow = flagged -> re-synthesized ->
  cleared; red = flagged -> re-synthesized -> still flagged. We ALWAYS ship the answer — a red
  section ships WITH its criticism (never hidden; the renderer decides visibility). A reasonless
  Answer flag (answer_ok=False, empty issues) is treated as PASS (noise guard). The structured
  confidence rides the envelope `confidence` field AND the reasoning trace.

Mocks only the model boundary (`_chat`) and exercises the compiled stages end-to-end. The mock
branches on the response_format json_schema name so the four call sites (chart_answer synth, in_depth
synth, answer_verdict, indepth_verdict) are served independently.
Run: pytest tests/test_validator.py
"""

import asyncio
import json

from server import team
from tests.factories import run_profile, team_profile

_MESSAGES = [
    {"role": "system", "content": "envelope system"},
    {"role": "user", "content": "patient chart"},
    {"role": "user", "content": "the question"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def _factory(calls, verdicts, iv_drops=None):
    """Mock _chat. Branches on the response_format json_schema name:
    "chart_answer"    (Answer synth)      -> {answer: ans-vN, ...} (N = answer-synth call index);
    "in_depth"        (In-Depth synth)    -> {claims: [claim-A-vN, claim-B-vN]};
    "answer_verdict"  (Answer validator)  -> the Nth entry of `verdicts` ({answer_ok, answer_issues});
    "indepth_verdict" (In-Depth validator)-> {drop} = the Nth entry of `iv_drops` (a list of drop-
                        lists per In-Depth audit attempt); falls back to verdicts[0].drop / [].
    no response_format (orchestrator)     -> no tools (break to synthesis)."""
    state = {"answer_synth": 0, "indepth_synth": 0, "answer_val": 0, "iv": 0}

    async def fake_chat(
        client,
        model,
        messages,
        *,
        tools=None,
        response_format=None,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry_multiplier=None,
        **kwargs,
    ):
        name = (response_format or {}).get("json_schema", {}).get("name")
        calls.append((model, name))

        if name == "chart_answer":
            n = state["answer_synth"]
            state["answer_synth"] += 1
            return {
                "content": json.dumps(
                    {"answer": f"ans-v{n}", "citations": [], "blocks": []}
                )
            }

        if name == "in_depth":
            n = state["indepth_synth"]
            state["indepth_synth"] += 1
            return {
                "content": json.dumps({"claims": [f"claim-A-v{n}", f"claim-B-v{n}"]})
            }

        if name == "answer_verdict":
            v = (
                verdicts[min(state["answer_val"], len(verdicts) - 1)]
                if verdicts
                else {"answer_ok": True}
            )
            state["answer_val"] += 1
            return {
                "content": json.dumps(
                    {
                        "answer_ok": v["answer_ok"],
                        "answer_issues": v.get("answer_issues", ""),
                    }
                )
            }

        if name == "indepth_verdict":
            if iv_drops is not None:
                d = iv_drops[min(state["iv"], len(iv_drops) - 1)]
            else:
                d = verdicts[0].get("drop", []) if verdicts else []
            state["iv"] += 1
            return {
                "content": json.dumps({"drop": d, "issues": "flagged" if d else ""})
            }

        return {"content": "", "tool_calls": None}

    return fake_chat


def _counts(calls):
    return (
        sum(1 for _m, n in calls if n == "chart_answer"),
        sum(1 for _m, n in calls if n == "in_depth"),
        sum(1 for _m, n in calls if n == "answer_verdict"),
        sum(1 for _m, n in calls if n == "indepth_verdict"),
    )


def _run(verdicts, iv_drops=None, **kw):
    """Run a turn; return (calls, parsed_envelope)."""
    calls = []
    team._chat = _factory(calls, verdicts, iv_drops)
    profile = team_profile(
        orchestrator="ORCH",
        answer="SYNTH",
        review="VALIDATOR",
        indepth="SYNTH",
        output="combined",
        policies={"review_loops": int(kw.get("validator_max_loops", 1))},
    )
    out = asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF))
    return calls, json.loads(out)


def test_no_validator_ships_answer_and_both_claims(monkeypatch):
    calls = []
    monkeypatch.setattr(team, "_chat", _factory(calls, []))
    profile = team_profile(
        orchestrator="ORCH", answer="SYNTH", indepth="SYNTH", output="combined"
    )
    env = json.loads(asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF)))
    ans = env["answer"]
    assert "ans-v0" in ans and "claim-A-v0" in ans and "claim-B-v0" in ans
    _as, _is, av, iv = _counts(calls)
    assert av == 0 and iv == 0, calls


# ---- ANSWER confidence -----------------------------------------------------


def test_answer_green_clean_first_pass():
    calls, env = _run([{"answer_ok": True, "drop": []}])
    assert env["confidence"]["answer"]["level"] == "green"
    assert "🔴" not in env["answer"] and "🟡" not in env["answer"]
    assert "ans-v0" in env["answer"] and "claim-A-v0" in env["answer"]


# NOTE: the regenerate-path answer tests (flag -> re-synthesize -> yellow/red, and reasonless-noise ->
# green) were removed when the answer validator collapsed to rewrite-only. The rewrite validator's
# flag/adopt/never-regress behavior is covered in test_validator_rewrite.py; the deterministic substance
# gate (non-substantive answer never ships green) is covered there too.


# ---- IN-DEPTH confidence (re-synth before strip) ---------------------------


def test_indepth_green_no_drop():
    calls, env = _run([{"answer_ok": True, "drop": []}])
    assert env["confidence"]["in_depth"]["level"] == "green"
    assert "claim-A-v0" in env["answer"] and "claim-B-v0" in env["answer"]


def test_indepth_yellow_resynth_clears_drop():
    """In-Depth flagged on first audit -> re-synth -> clean -> yellow, the REVISED claims (v1) ship."""
    calls, env = _run([{"answer_ok": True}], iv_drops=[[2], []])
    _a, i_synth, _av, i_val = _counts(calls)
    assert i_synth == 2 and i_val == 2, calls  # synth + re-synth, two audits
    assert env["confidence"]["in_depth"]["level"] == "yellow"
    assert "claim-A-v1" in env["answer"]  # revised claims adopted


def test_indepth_red_strips_after_failed_resynth():
    """In-Depth flagged -> re-synth -> STILL flagged -> red: strip the still-flagged claim, keep rest."""
    calls, env = _run([{"answer_ok": True}], iv_drops=[[2], [2]])
    _a, i_synth, _av, i_val = _counts(calls)
    assert i_synth == 2 and i_val == 2, calls
    assert env["confidence"]["in_depth"]["level"] == "red"
    assert "claim-A-v1" in env["answer"]  # surviving claim kept
    assert "claim-B-v1" not in env["answer"]  # still-flagged claim stripped


def test_unavailable_indepth_reviewer_cannot_ship_complete(monkeypatch):
    async def synthesize(*_args, **_kwargs):
        return ["A claim whose review did not complete."]

    async def fail_review(*_args, **_kwargs):
        raise RuntimeError("reviewer unavailable")

    monkeypatch.setattr(team, "_synthesize_indepth", synthesize)
    monkeypatch.setattr(team, "_validate_indepth_verdict", fail_review)
    steps = []

    claims, confidence = asyncio.run(
        team._gen_indepth(
            None,
            "SYNTH",
            [],
            "instruction",
            "gathered",
            "answer",
            validator_model="VALIDATOR",
            validator_prompt="validation",
            chart="chart",
            synth_temperature=0.0,
            synth_repeat_penalty=None,
            synth_dry=None,
            validator_temperature=0.0,
            validator_repeat_penalty=None,
            validator_dry=None,
            max_tokens=128,
            max_loops=1,
            steps=steps,
        )
    )

    assert claims == []
    assert confidence["level"] == "red"
    assert "unavailable" in confidence["note"].lower()
    validator_step = next(step for step in steps if step["role"] == "indepth_validator")
    assert validator_step["status"] == "unavailable"


# ---- trace package ---------------------------------------------------------


def test_trace_carries_structured_package(tmp_path, monkeypatch):
    """The hub writes the structured package: shipped answer_text + in_depth_claims + per-section
    confidence + ordered steps, keyed by level_id (the dashboard's render + correlation source).
    """
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))
    calls = []
    monkeypatch.setattr(
        team, "_chat", _factory(calls, [{"answer_ok": True, "drop": []}])
    )
    profile = team_profile(
        orchestrator="ORCH",
        answer="SYNTH",
        review="VALIDATOR",
        indepth="SYNTH",
        output="combined",
        profile_id="med-agent-team-low-validated",
    )
    asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF))
    entry = json.loads((tmp_path / "trace.jsonl").read_text().splitlines()[0])
    assert entry["level_id"] == "med-agent-team-low-validated"
    assert entry["answer_confidence"]["level"] == "green"
    assert entry["indepth_confidence"]["level"] == "green"
    assert "ans-v0" in entry["answer_text"]
    assert entry["in_depth_claims"] and "claim-A-v0" in entry["in_depth_claims"][0]
    roles = [s["role"] for s in entry["steps"]]
    assert "answer_synth" in roles and "answer_review" in roles
    assert "indepth_synth" in roles and "indepth_validator" in roles
