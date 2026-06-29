"""P1: scaffolding (solo vs team) is DECOUPLED from context.

A `solo` arm runs ONE model (the writer) over the deterministic context (chart + temporal) — NO
orchestrator, no team. A `team` arm (solo=False) runs the orchestrator tool loop, as before. The
temporal context reaches BOTH (context is independent of scaffolding). Mocks the `_chat` boundary.
Run: pytest tests/test_scaffolding.py
"""
import asyncio
import json

from server import team


def _recording_chat(calls):
    async def fake_chat(client, model, messages, *, tools=None, response_format=None, **kwargs):
        calls.append({"model": model, "tools": bool(tools), "rf": response_format is not None,
                      "messages": messages})
        if response_format is not None:  # a synthesis turn
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # orchestrator turn: nothing to gather -> stop
    return fake_chat


_MSGS = [
    {"role": "system", "content": "s"},
    {"role": "user", "content": "[1] (2026-01-07) Finding — Weight (kg): 41.0 kg"},
    {"role": "user", "content": "When was the patient's last visit?"},
]
_RF = {"type": "json_schema", "json_schema": {}}


def test_solo_runs_one_model_no_orchestrator(monkeypatch):
    # solo scaffolding: the writer answers directly; NO orchestrator tool-loop turn at all.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    asyncio.run(team.run_team(_MSGS, response_format=_RF,
                              orchestrator_model="ORCH", synthesizer_model="SYNTH",
                              has_expert=False, two_call=False, solo=True))
    assert not any(c["tools"] for c in calls), calls            # no orchestrator/tool turns
    assert not any(c["model"] == "ORCH" for c in calls), calls  # orchestrator model never called
    assert any(c["model"] == "SYNTH" and c["rf"] for c in calls), calls  # the writer synthesized


def test_team_runs_orchestrator(monkeypatch):
    # team scaffolding (solo=False, the contrast): the orchestrator tool loop DOES run.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    asyncio.run(team.run_team(_MSGS, response_format=_RF,
                              orchestrator_model="ORCH", synthesizer_model="SYNTH",
                              has_expert=False, two_call=False, solo=False))
    assert any(c["model"] == "ORCH" and c["tools"] for c in calls), calls


def test_solo_still_gets_temporal_context(monkeypatch):
    # Context is independent of scaffolding: deterministic temporal facts reach the solo writer.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    asyncio.run(team.run_team(_MSGS, response_format=_RF,
                              orchestrator_model="ORCH", synthesizer_model="SYNTH",
                              has_expert=False, two_call=False, solo=True))
    synth = next(c for c in calls if c["rf"])
    blob = json.dumps(synth["messages"])
    assert "temporal_facts.v1" in blob
    assert "reference_date" in blob and "2026-01-07" in blob
    assert "last_clinical_encounter" in blob


_APPT_MSGS = [
    {"role": "system", "content": "s"},
    {"role": "user", "content": "\n".join([
        "[1] (2026-01-07) Assessment — Scheduled visit: No",
        "[2] (2026-01-07) Misc — Return visit date: 2026-01-07",
    ])},
    {"role": "user", "content": "Does this patient have any upcoming appointments?"},
]


def _wrong_upcoming_chat(calls):
    async def fake_chat(client, model, messages, *, tools=None, response_format=None, **kwargs):
        calls.append({"model": model, "tools": bool(tools), "rf": response_format is not None,
                      "messages": messages})
        if response_format is not None:
            return {"content": json.dumps({
                "answer": "The next upcoming appointment is 2026-01-07 [2].",
                "citations": [2],
                "blocks": [],
            })}
        return {"content": "", "tool_calls": None}
    return fake_chat


def test_temporal_gate_enforce_patches_answer_and_trace(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(team, "_chat", _wrong_upcoming_chat(calls))
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))

    out = asyncio.run(team.run_team(
        _APPT_MSGS, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        has_expert=False, two_call=False, solo=True,
        anchor="2026-06-20", context={"temporal_gate": "enforce"},
    ))
    env = json.loads(out)
    assert env["answer"].startswith("No upcoming appointment is documented after 2026-06-20")
    assert env["citations"] == [2]

    trace = json.loads((tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    gate = trace["temporal_gate"]
    assert gate["mode"] == "enforce"
    assert gate["status"] == "fail"
    assert gate["applied"] == "patch"
    assert trace["original_answer_text"] == "The next upcoming appointment is 2026-01-07 [2]."
    assert trace["temporal_facts_schema_version"] == "temporal_facts.v1.1"


def test_temporal_gate_warn_records_failure_without_changing_answer(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(team, "_chat", _wrong_upcoming_chat(calls))
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))

    out = asyncio.run(team.run_team(
        _APPT_MSGS, response_format=_RF,
        orchestrator_model="ORCH", synthesizer_model="SYNTH",
        has_expert=False, two_call=False, solo=True,
        anchor="2026-06-20", context={"temporal_gate": "warn"},
    ))
    env = json.loads(out)
    assert env["answer"] == "The next upcoming appointment is 2026-01-07 [2]."

    trace = json.loads((tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    gate = trace["temporal_gate"]
    assert gate["mode"] == "warn"
    assert gate["status"] == "fail"
    assert gate["applied"] == "none"
    assert trace["original_answer_text"] is None


# ---- envelope-shape equivalence (the unify must NOT change any arm's output shape) ----
# A setup quirk in the envelope (bare vs sectioned) would confound LLM comparison, so pin each shape.

def _shape_chat():
    async def fake_chat(client, model, messages, *, tools=None, response_format=None, **kwargs):
        name = (response_format or {}).get("json_schema", {}).get("name")
        if name == "in_depth":
            return {"content": json.dumps({"claims": ["WHO anemia threshold is Hgb < 12 g/dL."]})}
        if name in ("rewrite_verdict", "indepth_verdict", "answer_verdict"):
            return {"content": json.dumps({"answer_ok": True, "errors": [], "drop": []})}
        if response_format is not None:  # the answer synth (chart_answer envelope)
            return {"content": json.dumps({"answer": "Hemoglobin 7.1 g/dL [1].", "citations": [1], "blocks": []})}
        return {"content": "", "tool_calls": None}  # orchestrator: no tools
    return fake_chat


def test_parity_no_indepth_ships_bare_envelope(monkeypatch):
    """Parity (two_call=False, no shared In-Depth) ships the BARE {answer,citations,blocks} — no
    **Answer** header — so its output is byte-identical to the direct single-LLM arms."""
    monkeypatch.setattr(team, "_chat", _shape_chat())
    out = asyncio.run(team.run_team(_MSGS, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", has_expert=False, two_call=False, solo=True))
    ans = json.loads(out)["answer"]
    assert "**Answer**" not in ans and "**In Depth**" not in ans, ans
    assert "7.1" in ans


def test_parity_shared_indepth_ships_sectioned(monkeypatch):
    """Parity + shared In-Depth ships the sectioned **Answer** / **In Depth** body."""
    monkeypatch.setattr(team, "_chat", _shape_chat())
    out = asyncio.run(team.run_team(_MSGS, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", has_expert=False, two_call=False,
                                    solo=True, indepth_shared=True))
    ans = json.loads(out)["answer"]
    assert "**Answer**" in ans and "**In Depth**" in ans, ans


def test_two_call_ships_sectioned(monkeypatch):
    """Two-call ships the sectioned **Answer** / **In Depth** body."""
    monkeypatch.setattr(team, "_chat", _shape_chat())
    out = asyncio.run(team.run_team(_MSGS, response_format=_RF, orchestrator_model="ORCH",
                                    synthesizer_model="SYNTH", validator_model="VAL",
                                    has_expert=False, two_call=True, solo=True))
    ans = json.loads(out)["answer"]
    assert "**Answer**" in ans and "**In Depth**" in ans, ans
