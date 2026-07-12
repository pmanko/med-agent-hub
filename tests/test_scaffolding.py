"""Single and team stage plans share deterministic context.

A single profile declares only a writer. A team profile declares gather plus an
orchestrator. Temporal context reaches both. Mocks the `_chat` boundary.
Run: pytest tests/test_scaffolding.py
"""
import asyncio
import json

from server import team
from tests.factories import run_profile, single_profile, team_profile


def _recording_chat(calls):
    async def fake_chat(
        client, model, messages, *, tools=None, response_format=None, **kwargs
    ):
        calls.append(
            {
                "model": model,
                "tools": bool(tools),
                "rf": response_format is not None,
                "messages": messages,
                "kwargs": kwargs,
            }
        )
        if response_format is not None:  # a synthesis turn
            return {
                "content": json.dumps({"answer": "ok", "citations": [], "blocks": []})
            }
        return {
            "content": "",
            "tool_calls": None,
        }  # orchestrator turn: nothing to gather -> stop

    return fake_chat


_MSGS = [
    {"role": "system", "content": "s"},
    {"role": "user", "content": "[1] (2026-01-07) Finding — Weight (kg): 41.0 kg"},
    {"role": "user", "content": "When was the patient's last visit?"},
]
_RF = {"type": "json_schema", "json_schema": {}}


def test_single_profile_runs_one_model_without_orchestrator(monkeypatch):
    # A single profile answers directly without a gather stage.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    profile = single_profile(answer="SYNTH", output="bare")
    asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    assert not any(c["tools"] for c in calls), calls  # no orchestrator/tool turns
    assert not any(
        c["model"] == "ORCH" for c in calls
    ), calls  # orchestrator model never called
    assert any(
        c["model"] == "SYNTH" and c["rf"] for c in calls
    ), calls  # the writer synthesized


def test_team_runs_orchestrator(monkeypatch):
    # A team profile explicitly declares gather and runs the orchestrator.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    profile = team_profile(
        orchestrator="ORCH", expert="EXPERT", answer="SYNTH", output="bare"
    )
    asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    assert any(c["model"] == "ORCH" and c["tools"] for c in calls), calls


def test_single_profile_still_gets_temporal_context(monkeypatch):
    # Context is independent of topology: deterministic temporal facts reach the writer.
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    profile = single_profile(answer="SYNTH", output="bare")
    asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    synth = next(c for c in calls if c["rf"])
    blob = json.dumps(synth["messages"])
    assert "temporal_facts" in blob
    assert "temporal_facts.v" not in blob
    assert "reference_date" in blob and "2026-01-07" in blob
    assert "last_clinical_encounter" in blob


_APPT_MSGS = [
    {"role": "system", "content": "s"},
    {
        "role": "user",
        "content": "\n".join(
            [
                "[1] (2026-01-07) Assessment — Scheduled visit: No",
                "[2] (2026-01-07) Misc — Return visit date: 2026-01-07",
            ]
        ),
    },
    {"role": "user", "content": "Does this patient have any upcoming appointments?"},
]


def _wrong_upcoming_chat(calls):
    async def fake_chat(
        client, model, messages, *, tools=None, response_format=None, **kwargs
    ):
        calls.append(
            {
                "model": model,
                "tools": bool(tools),
                "rf": response_format is not None,
                "messages": messages,
            }
        )
        if response_format is not None:
            return {
                "content": json.dumps(
                    {
                        "answer": "The next upcoming appointment is 2026-01-07 [2].",
                        "citations": [2],
                        "blocks": [],
                    }
                )
            }
        return {"content": "", "tool_calls": None}

    return fake_chat


def test_temporal_gate_enforce_patches_answer_and_trace(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(team, "_chat", _wrong_upcoming_chat(calls))
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))
    monkeypatch.setenv("HUB_BUILD_REVISION", "a" * 40)

    profile = single_profile(
        answer="SYNTH",
        output="bare",
        policies={"temporal_gate": "enforce", "anchor": "2026-06-20"},
    )
    out = asyncio.run(
        run_profile(
            profile,
            _APPT_MSGS,
            response_format=_RF,
            context={"temporal_gate": "enforce"},
        )
    )
    env = json.loads(out)
    assert env["answer"].startswith(
        "No upcoming appointment is documented after 2026-06-20"
    )
    assert env["citations"] == [2]

    trace = json.loads(
        (tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    )
    gate = trace["temporal_gate"]
    assert gate["mode"] == "enforce"
    assert gate["status"] == "fail"
    assert gate["applied"] == "patch"
    assert (
        trace["original_answer_text"]
        == "The next upcoming appointment is 2026-01-07 [2]."
    )
    assert trace["hub_revision"] == "a" * 40
    assert "temporal_facts_schema_version" not in trace


def test_temporal_gate_warn_records_failure_without_changing_answer(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(team, "_chat", _wrong_upcoming_chat(calls))
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))

    profile = single_profile(
        answer="SYNTH",
        output="bare",
        policies={"temporal_gate": "warn", "anchor": "2026-06-20"},
    )
    out = asyncio.run(
        run_profile(
            profile, _APPT_MSGS, response_format=_RF, context={"temporal_gate": "warn"}
        )
    )
    env = json.loads(out)
    assert env["answer"] == "The next upcoming appointment is 2026-01-07 [2]."

    trace = json.loads(
        (tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    )
    gate = trace["temporal_gate"]
    assert gate["mode"] == "warn"
    assert gate["status"] == "fail"
    assert gate["applied"] == "none"
    assert trace["original_answer_text"] is None


def _non_substantive_then_ok_chat(calls):
    state = {"n": 0}

    async def fake_chat(
        client, model, messages, *, tools=None, response_format=None, **kwargs
    ):
        calls.append(
            {
                "model": model,
                "tools": bool(tools),
                "rf": response_format is not None,
                "messages": messages,
                "kwargs": kwargs,
            }
        )
        if response_format is not None:
            state["n"] += 1
            answer = "..." if state["n"] == 1 else "The last visit was 2026-01-07 [1]."
            return {
                "content": json.dumps(
                    {"answer": answer, "citations": [1], "blocks": []}
                )
            }
        return {"content": "", "tool_calls": None}

    return fake_chat


def test_non_substantive_answer_resynthesizes_and_trace_is_not_green(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(team, "_chat", _non_substantive_then_ok_chat(calls))
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))

    profile = single_profile(answer="SYNTH", output="bare")
    out = asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    env = json.loads(out)
    assert env["answer"] == "The last visit was 2026-01-07 [1]."
    assert sum(1 for c in calls if c["rf"]) == 2

    trace = json.loads(
        (tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    )
    assert trace["answer_confidence"]["level"] == "yellow"
    assert any(s.get("reason") == "non-substantive" for s in trace["steps"])


def test_synth_temperature_floor_override_is_used_and_traced(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(team, "_chat", _recording_chat(calls))
    monkeypatch.setattr(team, "_TRACE_DIR", str(tmp_path))

    profile = single_profile(
        answer="SYNTH",
        output="bare",
        answer_prompt="synthesis-chartsearchai",
        knobs={"answer": {"temperature": 0.0}},
        profile_id="answer:SYNTH@synthesis-chartsearchai~enforce~temp0",
    )
    asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    synth = next(c for c in calls if c["rf"])
    assert synth["kwargs"]["temperature"] == 0.0
    trace = json.loads(
        (tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    )
    assert trace["sampling"]["synth_temperature"] == 0.0
    assert trace["sampling"]["synth_temperature_floor"] == 0.0
    assert trace["sampling"]["synth_temperature_source"] == "level_knob"


# ---- envelope-shape equivalence (the unify must NOT change any arm's output shape) ----
# A setup quirk in the envelope (bare vs sectioned) would confound LLM comparison, so pin each shape.


def _shape_chat():
    async def fake_chat(
        client, model, messages, *, tools=None, response_format=None, **kwargs
    ):
        name = (response_format or {}).get("json_schema", {}).get("name")
        if name == "in_depth":
            return {
                "content": json.dumps(
                    {"claims": ["WHO anemia threshold is Hgb < 12 g/dL."]}
                )
            }
        if name in ("rewrite_verdict", "indepth_verdict", "answer_verdict"):
            return {
                "content": json.dumps({"answer_ok": True, "errors": [], "drop": []})
            }
        if response_format is not None:  # the answer synth (chart_answer envelope)
            return {
                "content": json.dumps(
                    {
                        "answer": "Hemoglobin 7.1 g/dL [1].",
                        "citations": [1],
                        "blocks": [],
                    }
                )
            }
        return {"content": "", "tool_calls": None}  # orchestrator: no tools

    return fake_chat


def test_parity_no_indepth_ships_bare_envelope(monkeypatch):
    """A bare Answer profile ships {answer,citations,blocks} with no
    **Answer** header — so its output is byte-identical to the direct single-LLM arms.
    """
    monkeypatch.setattr(team, "_chat", _shape_chat())
    profile = single_profile(answer="SYNTH", output="bare")
    out = asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    ans = json.loads(out)["answer"]
    assert "**Answer**" not in ans and "**In Depth**" not in ans, ans
    assert "7.1" in ans


def test_parity_shared_indepth_ships_sectioned(monkeypatch):
    """Parity + shared In-Depth ships the sectioned **Answer** / **In Depth** body."""
    monkeypatch.setattr(team, "_chat", _shape_chat())
    profile = single_profile(answer="SYNTH", indepth="SYNTH", output="combined")
    out = asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    ans = json.loads(out)["answer"]
    assert "**Answer**" in ans and "**In Depth**" in ans, ans


def test_combined_profile_ships_sectioned_answer_and_indepth(monkeypatch):
    """A combined profile ships the sectioned **Answer** / **In Depth** body."""
    monkeypatch.setattr(team, "_chat", _shape_chat())
    profile = single_profile(
        answer="SYNTH", review="VAL", indepth="SYNTH", output="combined"
    )
    out = asyncio.run(run_profile(profile, _MSGS, response_format=_RF))
    ans = json.loads(out)["answer"]
    assert "**Answer**" in ans and "**In Depth**" in ans, ans
