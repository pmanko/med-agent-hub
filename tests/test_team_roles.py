"""Unit test for the team role split: the orchestrator tool loop and the final
synthesis use SEPARATE models — a fast small model runs the chatty loop while a
larger model composes the answer. Mocks the LM Studio boundary (`_chat`) and
asserts which model each call targets, exercising the real `run_team` routing
(not a reimplementation). Run: `pytest tests/test_team_roles.py`.
"""

import asyncio
import json

from server import team


def _fake_chat_factory(calls):
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
        calls.append((model, response_format is not None))
        if response_format is not None:  # synthesis turn (schema-bound)
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # loop turn: no tool calls -> break to synthesis
    return fake_chat


_MESSAGES = [
    {"role": "system", "content": "envelope system"},
    {"role": "user", "content": "patient chart"},
    {"role": "user", "content": "the question"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def test_synthesis_uses_synthesizer_model_loop_uses_orchestrator(monkeypatch):
    """Explicit per-call models: the loop runs on the orchestrator, synthesis on
    the synthesizer. RED on today's code (run_team has no synthesizer_model param)."""
    calls = []
    monkeypatch.setattr(team, "_chat", _fake_chat_factory(calls))
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH-MODEL", synthesizer_model="SYNTH-MODEL",
    ))
    loop_models = [m for (m, is_synth) in calls if not is_synth]
    synth_models = [m for (m, is_synth) in calls if is_synth]
    assert loop_models and all(m == "ORCH-MODEL" for m in loop_models), calls
    assert synth_models == ["SYNTH-MODEL"], calls
    assert json.loads(out)["answer"] == "ok"


def test_synthesis_applies_anti_degeneration_params(monkeypatch):
    """The synthesis call gets a repeat penalty + a temperature floor (breaks the
    small synth's repetition loop); the orchestrator loop gets neither, so its
    tool-calling stays at the request temperature. Red without the fix: synth's
    repeat_penalty would be None and its temperature would be the request's 0.0."""
    seen = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
        seen.append({"synth": response_format is not None,
                     "temperature": temperature, "repeat_penalty": repeat_penalty})
        if response_format is not None:
            return {"content": json.dumps({"answer": "ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)
    asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF, temperature=0.0,
        orchestrator_model="ORCH-MODEL", synthesizer_model="SYNTH-MODEL",
    ))
    synth = [c for c in seen if c["synth"]]
    loop = [c for c in seen if not c["synth"]]
    assert synth and synth[0]["repeat_penalty"] == team.SYNTH_REPEAT_PENALTY, seen
    assert synth[0]["temperature"] >= team._SYNTH_MIN_TEMPERATURE, seen
    assert loop and all(c["repeat_penalty"] is None for c in loop), seen


def test_synthesis_reads_reasoning_content_when_content_empty(monkeypatch):
    """A reasoning synthesizer (Qwen 3.x via LM Studio MLX) returns the structured
    envelope in `reasoning_content` and leaves `content` empty. The synth must read it
    instead of falling back. Red without the fix: empty content -> run_team returns the
    'I could not produce a complete answer' fallback, so out['answer'] != the real answer."""
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
        if response_format is not None:  # synthesis turn: answer hidden in reasoning_content
            return {"content": "",
                    "reasoning_content": json.dumps({"answer": "qwen-ok", "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}  # loop turn: no tools -> straight to synthesis

    monkeypatch.setattr(team, "_chat", fake_chat)
    out = asyncio.run(team.run_team(
        _MESSAGES, response_format=_RF,
        orchestrator_model="ORCH-MODEL", synthesizer_model="SYNTH-MODEL",
    ))
    assert json.loads(out)["answer"] == "qwen-ok", out


def test_synthesis_normalizes_literal_newline_and_reconciles_citations(monkeypatch):
    """Post-process the synth envelope: a literal backslash-n in `answer` becomes a real
    newline (small models, e.g. qwen3-14b, copy the prompt's JSON \\n escaping verbatim
    and garble), and inline [N] chart-record markers are reconciled into `citations` so
    the count is not lost. Red without _normalize_envelope: the literal \\n survives and
    citations stays []."""
    literal = "**Answer**" + "\\n" + "Regimen is outdated [29], [30]."  # literal backslash-n

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, repeat_penalty=None, dry_multiplier=None, **kwargs):
        if response_format is not None:
            return {"content": json.dumps({"answer": literal, "citations": [], "blocks": []})}
        return {"content": "", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)
    env = json.loads(asyncio.run(team.run_team(_MESSAGES, response_format=_RF, synthesizer_model="S")))
    assert "\\n" not in env["answer"], env["answer"]      # literal backslash-n normalized away
    assert "\n" in env["answer"], env["answer"]            # to a real newline
    assert env["citations"] == [29, 30], env               # inline [N] reconciled into citations


def test_normalize_envelope_strips_backslash_run_artifacts():
    """qwen2.5-14b mis-escapes the section line breaks as RUNS of backslashes
    (e.g. "**Answer**\\\\\\: text", "**Answer**\\\\\\\\<newline>This"), which render as
    literal backslashes. _normalize_envelope must collapse those runs to a clean line
    break. Red without the run-collapse: the backslashes survive (only a single \\n was
    handled before)."""
    raw = json.dumps(
        {
            "answer": "**Answer**" + "\\" * 6 + ": The patient is on lamivudine [3]."
            + "\\" * 4 + "\n**In Depth**" + "\\" * 3 + "Per WHO guidance, the regimen is outdated.",
            "citations": [],
            "blocks": [],
        }
    )
    out = json.loads(team._normalize_envelope(raw))
    assert "\\" not in out["answer"], repr(out["answer"])   # no backslash artifacts remain
    assert "**Answer**" in out["answer"]                    # header preserved
    assert "**In Depth**" in out["answer"]
    assert "The patient is on lamivudine" in out["answer"]  # content preserved
    assert out["citations"] == [3]                          # inline [N] reconcile still works
