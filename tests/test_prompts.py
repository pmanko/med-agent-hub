"""
Prompt-loading tests (server/prompt_loader.py + its wiring into server/team.py).

The team's system prompts are plain files under server/prompts/, read per request
so a prompt edit changes behaviour with no rebuild. These tests assert the
MECHANISM — which prompt TEXT the loader returns and the team actually sends — not
the model's output. `team._chat` is seamed: no model, no HTTP.

The files are the single source of truth (git is the version history); a referenced
prompt with no file fails loud rather than silently substituting.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from server import prompt_loader, team

PROMPTS_DIR = Path(team.__file__).parent / "prompts"

# Distinctive markers from the two-call synthesis prompts (synthesis-answer + synthesis-indepth);
# used to prove BOTH synthesis prompts actually reach the model.
SYNTH_MARKERS = ("FALSE PREMISE", "DIRECT ANSWER ONLY", "IN-DEPTH elaboration")

ENVELOPE = json.dumps({"answer": "Lisinopril 10 mg [1]", "citations": [1], "blocks": []})
RESP_FORMAT = {"type": "json_schema", "json_schema": {"name": "chart_answer", "schema": {}}}
MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Lisinopril 10 mg"},
    {"role": "user", "content": "What meds is the patient on?"},
]


def _run_team_capturing_synth():
    """Run the team and return the JSON-serialized message arrays sent to the constrained
    synthesis calls — the two turns (Answer + In-Depth) that carry the synthesis prompts.
    `team._chat` is seamed; no model, no HTTP."""
    import asyncio

    captured = {"synths": []}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            captured["synths"].append(messages)
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        asyncio.run(team.run_team(MESSAGES, response_format=RESP_FORMAT, temperature=0.0))
    return json.dumps(captured["synths"])


def test_load_prompt_returns_the_committed_file_text():
    # The loader reads prompts/<name>.txt verbatim (trailing newline stripped) for
    # every name the team references.
    for name in ("orchestrator", "medical_expert", "synthesis", "synthesis-low"):
        on_disk = (PROMPTS_DIR / f"{name}.txt").read_text(encoding="utf-8").rstrip("\n")
        assert on_disk, f"{name}.txt is empty"
        assert prompt_loader.load_prompt(name) == on_disk


def test_missing_prompt_file_fails_loud(tmp_path, monkeypatch):
    # A referenced-but-absent prompt is a configuration bug: raise FileNotFoundError
    # naming the path, never silently substitute.
    monkeypatch.setattr(prompt_loader, "_DIR", tmp_path)  # empty dir: no prompt files
    with pytest.raises(FileNotFoundError) as exc_info:
        prompt_loader.load_prompt("synthesis")
    assert "synthesis" in str(exc_info.value)


def test_load_prompt_reads_the_file_each_call_so_edits_are_live(tmp_path, monkeypatch):
    # A mounted .txt edit must change behaviour with NO restart -> the loader must
    # re-read the file per call, never memoize at import.
    monkeypatch.setattr(prompt_loader, "_DIR", tmp_path)
    f = tmp_path / "synthesis.txt"
    f.write_text("FIRST\n", encoding="utf-8")
    assert prompt_loader.load_prompt("synthesis") == "FIRST"
    f.write_text("SECOND\n", encoding="utf-8")  # edit "the mounted file"
    assert prompt_loader.load_prompt("synthesis") == "SECOND"


def test_synthesis_low_is_a_distinct_prompt():
    # The low level swaps synthesis -> synthesis-low (a synthesis prompt tuned for the
    # smaller synthesizer). It must be its own file, distinct from the default.
    assert prompt_loader.load_prompt("synthesis-low") != prompt_loader.load_prompt("synthesis")


def test_team_sends_the_synthesis_prompts_to_the_model():
    # Behaviour-preservation: the team's two constrained synthesis turns must carry the
    # committed synthesis instructions — the Answer prompt (FALSE PREMISE, DIRECT ANSWER ONLY)
    # and the In-Depth prompt (IN-DEPTH elaboration). Asserts on what the team SENDS (the
    # seam), not the mocked return.
    blob = _run_team_capturing_synth()
    for marker in SYNTH_MARKERS:
        assert marker in blob, f"synthesis marker {marker!r} not sent to the model"


def test_team_passes_through_a_schema_valid_two_section_table_envelope():
    # The synthesis prompt asks for a two-section `answer` + a `blocks` table when the
    # question enumerates. Assert the team PRESERVES such an envelope end-to-end (does
    # not drop or mangle `blocks`) and that it stays structurally valid for the
    # chartsearchai consumer. The synthesis call is seamed to return a realistic envelope.
    import asyncio

    envelope = json.dumps({
        "answer": "**Answer**\nShe is on lamivudine, nevirapine, and stavudine [29], [30], [31].\n\n"
                  "**In Depth**\nThe regimen is stavudine-based [31]; per WHO HIV guidelines "
                  "stavudine is no longer a preferred backbone.",
        "citations": [29, 30, 31],
        "blocks": [{
            "kind": "table",
            "title": "Medications Ordered",
            "columns": [
                {"key": "medication", "label": "Medication"},
                {"key": "action", "label": "Action"},
            ],
            "rows": [
                {"cells": {"medication": {"text": "Lamivudine", "refs": [29]},
                           "action": {"text": "Continue", "refs": []}}},
                {"cells": {"medication": {"text": "Stavudine", "refs": [31]},
                           "action": {"text": "Review", "refs": [31]}}},
            ],
        }],
    })

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            return {"content": envelope}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = asyncio.run(team.run_team(MESSAGES, response_format=RESP_FORMAT, temperature=0.0))

    env_obj = json.loads(out)
    # Structural envelope contract the chartsearchai consumer parses.
    assert set(env_obj.keys()) >= {"answer", "citations", "blocks"}
    assert isinstance(env_obj["citations"], list) and all(isinstance(i, int) for i in env_obj["citations"])
    assert isinstance(env_obj["blocks"], list)
    # The two-section answer survived intact.
    assert "**Answer**" in env_obj["answer"] and "**In Depth**" in env_obj["answer"]
    # The table block survived with its kind/columns/rows and per-cell refs.
    block = env_obj["blocks"][0]
    assert block["kind"] == "table"
    assert [c["key"] for c in block["columns"]] == ["medication", "action"]
    first_cell = block["rows"][0]["cells"]["medication"]
    assert first_cell["text"] == "Lamivudine" and first_cell["refs"] == [29]
