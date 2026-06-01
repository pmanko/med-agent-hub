"""
Prompt-loading tests (server/prompts.py + its wiring into server/team.py).

The team's three system prompts are read from files per request, selected by the
PROMPT_VARIANT env var, so a prompt edit changes behaviour with no rebuild and an
A/B can run two variants side by side. These tests assert the MECHANISM — which
prompt TEXT the team actually loads and sends — not the model's output (that is
what the harness A/B run measures). `team._chat` is seamed: no model, no HTTP.

v2 is a one-file delta: only synthesis.txt differs; orchestrator/medical_expert
fall back to v1, which is what isolates the synthesis prompt as the single A/B
variable. v1 must stay byte-identical to the original module constants.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

from server import prompt_loader, team

PROMPTS_DIR = Path(team.__file__).parent / "prompts"

# v2-only marker strings (verified absent in v1/synthesis.txt).
V2_MARKERS = ("**In Depth**", "FALSE PREMISE")

ENVELOPE = json.dumps({"answer": "Lisinopril 10 mg [1]", "citations": [1], "blocks": []})
RESP_FORMAT = {"type": "json_schema", "json_schema": {"name": "chart_answer", "schema": {}}}
MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Lisinopril 10 mg"},
    {"role": "user", "content": "What meds is the patient on?"},
]


def _run_team_capturing_synth(variant):
    """Run the team with PROMPT_VARIANT=`variant` (None -> unset), returning the
    JSON-serialized message array sent to the constrained synthesis call — the turn
    that carries the synthesis prompt. `team._chat` is seamed; no model, no HTTP."""
    import asyncio

    captured = {}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None):
        if response_format is not None:
            captured["synth"] = messages
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    env = {k: v for k, v in os.environ.items() if k != "PROMPT_VARIANT"}
    if variant is not None:
        env["PROMPT_VARIANT"] = variant
    with patch.dict(os.environ, env, clear=True), \
            patch.object(team, "_chat", side_effect=fake_chat):
        asyncio.run(team.run_team(MESSAGES, response_format=RESP_FORMAT, temperature=0.0))
    return json.dumps(captured["synth"])


def test_default_variant_is_v1():
    # No PROMPT_VARIANT set -> v1 (a missing env var must never 500 or pick v2).
    env = {k: v for k, v in os.environ.items() if k != "PROMPT_VARIANT"}
    with patch.dict(os.environ, env, clear=True):
        assert prompt_loader.load_prompt("synthesis") == \
            (PROMPTS_DIR / "v1" / "synthesis.txt").read_text(encoding="utf-8").rstrip("\n")


def test_load_prompt_v1_is_byte_identical_to_the_baked_fallback():
    # v1 is the TRUE control: the .txt files must match the baked-in constants the
    # team used before the prompts were file-backed, so the A/B is unconfounded.
    with patch.dict(os.environ, {"PROMPT_VARIANT": "v1"}):
        for name in ("orchestrator", "medical_expert", "synthesis"):
            on_disk = (PROMPTS_DIR / "v1" / f"{name}.txt").read_text(encoding="utf-8").rstrip("\n")
            assert prompt_loader.load_prompt(name) == on_disk
            assert prompt_loader.load_prompt(name) == prompt_loader._FALLBACK[name]


def test_v2_overrides_only_synthesis_others_fall_back_to_v1():
    # v2/ ships ONLY synthesis.txt. The single-variable A/B requires orchestrator
    # and medical_expert to resolve to their v1 text under PROMPT_VARIANT=v2.
    with patch.dict(os.environ, {"PROMPT_VARIANT": "v2"}):
        synth = prompt_loader.load_prompt("synthesis")
        orch = prompt_loader.load_prompt("orchestrator")
        med = prompt_loader.load_prompt("medical_expert")
    v2_synth = (PROMPTS_DIR / "v2" / "synthesis.txt").read_text(encoding="utf-8").rstrip("\n")
    v1_synth = (PROMPTS_DIR / "v1" / "synthesis.txt").read_text(encoding="utf-8").rstrip("\n")
    v1_orch = (PROMPTS_DIR / "v1" / "orchestrator.txt").read_text(encoding="utf-8").rstrip("\n")
    v1_med = (PROMPTS_DIR / "v1" / "medical_expert.txt").read_text(encoding="utf-8").rstrip("\n")
    assert synth == v2_synth and synth != v1_synth
    assert all(m in synth for m in V2_MARKERS)
    assert orch == v1_orch  # fell back to v1 (no v2/orchestrator.txt)
    assert med == v1_med    # fell back to v1 (no v2/medical_expert.txt)


def test_unknown_variant_falls_back_to_v1():
    # A typo in PROMPT_VARIANT must degrade to v1, not error.
    with patch.dict(os.environ, {"PROMPT_VARIANT": "does-not-exist"}):
        assert prompt_loader.load_prompt("synthesis") == \
            (PROMPTS_DIR / "v1" / "synthesis.txt").read_text(encoding="utf-8").rstrip("\n")


def test_missing_file_falls_back_to_baked_constant(tmp_path, monkeypatch):
    # Last-resort safety: if even the v1 file is unreadable, the baked-in constant
    # is returned so a deleted/renamed prompt file never 500s the team.
    monkeypatch.setattr(prompt_loader, "_DIR", tmp_path)  # empty dir: no variant files
    with patch.dict(os.environ, {"PROMPT_VARIANT": "v1"}):
        assert prompt_loader.load_prompt("synthesis") == prompt_loader._FALLBACK["synthesis"]
        assert prompt_loader.load_prompt("orchestrator") == prompt_loader._FALLBACK["orchestrator"]


def test_load_prompt_reads_the_file_each_call_so_edits_are_live(tmp_path, monkeypatch):
    # A mounted .txt edit must change behaviour with NO restart -> the loader must
    # re-read the file per call, never memoize at import.
    monkeypatch.setattr(prompt_loader, "_DIR", tmp_path)
    vdir = tmp_path / "v1"
    vdir.mkdir()
    f = vdir / "synthesis.txt"
    f.write_text("FIRST\n", encoding="utf-8")
    with patch.dict(os.environ, {"PROMPT_VARIANT": "v1"}):
        assert prompt_loader.load_prompt("synthesis") == "FIRST"
        f.write_text("SECOND\n", encoding="utf-8")  # edit "the mounted file"
        assert prompt_loader.load_prompt("synthesis") == "SECOND"


def test_team_synthesis_uses_v2_text_when_variant_is_v2():
    # Under PROMPT_VARIANT=v2 the constrained synthesis turn must carry the v2
    # synthesis instruction (two-section + false-premise deltas), not the v1 text.
    # Asserts on what the team SENDS (the seam), not the mocked return.
    blob = _run_team_capturing_synth("v2")
    for marker in V2_MARKERS:
        assert marker in blob, f"v2 synthesis marker {marker!r} not sent to the model"


def test_team_synthesis_uses_v1_text_by_default():
    # Default (no variant) must send v1 synthesis text — v2-only markers ABSENT.
    # Locks v1 behaviour: file-backing the prompt changed nothing for the control.
    blob = _run_team_capturing_synth(None)
    for marker in V2_MARKERS:
        assert marker not in blob, f"v2 marker {marker!r} leaked into the v1/default synthesis"
    # And the v1 synthesis instruction text actually reached synthesis (a distinctive
    # v1 tail phrase that is NOT present in v2).
    assert "Keep the answer concise." in blob


def test_team_passes_through_a_schema_valid_two_section_table_envelope_under_v2():
    # The v2 prompt asks for a two-section `answer` + a `blocks` table when the
    # question enumerates. The model's compliance is the harness A/B's measurement;
    # here we assert the team PRESERVES such an envelope end-to-end (does not drop or
    # mangle `blocks`) and that it stays structurally valid for the consumer. The
    # synthesis call is seamed to return a realistic v2-shaped envelope.
    import asyncio

    v2_envelope = json.dumps({
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
                        temperature=None, max_tokens=None):
        if response_format is not None:
            return {"content": v2_envelope}
        return {"content": "ok", "tool_calls": None}

    env = {k: v for k, v in os.environ.items() if k != "PROMPT_VARIANT"}
    env["PROMPT_VARIANT"] = "v2"
    with patch.dict(os.environ, env, clear=True), \
            patch.object(team, "_chat", side_effect=fake_chat):
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
