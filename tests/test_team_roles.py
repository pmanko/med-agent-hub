"""Unit test for the team role split: the orchestrator tool loop and the final
synthesis use SEPARATE models — a fast small model runs the chatty loop while a
larger model composes the answer. Mocks the model boundary (`_chat`) and
asserts which model each compiled stage targets
(not a reimplementation). Run: `pytest tests/test_team_roles.py`.
"""

import asyncio
import json

import pytest

from server import config, team
from tests.factories import run_profile, team_profile


def _fake_chat_factory(calls):
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
        calls.append((model, response_format is not None))
        if response_format is not None:  # synthesis turn (schema-bound)
            return {
                "content": json.dumps({"answer": "ok", "citations": [], "blocks": []})
            }
        return {
            "content": "",
            "tool_calls": None,
        }  # loop turn: no tool calls -> break to synthesis

    return fake_chat


_MESSAGES = [
    {"role": "system", "content": "envelope system"},
    {"role": "user", "content": "patient chart"},
    {"role": "user", "content": "the question"},
]
_RF = {"type": "json_schema", "json_schema": {"name": "chart_answer"}}


def test_synthesis_uses_synthesizer_model_loop_uses_orchestrator(monkeypatch):
    """Explicit profile roles: the gather loop uses the orchestrator and answer
    synthesis uses the writer declared by the compiled stage plan."""
    calls = []
    monkeypatch.setattr(team, "_chat", _fake_chat_factory(calls))
    profile = team_profile(
        orchestrator="ORCH-MODEL",
        expert=team.llm_config.med_model,
        answer="SYNTH-MODEL",
        indepth="SYNTH-MODEL",
        output="combined",
    )
    out = asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF))
    loop_models = [m for (m, is_synth) in calls if not is_synth]
    synth_models = [m for (m, is_synth) in calls if is_synth]
    assert loop_models and all(m == "ORCH-MODEL" for m in loop_models), calls
    # Two-call synthesis (Answer + In-Depth) — both run on the synthesizer model.
    assert synth_models and all(m == "SYNTH-MODEL" for m in synth_models), calls
    assert "ok" in json.loads(out)["answer"]


def test_synthesis_applies_anti_degeneration_params(monkeypatch):
    """The synthesis call gets a repeat penalty + a temperature floor (breaks the
    small synth's repetition loop); the orchestrator loop gets neither, so its
    tool-calling stays at the request temperature. Red without the fix: synth's
    repeat_penalty would be None and its temperature would be the request's 0.0."""
    seen = []

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
        seen.append(
            {
                "synth": response_format is not None,
                "temperature": temperature,
                "repeat_penalty": repeat_penalty,
            }
        )
        if response_format is not None:
            return {
                "content": json.dumps({"answer": "ok", "citations": [], "blocks": []})
            }
        return {"content": "", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)
    profile = team_profile(
        orchestrator="ORCH-MODEL",
        expert=team.llm_config.med_model,
        answer="SYNTH-MODEL",
        indepth="SYNTH-MODEL",
        output="combined",
    )
    asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF, temperature=0.0))
    synth = [c for c in seen if c["synth"]]
    loop = [c for c in seen if not c["synth"]]
    assert synth and synth[0]["repeat_penalty"] == config.SYNTH_REPEAT_PENALTY, seen
    assert synth[0]["temperature"] >= team._SYNTH_MIN_TEMPERATURE, seen
    assert loop and all(c["repeat_penalty"] is None for c in loop), seen


def test_synthesis_reads_reasoning_content_when_content_empty(monkeypatch):
    """A reasoning synthesizer (Qwen 3.x via LM Studio MLX) returns the structured
    envelope in `reasoning_content` and leaves `content` empty. The synth must read it
    instead of falling back. Red without the fix: empty content makes the profile return the
    'I could not produce a complete answer' fallback, so out['answer'] != the real answer.
    """

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
        if (
            response_format is not None
        ):  # synthesis turn: answer hidden in reasoning_content
            return {
                "content": "",
                "reasoning_content": json.dumps(
                    {"answer": "qwen-ok", "citations": [], "blocks": []}
                ),
            }
        return {
            "content": "",
            "tool_calls": None,
        }  # loop turn: no tools -> straight to synthesis

    monkeypatch.setattr(team, "_chat", fake_chat)
    profile = team_profile(
        orchestrator="ORCH-MODEL",
        expert=team.llm_config.med_model,
        answer="SYNTH-MODEL",
        indepth="SYNTH-MODEL",
        output="combined",
    )
    out = asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF))
    assert "qwen-ok" in json.loads(out)["answer"], out


def test_synthesis_normalizes_literal_newline_and_reconciles_citations(monkeypatch):
    """Post-process the synth envelope: a literal backslash-n in `answer` becomes a real
    newline (small models, e.g. qwen3-14b, copy the prompt's JSON \\n escaping verbatim
    and garble), and inline [N] chart-record markers are reconciled into `citations` so
    the count is not lost. Red without _normalize_envelope: the literal \\n survives and
    citations stays []."""
    literal = (
        "**Answer**" + "\\n" + "Regimen is outdated [29], [30]."
    )  # literal backslash-n

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
        if response_format is not None:
            return {
                "content": json.dumps(
                    {"answer": literal, "citations": [], "blocks": []}
                )
            }
        return {"content": "", "tool_calls": None}

    monkeypatch.setattr(team, "_chat", fake_chat)
    profile = team_profile(
        orchestrator=team.llm_config.orchestrator_model,
        expert=team.llm_config.med_model,
        answer="S",
        indepth="S",
        output="combined",
    )
    env = json.loads(asyncio.run(run_profile(profile, _MESSAGES, response_format=_RF)))
    assert "\\n" not in env["answer"], env[
        "answer"
    ]  # literal backslash-n normalized away
    assert "\n" in env["answer"], env["answer"]  # to a real newline
    assert env["citations"] == [29, 30], env  # inline [N] reconciled into citations


def test_normalize_envelope_strips_backslash_run_artifacts():
    """qwen2.5-14b mis-escapes the section line breaks as RUNS of backslashes
    (e.g. "**Answer**\\\\\\: text", "**Answer**\\\\\\\\<newline>This"), which render as
    literal backslashes. _normalize_envelope must collapse those runs to a clean line
    break. Red without the run-collapse: the backslashes survive (only a single \\n was
    handled before)."""
    raw = json.dumps(
        {
            "answer": "**Answer**"
            + "\\" * 6
            + ": The patient is on lamivudine [3]."
            + "\\" * 4
            + "\n**In Depth**"
            + "\\" * 3
            + "Per WHO guidance, the regimen is outdated.",
            "citations": [],
            "blocks": [],
        }
    )
    out = json.loads(team._normalize_envelope(raw))
    assert "\\" not in out["answer"], repr(
        out["answer"]
    )  # no backslash artifacts remain
    assert "**Answer**" in out["answer"]  # header preserved
    assert "**In Depth**" in out["answer"]
    assert "The patient is on lamivudine" in out["answer"]  # content preserved
    assert out["citations"] == [3]  # inline [N] reconcile still works


def test_product_citation_contract_uses_explicit_markers_over_declared_extras():
    answer, citations, issues = team._enforce_product_citation_contract(
        "The documented visit was on 2006-06-06 [4].",
        [4, 1, 2, 3, 5],
        [],
    )

    assert answer == "The documented visit was on 2006-06-06 [4]."
    assert citations == [4]
    assert issues == []


def test_product_citation_contract_scopes_one_declared_source_to_prose():
    answer, citations, issues = team._enforce_product_citation_contract(
        "The documented visit was on 2006-06-06.", [4], []
    )

    assert answer == "The documented visit was on 2006-06-06 [4]."
    assert citations == [4]
    assert issues == []


def test_product_citation_contract_does_not_scope_one_source_over_two_claims():
    answer, citations, issues = team._enforce_product_citation_contract(
        "The visit was on 2006-06-06. The weight was 71 kg.", [4], []
    )

    assert answer == "The visit was on 2006-06-06. The weight was 71 kg."
    assert citations == [4]
    assert issues[0]["id"] == "citation_scope"


def test_product_citation_contract_scopes_multi_source_set_to_one_prose_claim():
    answer, citations, issues = team._enforce_product_citation_contract(
        "The documented visit was on 2006-06-06.", [4, 1, 2], []
    )

    assert answer == "The documented visit was on 2006-06-06 [4][1][2]."
    assert citations == [4, 1, 2]
    assert issues == []


@pytest.mark.parametrize(
    "answer",
    (
        "The visit was on 2006-06-06. The weight was 71 kg.",
        "The visit was on 2006-06-06; the weight was 71 kg.",
        "The visit was on 2006-06-06, and the weight was 71 kg.",
        "The visit was on 2006-06-06, but the weight was 71 kg.",
        "The visit was on 2006-06-06, for the weight was 71 kg.",
        "The visit was on 2006-06-06, nor was the weight 71 kg.",
        "The visit was on 2006-06-06, or the weight was 71 kg.",
        "The visit was on 2006-06-06, so the weight was 71 kg.",
        "The visit was on 2006-06-06, yet the weight was 71 kg.",
    ),
)
def test_product_citation_contract_blocks_multi_source_set_over_two_claims(answer):
    answer, citations, issues = team._enforce_product_citation_contract(
        answer, [4, 1, 2], []
    )

    assert citations == [4, 1, 2]
    assert issues[0]["id"] == "citation_scope"
    assert issues[0]["severity"] == "block"


def test_product_table_contract_repairs_alternating_cells_by_declared_columns():
    blocks = [
        {
            "kind": "table",
            "title": "Weights",
            "columns": [
                {"key": "date", "label": "Date"},
                {"key": "weight", "label": "Weight"},
            ],
            "rows": [
                {"cells": {"refs": {"text": "2026-01-01", "refs": [7]}}},
                {"cells": {"refs": {"text": "71.0 kg", "refs": [7]}}},
                {"cells": {"refs": {"text": "2026-02-01", "refs": [8]}}},
                {"cells": {"refs": {"text": "70.0 kg", "refs": [8]}}},
            ],
        }
    ]

    normalized, issues = team._normalize_product_blocks(blocks)

    assert issues == []
    assert normalized[0]["rows"] == [
        {
            "cells": {
                "date": {"text": "2026-01-01", "refs": [7]},
                "weight": {"text": "71.0 kg", "refs": [7]},
            }
        },
        {
            "cells": {
                "date": {"text": "2026-02-01", "refs": [8]},
                "weight": {"text": "70.0 kg", "refs": [8]},
            }
        },
    ]


def test_product_table_contract_drops_an_unrepairable_block():
    blocks = [
        {
            "kind": "table",
            "title": "Weights",
            "columns": [
                {"key": "date", "label": "Date"},
                {"key": "weight", "label": "Weight"},
            ],
            "rows": [
                {"cells": {"unexpected": {"text": "2026-01-01", "refs": [7]}}}
            ],
        }
    ]

    normalized, issues = team._normalize_product_blocks(blocks)

    assert normalized == []
    assert issues[0]["id"] == "table_contract"
    assert issues[0]["severity"] == "block"


def test_product_table_contract_does_not_guess_reversed_flat_cell_order():
    blocks = [
        {
            "kind": "table",
            "title": "Weights",
            "columns": [
                {"key": "date", "label": "Date"},
                {"key": "weight", "label": "Weight"},
            ],
            "rows": [
                {"cells": {"refs": {"text": "71.0 kg", "refs": [7]}}},
                {"cells": {"refs": {"text": "2026-01-01", "refs": [7]}}},
            ],
        }
    ]

    normalized, issues = team._normalize_product_blocks(blocks)

    assert normalized == []
    assert issues[0]["id"] == "table_contract"
