"""Integration tests for H7's wiring — that `drug_safety=True` actually threads through
run_team (batch), run_team_stream (staged), and run_team_stage_drain to the right envelope keys,
and that `drug_safety=False` (the default on every existing level) leaves envelopes byte-identical
(no `safetyWarnings` key at all). The validator/injector ALGORITHM itself is covered by
test_drug_safety.py; this file only exercises the plumbing.
"""

import asyncio
import json
from unittest.mock import patch

from server import team

QUESTION_MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "Can I add ibuprofen for this patient's pain?"},
]

# Bundled dataset rule: ibuprofen interacts with an active warfarin order (by name token).
_WARFARIN_RECORDS = [
    {"resourceType": "drug_order", "resourceUuid": "order-1", "date": "2025-01-01",
     "metadata": {"drug_name": "Warfarin"}},
]

_IBUPROFEN_ENVELOPE = json.dumps(
    {"answer": "Ibuprofen could be considered for the patient's pain.", "citations": [], "blocks": []})


def run(coro):
    return asyncio.run(coro)


async def _fake_retrieve_with_warfarin(_patient):
    return "[1] Active order: warfarin\n", [
        {"index": 1, "resourceType": "drug_order", "resourceUuid": "order-1", "date": "2025-01-01",
         "text": "Active order: warfarin"},
    ], _WARFARIN_RECORDS


async def _fake_chat_ibuprofen_answer(client, model, messages, *, tools=None, response_format=None,
                                       temperature=None, max_tokens=None, **kwargs):
    if response_format is not None:
        return {"content": _IBUPROFEN_ENVELOPE}
    return {"content": "ok", "tool_calls": None}


def test_run_team_attaches_safety_warnings_when_enabled():
    with patch.object(team, "_retrieve_chart", side_effect=_fake_retrieve_with_warfarin), \
         patch.object(team, "_chat", side_effect=_fake_chat_ibuprofen_answer):
        out = run(team.run_team(
            QUESTION_MESSAGES, response_format={"type": "json_schema"}, temperature=0.0, max_tokens=1024,
            solo=True, validator_model=None, patient="patient-1", drug_safety=True))
    env = json.loads(out)
    assert env["safetyWarnings"] == [
        {"type": "interaction", "drug": "Ibuprofen",
         "detail": "interacts with active order warfarin — increased risk of GI bleeding"},
    ]


def test_run_team_omits_safety_warnings_key_when_disabled_default():
    with patch.object(team, "_retrieve_chart", side_effect=_fake_retrieve_with_warfarin), \
         patch.object(team, "_chat", side_effect=_fake_chat_ibuprofen_answer):
        out = run(team.run_team(
            QUESTION_MESSAGES, response_format={"type": "json_schema"}, temperature=0.0, max_tokens=1024,
            solo=True, validator_model=None, patient="patient-1"))
    env = json.loads(out)
    assert "safetyWarnings" not in env


def test_run_team_stream_done_event_carries_safety_warnings():
    async def fake_synthesize_answer(client, model, base_messages, instruction, gathered, *,
                                      response_format=None, temperature=None, max_tokens=None,
                                      repeat_penalty=None, dry=None):
        return "Ibuprofen could be considered for the patient's pain.", [], []

    async def fake_gen_indepth(*_a, **_k):
        return ([], {"level": "green", "note": ""})

    async def fake_ground(_client, _model, _answer, references, _mappings):
        return references

    def fake_gate(**k):
        return (k["answer_text"], k["citations"], k["blocks"],
                {"mode": "off", "status": "ok", "applied": "none"}, None)

    async def _collect():
        events = []
        with patch.object(team, "_retrieve_chart", side_effect=_fake_retrieve_with_warfarin), \
             patch.object(team, "_synthesize_answer", side_effect=fake_synthesize_answer), \
             patch.object(team, "_gen_indepth", side_effect=fake_gen_indepth), \
             patch.object(team, "_ground_references", side_effect=fake_ground), \
             patch.object(team, "_apply_temporal_gate", side_effect=fake_gate), \
             patch.object(team, "_write_trace", lambda *_a, **_k: None):
            async for name, data in team.run_team_stream(
                    QUESTION_MESSAGES, synth_model="writer", patient="patient-1", drug_safety=True):
                events.append((name, json.loads(data) if data else {}))
        return events

    events = run(_collect())
    by_name = dict(events)
    assert by_name["done"]["safetyWarnings"] == [
        {"type": "interaction", "drug": "Ibuprofen",
         "detail": "interacts with active order warfarin — increased risk of GI bleeding"},
    ]
    # Not yet computed at answer_done (fires before grounding/the safety check).
    assert "safetyWarnings" not in by_name["answer_done"]


def test_run_team_stream_omits_safety_warnings_when_disabled_default():
    async def fake_synthesize_answer(client, model, base_messages, instruction, gathered, *,
                                      response_format=None, temperature=None, max_tokens=None,
                                      repeat_penalty=None, dry=None):
        return "Ibuprofen could be considered for the patient's pain.", [], []

    async def fake_gen_indepth(*_a, **_k):
        return ([], {"level": "green", "note": ""})

    def fake_gate(**k):
        return (k["answer_text"], k["citations"], k["blocks"],
                {"mode": "off", "status": "ok", "applied": "none"}, None)

    async def _collect():
        events = []
        with patch.object(team, "_retrieve_chart", side_effect=_fake_retrieve_with_warfarin), \
             patch.object(team, "_synthesize_answer", side_effect=fake_synthesize_answer), \
             patch.object(team, "_gen_indepth", side_effect=fake_gen_indepth), \
             patch.object(team, "_apply_temporal_gate", side_effect=fake_gate), \
             patch.object(team, "_write_trace", lambda *_a, **_k: None):
            async for name, data in team.run_team_stream(
                    QUESTION_MESSAGES, synth_model="writer", patient="patient-1"):
                events.append((name, json.loads(data) if data else {}))
        return events

    events = run(_collect())
    by_name = dict(events)
    assert "safetyWarnings" not in by_name["done"]


def test_run_team_stage_drain_carries_safety_warnings_through():
    async def fake_stream(**kwargs):
        assert kwargs.get("drug_safety") is True
        yield ("done", json.dumps({
            "answer": "Ibuprofen could help.",
            "references": [],
            "blocks": [],
            "safetyWarnings": [{"type": "interaction", "drug": "Ibuprofen",
                                 "detail": "interacts with active order warfarin — increased risk of GI bleeding"}],
        }))

    with patch.object(team, "run_team_stream", side_effect=fake_stream):
        out = run(team.run_team_stage_drain(drug_safety=True))
    env = json.loads(out)
    assert env["safetyWarnings"] == [
        {"type": "interaction", "drug": "Ibuprofen", "detail": "interacts with active order warfarin — increased risk of GI bleeding"},
    ]
