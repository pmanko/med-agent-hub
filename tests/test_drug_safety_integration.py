"""Drug-safety metadata through blocking and staged engine adapters."""

import asyncio
import json
from unittest.mock import patch

from server import engine, team
from tests.factories import (
    make_profile,
    patient_source_registry,
    run_profile,
    stream_profile,
)

QUESTION_MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "Can I add ibuprofen for this patient's pain?"},
]

# Bundled dataset rule: ibuprofen interacts with an active warfarin order (by name token).
_WARFARIN_RECORDS = [
    {
        "resourceType": "drug_order",
        "resourceUuid": "order-1",
        "date": "2025-01-01",
        "metadata": {"drug_name": "Warfarin"},
    },
]

_IBUPROFEN_ENVELOPE = json.dumps(
    {
        "answer": "Ibuprofen could be considered for the patient's pain.",
        "citations": [],
        "blocks": [],
    }
)


def run(coro):
    return asyncio.run(coro)


_PATIENT_SOURCE = patient_source_registry(
    "[1] Active order: warfarin\n",
    [
        {
            "index": 1,
            "resourceType": "drug_order",
            "resourceUuid": "order-1",
            "date": "2025-01-01",
            "text": "Active order: warfarin",
        }
    ],
    _WARFARIN_RECORDS,
)

_WEIGHT_SOURCE = patient_source_registry(
    "Patient: 40-year-old Female\n\n[1] Patient demographics\n[2] (2026-06-20) Weight: 50 kg\n",
    [
        {
            "index": 1,
            "resourceType": "patient",
            "resourceUuid": "patient-1",
            "date": None,
            "text": "Patient demographics",
        },
        {
            "index": 2,
            "resourceType": "obs",
            "resourceUuid": "weight-1",
            "date": "2026-06-20",
            "text": "Weight: 50 kg",
        }
    ],
    [
        {
            "resourceType": "patient",
            "resourceUuid": "patient-1",
            "date": None,
            "metadata": {"age_years": 40},
        },
        {
            "resourceType": "obs",
            "resourceUuid": "weight-1",
            "date": "2026-06-20",
            "metadata": {
                "concept_uuid": "5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "value_numeric": 50.0,
                "units": "kg",
            },
        }
    ],
)


async def _fake_chat_ibuprofen_answer(
    client,
    model,
    messages,
    *,
    tools=None,
    response_format=None,
    temperature=None,
    max_tokens=None,
    **kwargs,
):
    if response_format is not None:
        return {"content": _IBUPROFEN_ENVELOPE}
    return {"content": "ok", "tool_calls": None}


def _answer_profile(*, drug_safety=False):
    return make_profile(
        topology="single",
        stages=("context", "answer", "gate"),
        models={"answer": "writer"},
        prompts={"answer": "synthesis-answer"},
        output="bare",
        policies={"drug_safety": drug_safety},
    )


def _product_profile(*, drug_safety=False):
    return make_profile(
        topology="single",
        stages=(
            "context",
            "answer",
            "gate",
            "resolve_refs",
            "final_resolve_refs",
            "ground_verdicts",
            "indepth",
            "indepth_gate",
        ),
        models={"answer": "writer", "grounding": "writer", "indepth": "writer"},
        prompts={"answer": "synthesis-answer", "indepth": "synthesis-indepth"},
        output="product",
        policies={"drug_safety": drug_safety},
    )


def test_profile_drain_attaches_safety_warnings_when_enabled():
    with patch.object(team, "_chat", side_effect=_fake_chat_ibuprofen_answer):
        out = run(
            run_profile(
                _answer_profile(drug_safety=True),
                QUESTION_MESSAGES,
                response_format={"type": "json_schema"},
                temperature=0.0,
                max_tokens=1024,
                patient="patient-1",
                source_registry=_PATIENT_SOURCE,
            )
        )
    env = json.loads(out)
    assert env["safetyWarnings"] == [
        {
            "type": "interaction",
            "drug": "Ibuprofen",
            "detail": "interacts with active order warfarin — increased risk of GI bleeding",
        },
    ]


def test_profile_drain_uses_fresh_querystore_weight_for_per_dose_limit():
    async def fake_weighted_answer(*_args, **_kwargs):
        return {
            "content": json.dumps({
                "answer": "Ibuprofen 600 mg every 8 hours can be given.",
                "citations": [],
                "blocks": [],
            })
        }

    with patch.object(team, "_chat", side_effect=fake_weighted_answer):
        out = run(
            run_profile(
                _answer_profile(drug_safety=True),
                QUESTION_MESSAGES,
                response_format={"type": "json_schema"},
                temperature=0.0,
                max_tokens=1024,
                patient="patient-1",
                source_registry=_WEIGHT_SOURCE,
            )
        )

    warnings = json.loads(out)["safetyWarnings"]
    assert len(warnings) == 1
    assert warnings[0]["type"] == "overdose"
    assert "10 mg/kg" in warnings[0]["detail"]
    assert "50 kg" in warnings[0]["detail"]


def test_profile_drain_omits_safety_warnings_key_when_disabled_default():
    with patch.object(team, "_chat", side_effect=_fake_chat_ibuprofen_answer):
        out = run(
            run_profile(
                _answer_profile(),
                QUESTION_MESSAGES,
                response_format={"type": "json_schema"},
                temperature=0.0,
                max_tokens=1024,
                patient="patient-1",
                source_registry=_PATIENT_SOURCE,
            )
        )
    env = json.loads(out)
    assert "safetyWarnings" not in env


def test_profile_stream_done_event_carries_safety_warnings():
    async def fake_synthesize_answer(
        client,
        model,
        base_messages,
        instruction,
        gathered,
        *,
        response_format=None,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry=None,
    ):
        return "Ibuprofen could be considered for the patient's pain.", [], []

    async def fake_gen_indepth(*_a, **_k):
        return ([], {"level": "green", "note": ""})

    async def fake_ground(_client, _model, _answer, references, _mappings):
        return references

    def fake_gate(**k):
        return (
            k["answer_text"],
            k["citations"],
            k["blocks"],
            {"mode": "off", "status": "ok", "applied": "none"},
            None,
        )

    async def _collect():
        events = []
        with patch.object(
            team, "_synthesize_answer", side_effect=fake_synthesize_answer
        ), patch.object(
            team, "_gen_indepth", side_effect=fake_gen_indepth
        ), patch.object(
            team, "_ground_references", side_effect=fake_ground
        ), patch.object(
            team, "_apply_temporal_gate", side_effect=fake_gate
        ), patch.object(
            team, "_write_trace", lambda *_a, **_k: None
        ):
            async for name, data in stream_profile(
                _product_profile(drug_safety=True),
                QUESTION_MESSAGES,
                patient="patient-1",
                source_registry=_PATIENT_SOURCE,
            ):
                events.append((name, json.loads(data) if data else {}))
        return events

    events = run(_collect())
    by_name = dict(events)
    assert by_name["done"]["safetyWarnings"] == [
        {
            "type": "interaction",
            "drug": "Ibuprofen",
            "detail": "interacts with active order warfarin — increased risk of GI bleeding",
        },
    ]
    # Deterministic safety checks are available with the fast answer and persist to done.
    assert by_name["answer_done"]["safetyWarnings"] == by_name["done"]["safetyWarnings"]


def test_profile_stream_omits_safety_warnings_when_disabled_default():
    async def fake_synthesize_answer(
        client,
        model,
        base_messages,
        instruction,
        gathered,
        *,
        response_format=None,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry=None,
    ):
        return "Ibuprofen could be considered for the patient's pain.", [], []

    async def fake_gen_indepth(*_a, **_k):
        return ([], {"level": "green", "note": ""})

    def fake_gate(**k):
        return (
            k["answer_text"],
            k["citations"],
            k["blocks"],
            {"mode": "off", "status": "ok", "applied": "none"},
            None,
        )

    async def _collect():
        events = []
        with patch.object(
            team, "_synthesize_answer", side_effect=fake_synthesize_answer
        ), patch.object(
            team, "_gen_indepth", side_effect=fake_gen_indepth
        ), patch.object(
            team, "_apply_temporal_gate", side_effect=fake_gate
        ), patch.object(
            team, "_write_trace", lambda *_a, **_k: None
        ):
            async for name, data in stream_profile(
                _product_profile(),
                QUESTION_MESSAGES,
                patient="patient-1",
                source_registry=_PATIENT_SOURCE,
            ):
                events.append((name, json.loads(data) if data else {}))
        return events

    events = run(_collect())
    by_name = dict(events)
    assert "safetyWarnings" not in by_name["done"]


def test_stage_engine_drain_carries_safety_warnings_through():
    async def fake_stream(request, _budget_policy=None):
        assert request.profile.policies["drug_safety"] is True
        yield (
            "done",
            json.dumps(
                {
                    "answer": "Ibuprofen could help.",
                    "references": [],
                    "blocks": [],
                    "safetyWarnings": [
                        {
                            "type": "interaction",
                            "drug": "Ibuprofen",
                            "detail": "interacts with active order warfarin — increased risk of GI bleeding",
                        }
                    ],
                }
            ),
        )

    with patch.object(engine, "_execute_stages", new=fake_stream):
        out = run(run_profile(_product_profile(drug_safety=True), QUESTION_MESSAGES))
    env = json.loads(out)
    assert env["safetyWarnings"] == [
        {
            "type": "interaction",
            "drug": "Ibuprofen",
            "detail": "interacts with active order warfarin — increased risk of GI bleeding",
        },
    ]
