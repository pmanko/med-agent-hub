"""Byte-level output contracts captured before the stage-engine consolidation."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from server import engine, team
from server.levels_loader import get_profile

GOLDENS = Path(__file__).parent / "goldens"
ANSWER = json.dumps({"answer": "Lisinopril 10 mg [1]", "citations": [1], "blocks": []})
IN_DEPTH = json.dumps(
    {
        "claims": [
            "Per WHO guidance, start ART promptly after diagnosis.",
            "Monitor CD4 roughly every 6 months on stable therapy.",
        ]
    }
)
REVIEW_VERDICT = json.dumps({"answer_ok": True, "errors": [], "corrected_answer": ""})
RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {"name": "chart_answer", "schema": {}},
}
MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Lisinopril 10 mg"},
    {"role": "user", "content": "What meds is the patient on?"},
]
REVIEW_MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Aspirin active"},
    {"role": "assistant", "content": "The patient is taking aspirin [1]."},
    {
        "role": "user",
        "content": "Review this answer:\n```json\n"
        + json.dumps(
            {
                "schema_version": "answer_to_review.v1",
                "original_question": "What medications?",
                "answer": "The patient is taking aspirin [1].",
                "citations": [1],
                "blocks": [],
            }
        )
        + "\n```",
    },
]


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 7, 9, 12, 0, 0, tzinfo=tz)


async def _fake_chat(
    _client,
    _model,
    _messages,
    *,
    tools=None,
    response_format=None,
    **_kwargs,
):
    if response_format is None:
        return {"content": "ok", "tool_calls": None}
    schema = (response_format.get("json_schema") or {}).get("name")
    if schema == "rewrite_verdict":
        return {"content": REVIEW_VERDICT}
    return {"content": IN_DEPTH if schema == "in_depth" else ANSWER}


def _run(profile_id, *, messages=MESSAGES) -> str:
    with patch.object(team, "_chat", side_effect=_fake_chat), patch.object(
        team, "_write_trace"
    ), patch.object(team, "datetime", _FixedDateTime):
        return asyncio.run(
            engine.drain_profile(
                engine.ExecutionRequest(
                    profile=get_profile(profile_id),
                    messages=messages,
                    response_format=RESPONSE_FORMAT,
                    temperature=0.0,
                    max_tokens=1024,
                    context={"temporal": False},
                )
            )
        )


@pytest.mark.parametrize(
    ("golden", "profile_id", "messages"),
    [
        (
            "raw-answer.json",
            "answer:gemma-4-12b@synthesis-chartsearchai~off~temp0",
            MESSAGES,
        ),
        (
            "raw-indepth-only.json",
            "indepth-only:gemma-4-12b@synthesis-indepth~temp0",
            MESSAGES + [{"role": "assistant", "content": "Lisinopril 10 mg [1]"}],
        ),
        (
            "raw-answer-review.json",
            "answer-review:gemma-4-12b@validation-rewrite~enforce~temp0",
            REVIEW_MESSAGES,
        ),
        (
            "legacy-parity.json",
            "med-agent-team-parity",
            MESSAGES,
        ),
        ("legacy-two-call.json", "med-agent-team-low", MESSAGES),
    ],
)
def test_pre_refactor_envelopes_remain_byte_exact(golden, profile_id, messages):
    expected = (GOLDENS / golden).read_text(encoding="utf-8").rstrip("\n")

    assert _run(profile_id, messages=messages) == expected
