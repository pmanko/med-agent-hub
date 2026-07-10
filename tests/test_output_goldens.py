"""Byte-level output contracts captured before the stage-engine consolidation."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from server import team


GOLDENS = Path(__file__).parent / "goldens"
ANSWER = json.dumps(
    {"answer": "Lisinopril 10 mg [1]", "citations": [1], "blocks": []}
)
IN_DEPTH = json.dumps(
    {
        "claims": [
            "Per WHO guidance, start ART promptly after diagnosis.",
            "Monitor CD4 roughly every 6 months on stable therapy.",
        ]
    }
)
RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {"name": "chart_answer", "schema": {}},
}
MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Lisinopril 10 mg"},
    {"role": "user", "content": "What meds is the patient on?"},
]


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
    return {"content": IN_DEPTH if schema == "in_depth" else ANSWER}


def _run(**kwargs) -> str:
    with patch.object(team, "_chat", side_effect=_fake_chat), patch.object(
        team, "_write_trace"
    ):
        return asyncio.run(
            team.run_team(
                kwargs.pop("messages", MESSAGES),
                response_format=RESPONSE_FORMAT,
                temperature=0.0,
                max_tokens=1024,
                validator_model=None,
                context={"temporal": False},
                **kwargs,
            )
        )


@pytest.mark.parametrize(
    ("golden", "kwargs"),
    [
        (
            "raw-answer.json",
            {
                "synthesizer_prompt": "synthesis-chartsearchai",
                "two_call": False,
                "solo": True,
            },
        ),
        (
            "raw-indepth-only.json",
            {
                "messages": MESSAGES
                + [{"role": "assistant", "content": "Lisinopril 10 mg [1]"}],
                "synthesizer_prompt": "synthesis-indepth",
                "indepth_only": True,
                "solo": True,
            },
        ),
        (
            "legacy-parity.json",
            {"synthesizer_prompt": "synthesis-chartsearchai", "two_call": False},
        ),
        ("legacy-two-call.json", {}),
    ],
)
def test_pre_refactor_envelopes_remain_byte_exact(golden, kwargs):
    expected = (GOLDENS / golden).read_text(encoding="utf-8").rstrip("\n")

    assert _run(**kwargs) == expected
