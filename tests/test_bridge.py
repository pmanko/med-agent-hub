"""
Bridge tests: the in-process Med Agent Team loop (server/team.py) and the
OpenAI-compat surface (server/openai_compat.py).

Tests the REAL orchestration logic — the tool loop, the synthesis-on-final-call,
the guaranteed-valid fallback, and the endpoint shapes. The only thing seamed is
the external LM Studio call (`team._chat`): no real HTTP, no model required.
"""

import asyncio
import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from server import team
from server.main import app
from server.openai_compat import TEAM_MODEL_ID

ENVELOPE = json.dumps({"answer": "Lisinopril 10 mg [1]", "citations": [1], "blocks": []})
RESP_FORMAT = {"type": "json_schema", "json_schema": {"name": "chart_answer", "schema": {}}}
MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Lisinopril 10 mg"},
    {"role": "user", "content": "What meds is the patient on?"},
]


def run(coro):
    return asyncio.run(coro)


def test_run_team_produces_the_envelope_from_the_final_synthesis_call():
    # Orchestrator takes no tool action; the constrained synthesis call returns
    # the chart_answer envelope, which run_team passes straight through.
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None):
        if response_format is not None:
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(MESSAGES, response_format=RESP_FORMAT, temperature=0.0, max_tokens=1024))

    env = json.loads(out)
    assert env["answer"] == "Lisinopril 10 mg [1]"
    assert env["citations"] == [1]
    assert env["blocks"] == []


def test_response_format_is_only_applied_on_the_final_synthesis_call():
    # The tool-selection turns must run PLAIN (no response_format); only the
    # final synthesis is constrained. This is the load-bearing small-model rule.
    seen = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None):
        seen.append({"tools": bool(tools), "rf": bool(response_format)})
        return {"content": ENVELOPE} if response_format is not None else {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    # No single call mixes tools + response_format.
    assert all(not (c["tools"] and c["rf"]) for c in seen)
    # Exactly one constrained (synthesis) call, and it carries no tools.
    rf_calls = [c for c in seen if c["rf"]]
    assert len(rf_calls) == 1 and rf_calls[0]["tools"] is False


def test_orchestrator_consults_the_medical_expert_on_a_tool_call():
    calls = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None):
        calls.append(model)
        if response_format is not None:
            return {"content": ENVELOPE}
        # First orchestrator turn emits a tool call; later turns are done.
        orchestrator_turns = sum(1 for m in calls if m == team.llm_config.orchestrator_model)
        if tools is not None and orchestrator_turns == 1:
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "t1", "function": {"name": "medical_expert",
                                              "arguments": json.dumps({"query": "interpret"})}}
                ],
            }
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    json.loads(out)  # still a valid envelope
    # The medgemma expert was actually called (a _chat to the med model).
    assert team.llm_config.med_model in calls


def test_run_team_falls_back_to_a_valid_envelope_when_synthesis_fails():
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None):
        if response_format is not None:
            raise RuntimeError("LM Studio 400: context overflow")
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    env = json.loads(out)
    # Always a schema-valid envelope, even on failure.
    assert set(env.keys()) >= {"answer", "citations", "blocks"}
    assert env["citations"] == [] and env["blocks"] == []


def test_v1_models_advertises_the_single_team_choice():
    client = TestClient(app)
    r = client.get("/v1/models")
    assert r.status_code == 200
    ids = [m["id"] for m in r.json()["data"]]
    assert ids == [TEAM_MODEL_ID]


def test_chat_completions_team_returns_openai_shape_with_the_envelope():
    async def fake_run_team(messages, **kw):
        # Assert the bridge forwards the chart messages + response_format.
        assert messages == MESSAGES
        assert kw.get("response_format") == RESP_FORMAT
        return ENVELOPE

    with patch("server.openai_compat.run_team", side_effect=fake_run_team):
        client = TestClient(app)
        r = client.post(
            "/v1/chat/completions",
            json={"model": TEAM_MODEL_ID, "messages": MESSAGES, "response_format": RESP_FORMAT},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["model"] == TEAM_MODEL_ID
    content = body["choices"][0]["message"]["content"]
    assert json.loads(content)["citations"] == [1]


def test_raw_model_id_bypasses_the_team_and_passes_through():
    # A non-team model id must NOT run the team — it forwards straight to LM Studio.
    async def fake_passthrough(req):
        return "raw model said hi"

    with patch("server.openai_compat.run_team") as mock_team, \
         patch("server.openai_compat._passthrough_content", side_effect=fake_passthrough):
        client = TestClient(app)
        r = client.post(
            "/v1/chat/completions",
            json={"model": "some-raw-model", "messages": MESSAGES},
        )

    assert r.status_code == 200
    assert r.json()["choices"][0]["message"]["content"] == "raw model said hi"
    mock_team.assert_not_called()
