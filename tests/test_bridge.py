"""
Bridge tests: the in-process Med Agent Team loop (server/team.py) and the
OpenAI-compat surface (server/openai_compat.py).

Tests the real stage helpers, fallback, and endpoint shapes. The only seam is
the external model call (`team._chat`): no real HTTP or model is required.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from server import levels_loader, openai_compat, team
from server.main import app
from tests.factories import (
    TEST_ANSWER_MODEL,
    TEST_EXPERT_MODEL,
    TEST_ORCHESTRATOR_MODEL,
    make_profile,
    run_profile,
)

ENVELOPE = json.dumps(
    {"answer": "Lisinopril 10 mg [1]", "citations": [1], "blocks": []}
)
RESP_FORMAT = {
    "type": "json_schema",
    "json_schema": {"name": "chart_answer", "schema": {}},
}
MESSAGES = [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user", "content": "[1] Lisinopril 10 mg"},
    {"role": "user", "content": "What meds is the patient on?"},
]


def run(coro):
    return asyncio.run(coro)


def _team_profile(*, output="combined", answer_prompt="synthesis-answer", indepth=True):
    stages = ["context", "gather", "answer", "gate"]
    models = {
        "orchestrator": TEST_ORCHESTRATOR_MODEL,
        "expert": TEST_EXPERT_MODEL,
        "answer": TEST_ANSWER_MODEL,
    }
    prompts = {
        "orchestrator": "orchestrator",
        "expert": "medical_expert",
        "answer": answer_prompt,
    }
    if indepth:
        stages.append("indepth")
        models["indepth"] = TEST_ANSWER_MODEL
        prompts["indepth"] = "synthesis-indepth"
    return make_profile(
        topology="team",
        stages=stages,
        models=models,
        prompts=prompts,
        output=output,
    )


def _indepth_leg():
    return make_profile(
        topology="leg",
        stages=("context", "indepth"),
        models={"indepth": TEST_ANSWER_MODEL},
        prompts={"indepth": "synthesis-indepth"},
        output="indepth",
    )


def test_profile_drain_produces_the_envelope_from_the_final_synthesis_call():
    # Orchestrator takes no tool action; the constrained synthesis call returns
    # the chart_answer envelope, which the profile drain passes straight through.
    async def fake_chat(
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
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(
            run_profile(
                _team_profile(),
                MESSAGES,
                response_format=RESP_FORMAT,
                temperature=0.0,
                max_tokens=1024,
            )
        )

    env = json.loads(out)
    # The Answer synthesis text is wrapped under the **Answer** header in the combined body.
    assert "**Answer**" in env["answer"] and "Lisinopril 10 mg [1]" in env["answer"]
    assert env["citations"] == [1]
    assert env["blocks"] == []


def test_parity_lane_single_call_bare_envelope():
    # The parity profile declares one bare Answer synthesis call after gather.
    # chartsearchai-style call (synthesizer_prompt is a WHOLE prompt), validator off, and the
    # output is the BARE {answer, citations, blocks} envelope -- no **Answer**/**In Depth**
    # wrapper, no confidence block -- so it matches the direct single-LLM arms' format.
    rf_calls = []

    async def fake_chat(
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
            rf_calls.append(model)
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(
            run_profile(
                _team_profile(
                    output="bare",
                    answer_prompt="synthesis-chartsearchai",
                    indepth=False,
                ),
                MESSAGES,
                response_format=RESP_FORMAT,
                temperature=0.0,
                max_tokens=1024,
            )
        )

    env = json.loads(out)
    assert (
        env["answer"] == "Lisinopril 10 mg [1]"
    )  # raw answer, NOT wrapped under **Answer**
    assert "**Answer**" not in env["answer"] and "**In Depth**" not in env["answer"]
    assert env["citations"] == [1] and env["blocks"] == []
    assert "confidence" not in env  # bare envelope, no confidence block
    assert len(rf_calls) == 1  # ONE synthesis call, not the two-call split


INDEPTH = json.dumps(
    {
        "claims": [
            "Per WHO guidance, start ART promptly after diagnosis.",
            "Monitor CD4 roughly every 6 months on stable therapy.",
        ]
    }
)


def _branching_fake_chat(seen):
    """A fake `_chat` that distinguishes the Answer call (chart_answer schema -> ENVELOPE) from the
    shared In-Depth call (in_depth schema -> claims) and records each constrained call's schema
    name, so a test can assert exactly which synthesis / validator passes ran."""

    async def fake_chat(
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
        if response_format is None:
            return {"content": "ok", "tool_calls": None}
        name = (response_format.get("json_schema") or {}).get("name")
        seen.append(name)
        return {"content": INDEPTH} if name == "in_depth" else {"content": ENVELOPE}

    return fake_chat


def test_parity_indepth_emits_answer_and_shared_indepth():
    # The combined parity profile adds one In-Depth stage after its Answer.
    # THEN one shared In-Depth pass elaborates it -> the combined **Answer**/**In Depth** body so
    # a single-model-style arm is judged on the background dimension too.
    seen = []
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        out = run(
            run_profile(
                _team_profile(answer_prompt="synthesis-chartsearchai"),
                MESSAGES,
                response_format=RESP_FORMAT,
                temperature=0.0,
                max_tokens=1024,
            )
        )
    env = json.loads(out)
    assert "**Answer**" in env["answer"] and "Lisinopril 10 mg [1]" in env["answer"]
    assert "**In Depth**" in env["answer"] and "Per WHO guidance" in env["answer"]
    assert env["citations"] == [1]
    assert (
        seen.count("chart_answer") == 1 and seen.count("in_depth") == 1
    )  # one Answer + one In-Depth


def test_shared_indepth_is_single_pass_no_validator():
    # Even with a validator configured, the shared In-Depth lane runs ONE in-depth pass and NO
    # validator round -- it is the simpler single-pass path, not the validated two-call cycle.
    seen = []
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        run(
            run_profile(
                _team_profile(answer_prompt="synthesis-chartsearchai"),
                MESSAGES,
                response_format=RESP_FORMAT,
                temperature=0.0,
                max_tokens=1024,
            )
        )
    assert seen.count("in_depth") == 1
    assert (
        "answer_verdict" not in seen and "indepth_verdict" not in seen
    )  # zero validator calls


def test_parity_indepth_off_stays_bare_envelope():
    # Regression guard: a bare profile has no In-Depth or confidence wrapper.
    # (no **In Depth**, no confidence) -- validated/parity behavior must be byte-for-byte untouched.
    seen = []
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        out = run(
            run_profile(
                _team_profile(
                    output="bare",
                    answer_prompt="synthesis-chartsearchai",
                    indepth=False,
                ),
                MESSAGES,
                response_format=RESP_FORMAT,
                temperature=0.0,
                max_tokens=1024,
            )
        )
    env = json.loads(out)
    assert env["answer"] == "Lisinopril 10 mg [1]"
    assert "**In Depth**" not in env["answer"] and "confidence" not in env
    assert seen.count("in_depth") == 0


def test_single_indepth_answer_and_indepth_get_the_same_context_no_r1():
    # P1: R1 (answer-identity suppression) is DELETED — context is symmetric. A degenerate single (no
    # expert) emitting In-Depth now calls the ANSWER synthesis with the SAME gathered evidence as the
    # In-Depth pass. Forcing a KB hit makes the shared context observable (not a no-op).
    captured = {}

    async def fake_answer(
        client,
        synth_model,
        base_messages=None,
        answer_instruction=None,
        gathered=None,
        *,
        response_format=None,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry=None,
        extra_msgs=None,
    ):
        captured["answer_context"] = json.dumps(base_messages)
        return ("Answer text [1]", [1], [])

    async def fake_indepth(
        client,
        synth_model,
        base_messages=None,
        indepth_instruction=None,
        gathered=None,
        answer_text=None,
        *,
        temperature=None,
        max_tokens=None,
        repeat_penalty=None,
        dry=None,
        extra_msgs=None,
    ):
        captured["indepth_context"] = json.dumps(base_messages)
        return ["a claim"]

    async def fake_chat(
        client, model, messages, *, tools=None, response_format=None, **kwargs
    ):
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat), patch(
        "server.context_sources.kb.search",
        return_value=[
            {
                "id": "who-art",
                "title": "WHO guidance",
                "text": "start ART promptly",
                "source": "WHO",
            }
        ],
    ), patch.object(team, "_synthesize_answer", side_effect=fake_answer), patch.object(
        team, "_synthesize_indepth", side_effect=fake_indepth
    ):
        run(
            run_profile(
                _team_profile(answer_prompt="synthesis-chartsearchai"),
                MESSAGES,
                response_format=RESP_FORMAT,
                max_tokens=1024,
            )
        )

    assert "start ART promptly" in captured.get("answer_context", "")
    assert "start ART promptly" in captured.get("indepth_context", "")


def test_indepth_only_skips_answer_and_elaborates_the_prior_answer():
    # P1 (two-call architecture): the in-depth-only mode takes a prior ASSISTANT answer from the
    # message history and produces ONLY the In-Depth — no answer-synthesis call — so the harness can
    # fire it as a separate, later call (answer first, in-depth follows).
    seen = []
    msgs = MESSAGES + [{"role": "assistant", "content": "Lisinopril 10 mg [1]"}]
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        out = run(
            run_profile(
                _indepth_leg(), msgs, response_format=RESP_FORMAT, max_tokens=1024
            )
        )
    env = json.loads(out)
    assert (
        "in_depth" in seen and "chart_answer" not in seen
    )  # in-depth produced, NO answer synthesis
    assert "**In Depth**" in env["answer"] and "Per WHO guidance" in env["answer"]
    assert (
        "**Answer**" not in env["answer"]
    )  # in-depth-only artifact, no Answer section


def test_response_format_is_only_applied_on_the_synthesis_calls():
    # The tool-selection turns must run PLAIN (no response_format); only the
    # synthesis calls are constrained. This is the load-bearing small-model rule.
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
        **kwargs,
    ):
        seen.append({"tools": bool(tools), "rf": bool(response_format)})
        return (
            {"content": ENVELOPE}
            if response_format is not None
            else {"content": "ok", "tool_calls": None}
        )

    with patch.object(team, "_chat", side_effect=fake_chat):
        run(run_profile(_team_profile(), MESSAGES, response_format=RESP_FORMAT))

    # No single call mixes tools + response_format.
    assert all(not (c["tools"] and c["rf"]) for c in seen)
    # The two-call synthesis (Answer + In-Depth) — both constrained, neither carries tools.
    rf_calls = [c for c in seen if c["rf"]]
    assert len(rf_calls) == 2 and all(c["tools"] is False for c in rf_calls)


def test_orchestrator_consults_the_medical_expert_on_a_tool_call():
    calls = []

    async def fake_chat(
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
        calls.append(model)
        if response_format is not None:
            return {"content": ENVELOPE}
        # First orchestrator turn emits a tool call; later turns are done.
        orchestrator_turns = sum(
            1 for m in calls if m == TEST_ORCHESTRATOR_MODEL
        )
        if tools is not None and orchestrator_turns == 1:
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "t1",
                        "function": {
                            "name": "medical_expert",
                            "arguments": json.dumps({"query": "interpret"}),
                        },
                    }
                ],
            }
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(run_profile(_team_profile(), MESSAGES, response_format=RESP_FORMAT))

    json.loads(out)  # still a valid envelope
    # The medgemma expert was actually called (a _chat to the med model).
    assert TEST_EXPERT_MODEL in calls


def test_normalized_kb_records_are_threaded_into_the_medical_expert():
    # Retrieval is completed before gather. The expert receives the same numbered ledger,
    # including provenance-labelled KnowledgeReference records, with no second search path.
    captured = {}
    orch_turns = {"n": 0}

    async def fake_chat(
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
            return {"content": ENVELOPE}
        if model == TEST_EXPERT_MODEL:
            captured["expert_user"] = messages[-1]["content"]
            return {"content": "the chart's regimen is outdated per the guidance"}
        # Orchestrator: consult the expert once, then stop.
        orch_turns["n"] += 1
        if orch_turns["n"] == 1:
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "e1",
                        "function": {
                            "name": "medical_expert",
                            "arguments": json.dumps(
                                {"query": "is the regimen still recommended?"}
                            ),
                        },
                    }
                ],
            }
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat), patch(
        "server.context_sources.kb.search",
        return_value=[
            {
                "id": "who-d4t",
                "title": "WHO guidance",
                "text": "Stavudine is not a preferred antiretroviral backbone.",
                "source": "WHO",
            }
        ],
    ):
        run(run_profile(_team_profile(), MESSAGES, response_format=RESP_FORMAT))

    expert_user = captured["expert_user"]
    assert "KnowledgeReference (source: knowledge-base)" in expert_user
    assert "stavudine" in expert_user.lower()


def test_profile_drain_falls_back_to_a_valid_envelope_when_synthesis_fails():
    async def fake_chat(
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
            raise RuntimeError("LM Studio 400: context overflow")
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(run_profile(_team_profile(), MESSAGES, response_format=RESP_FORMAT))

    env = json.loads(out)
    # Always a schema-valid envelope, even on failure.
    assert set(env.keys()) >= {"answer", "citations", "blocks"}
    assert env["citations"] == [] and env["blocks"] == []


def test_v1_models_advertises_the_levels():
    client = TestClient(app)
    r = client.get("/v1/models")
    assert r.status_code == 200
    ids = [m["id"] for m in r.json()["data"]]
    # Discovery is authoritative product/config metadata. Low-level dynamic legs remain
    # callable but are intentionally not advertised in the product model picker.
    assert ids == levels_loader.profile_ids()


def test_root_status_does_not_duplicate_authoritative_profile_metadata():
    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert response.json()["status"] == "Server is running"
    assert "default_profile" not in response.json()


def test_backend_model_discovery_uses_configured_bearer_auth():
    backend = SimpleNamespace(base_url="https://router.example", api_key="secret")
    with patch.object(openai_compat, "llm_config", backend), patch.object(
        openai_compat.httpx, "get"
    ) as get:
        get.return_value.json.return_value = {"data": [{"id": "gemma-e4b"}]}

        assert openai_compat._served_backend_models() == {"gemma-e4b"}

    get.assert_called_once_with(
        "https://router.example/v1/models",
        headers={"Authorization": "Bearer secret"},
        timeout=3.0,
    )


def test_backend_model_discovery_omits_auth_when_api_key_is_blank():
    backend = SimpleNamespace(base_url="http://router", api_key="")
    with patch.object(openai_compat, "llm_config", backend), patch.object(
        openai_compat.httpx, "get"
    ) as get:
        get.return_value.json.return_value = {"data": []}

        assert openai_compat._served_backend_models() == set()

    get.assert_called_once_with(
        "http://router/v1/models",
        headers={},
        timeout=3.0,
    )


def test_backend_model_discovery_returns_none_when_router_is_unreachable():
    backend = SimpleNamespace(base_url="http://router", api_key="")
    with patch.object(openai_compat, "llm_config", backend), patch.object(
        openai_compat.httpx, "get", side_effect=ConnectionError("offline")
    ):
        assert openai_compat._served_backend_models() is None


def test_v1_models_distinguishes_empty_catalog_from_unreachable_router():
    with patch.object(openai_compat, "_served_backend_models", return_value=set()):
        empty = TestClient(app).get("/v1/models").json()["data"]
    with patch.object(openai_compat, "_served_backend_models", return_value=None):
        unreachable = TestClient(app).get("/v1/models").json()["data"]

    assert all(
        item["unavailable_reasons"]
        and all(reason.startswith("model_not_loaded:") for reason in item["unavailable_reasons"])
        for item in empty
    )
    assert all(
        item["unavailable_reasons"] == ["model_backend_unreachable"]
        for item in unreachable
    )


def test_v1_models_advertises_staged_capability_not_just_id_prefix():
    # Gate 10: clients must route by this field, never by pattern-matching the id string.
    with patch.object(openai_compat, "_served_backend_models", return_value={"gemma-e4b"}):
        client = TestClient(app)
        r = client.get("/v1/models")
    by_id = {m["id"]: m for m in r.json()["data"]}
    assert by_id["single-12b-checked"]["staged"] is True
    # parity is explicitly a single-shot (non-staged) relay target, never the phased engine
    assert by_id["med-agent-team-parity"]["staged"] is False
    assert by_id["single-e4b-checked"]["default"] is True
    assert "answer-review:qwen2.5-14b" not in by_id


def test_v1_models_selects_available_fallback_default_in_the_hub():
    with patch.object(openai_compat, "_served_backend_models", return_value={"gemma-26b"}):
        client = TestClient(app)
        r = client.get("/v1/models")

    product = [item for item in r.json()["data"] if item["visibility"] == "product"]
    defaults = [item for item in product if item["default"]]
    assert [(item["id"], item["available"]) for item in defaults] == [
        ("single-a4b-checked", True)
    ]
    assert next(item for item in product if item["id"] == "single-e4b-checked")["default"] is False


def test_chat_completions_profile_returns_openai_shape_with_the_envelope():
    async def fake_drain(execution):
        # The API is only an adapter: it compiles the profile and drains the engine.
        assert execution.messages == MESSAGES
        assert execution.response_format == RESP_FORMAT
        assert execution.profile.id == "med-agent-team-med"
        return ENVELOPE

    with patch("server.openai_compat.drain_profile", side_effect=fake_drain):
        client = TestClient(app)
        r = client.post(
            "/v1/chat/completions",
            json={
                "model": "med-agent-team-med",
                "messages": MESSAGES,
                "response_format": RESP_FORMAT,
            },
        )

    assert r.status_code == 200
    body = r.json()
    assert body["model"] == "med-agent-team-med"
    content = body["choices"][0]["message"]["content"]
    assert json.loads(content)["citations"] == [1]


def test_unknown_model_id_returns_structured_model_not_found():
    with patch("server.openai_compat.drain_profile") as mock_drain:
        client = TestClient(app)
        r = client.post(
            "/v1/chat/completions",
            json={"model": "some-raw-model", "messages": MESSAGES},
        )

    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "model_not_found"
    assert r.json()["detail"]["model"] == "some-raw-model"
    mock_drain.assert_not_called()


def test_product_client_rejects_low_level_leg_before_execution():
    with patch("server.openai_compat.drain_profile") as mock_drain:
        client = TestClient(app)
        r = client.post(
            "/v1/chat/completions",
            json={
                "model": "answer:gemma-e4b@synthesis-answer~off",
                "messages": MESSAGES,
                "context": {"require_product_profile": True},
            },
        )

    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "product_profile_required"
    mock_drain.assert_not_called()


def test_direct_hub_client_can_still_use_low_level_leg():
    async def fake_drain(execution):
        assert execution.profile.id == "answer:gemma-e4b@synthesis-answer~off"
        return ENVELOPE

    with patch("server.openai_compat.drain_profile", side_effect=fake_drain):
        client = TestClient(app)
        r = client.post(
            "/v1/chat/completions",
            json={
                "model": "answer:gemma-e4b@synthesis-answer~off",
                "messages": MESSAGES,
            },
        )

    assert r.status_code == 200
