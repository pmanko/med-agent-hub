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

from server import team, levels_loader
from server.main import app

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
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(MESSAGES, response_format=RESP_FORMAT, temperature=0.0, max_tokens=1024))

    env = json.loads(out)
    # The Answer synthesis text is wrapped under the **Answer** header in the combined body.
    assert "**Answer**" in env["answer"] and "Lisinopril 10 mg [1]" in env["answer"]
    assert env["citations"] == [1]
    assert env["blocks"] == []


def test_parity_lane_single_call_bare_envelope():
    # The parity lane (two_call=False): orchestration still runs, but synthesis is ONE
    # chartsearchai-style call (synthesizer_prompt is a WHOLE prompt), validator off, and the
    # output is the BARE {answer, citations, blocks} envelope -- no **Answer**/**In Depth**
    # wrapper, no confidence block -- so it matches the direct single-LLM arms' format.
    rf_calls = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            rf_calls.append(model)
            return {"content": ENVELOPE}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(
            MESSAGES, response_format=RESP_FORMAT, temperature=0.0, max_tokens=1024,
            synthesizer_prompt="synthesis-chartsearchai", two_call=False, validator_model=None))

    env = json.loads(out)
    assert env["answer"] == "Lisinopril 10 mg [1]"          # raw answer, NOT wrapped under **Answer**
    assert "**Answer**" not in env["answer"] and "**In Depth**" not in env["answer"]
    assert env["citations"] == [1] and env["blocks"] == []
    assert "confidence" not in env                           # bare envelope, no confidence block
    assert len(rf_calls) == 1                                # ONE synthesis call, not the two-call split


INDEPTH = json.dumps({"claims": ["Per WHO guidance, start ART promptly after diagnosis.",
                                 "Monitor CD4 roughly every 6 months on stable therapy."]})


def _branching_fake_chat(seen):
    """A fake `_chat` that distinguishes the Answer call (chart_answer schema -> ENVELOPE) from the
    shared In-Depth call (in_depth schema -> claims) and records each constrained call's schema
    name, so a test can assert exactly which synthesis / validator passes ran."""
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is None:
            return {"content": "ok", "tool_calls": None}
        name = (response_format.get("json_schema") or {}).get("name")
        seen.append(name)
        return {"content": INDEPTH} if name == "in_depth" else {"content": ENVELOPE}
    return fake_chat


def test_parity_indepth_emits_answer_and_shared_indepth():
    # Parity lane + indepth_shared: the Answer is the parity (chartsearchai-prompt) single call,
    # THEN one shared In-Depth pass elaborates it -> the combined **Answer**/**In Depth** body so
    # a single-model-style arm is judged on the background dimension too.
    seen = []
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        out = run(team.run_team(
            MESSAGES, response_format=RESP_FORMAT, temperature=0.0, max_tokens=1024,
            synthesizer_prompt="synthesis-chartsearchai", two_call=False,
            indepth_shared=True, has_expert=False, validator_model=None))
    env = json.loads(out)
    assert "**Answer**" in env["answer"] and "Lisinopril 10 mg [1]" in env["answer"]
    assert "**In Depth**" in env["answer"] and "Per WHO guidance" in env["answer"]
    assert env["citations"] == [1]
    assert seen.count("chart_answer") == 1 and seen.count("in_depth") == 1  # one Answer + one In-Depth


def test_shared_indepth_is_single_pass_no_validator():
    # Even with a validator configured, the shared In-Depth lane runs ONE in-depth pass and NO
    # validator round -- it is the simpler single-pass path, not the validated two-call cycle.
    seen = []
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        run(team.run_team(
            MESSAGES, response_format=RESP_FORMAT, temperature=0.0, max_tokens=1024,
            synthesizer_prompt="synthesis-chartsearchai", two_call=False,
            indepth_shared=True, has_expert=False, validator_model="gemma-4-12b"))
    assert seen.count("in_depth") == 1
    assert "answer_verdict" not in seen and "indepth_verdict" not in seen   # zero validator calls


def test_parity_indepth_off_stays_bare_envelope():
    # Regression guard: with indepth_shared unset the parity lane is the existing BARE envelope
    # (no **In Depth**, no confidence) -- validated/parity behavior must be byte-for-byte untouched.
    seen = []
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        out = run(team.run_team(
            MESSAGES, response_format=RESP_FORMAT, temperature=0.0, max_tokens=1024,
            synthesizer_prompt="synthesis-chartsearchai", two_call=False, validator_model=None))
    env = json.loads(out)
    assert env["answer"] == "Lisinopril 10 mg [1]"
    assert "**In Depth**" not in env["answer"] and "confidence" not in env
    assert seen.count("in_depth") == 0


def test_single_indepth_answer_and_indepth_get_the_same_context_no_r1():
    # P1: R1 (answer-identity suppression) is DELETED — context is symmetric. A degenerate single (no
    # expert) emitting In-Depth now calls the ANSWER synthesis with the SAME gathered evidence as the
    # In-Depth pass. Forcing a KB hit makes the shared context observable (not a no-op).
    captured = {}

    async def fake_answer(client, synth_model, base_messages=None, answer_instruction=None,
                          gathered=None, *, response_format=None, temperature=None, max_tokens=None,
                          repeat_penalty=None, dry=None, extra_msgs=None):
        captured["answer_gathered"] = gathered
        return ("Answer text [1]", [1], [])

    async def fake_indepth(client, synth_model, base_messages=None, indepth_instruction=None,
                           gathered=None, answer_text=None, *, temperature=None, max_tokens=None,
                           repeat_penalty=None, dry=None, extra_msgs=None):
        captured["indepth_gathered"] = gathered
        return ["a claim"]

    async def fake_chat(client, model, messages, *, tools=None, response_format=None, **kwargs):
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat), \
         patch.object(team, "_run_kb_search",
                      return_value=team._KB_BLOCK_HEADER + "\nWHO: start ART promptly"), \
         patch.object(team, "_synthesize_answer", side_effect=fake_answer), \
         patch.object(team, "_synthesize_indepth", side_effect=fake_indepth):
        run(team.run_team(MESSAGES, response_format=RESP_FORMAT, max_tokens=1024,
                          synthesizer_prompt="synthesis-chartsearchai", two_call=False,
                          indepth_shared=True, has_expert=False, validator_model=None))

    assert "WHO: start ART promptly" in (captured.get("answer_gathered") or "")    # P1: Answer gets the same context
    assert "WHO: start ART promptly" in (captured.get("indepth_gathered") or "")   # In-Depth grounded too (symmetric)


def test_indepth_only_skips_answer_and_elaborates_the_prior_answer():
    # P1 (two-call architecture): the in-depth-only mode takes a prior ASSISTANT answer from the
    # message history and produces ONLY the In-Depth — no answer-synthesis call — so the harness can
    # fire it as a separate, later call (answer first, in-depth follows).
    seen = []
    msgs = MESSAGES + [{"role": "assistant", "content": "Lisinopril 10 mg [1]"}]
    with patch.object(team, "_chat", side_effect=_branching_fake_chat(seen)):
        out = run(team.run_team(
            msgs, response_format=RESP_FORMAT, max_tokens=1024,
            synthesizer_prompt="synthesis-chartsearchai", indepth_only=True,
            has_expert=False, validator_model=None))
    env = json.loads(out)
    assert "in_depth" in seen and "chart_answer" not in seen   # in-depth produced, NO answer synthesis
    assert "**In Depth**" in env["answer"] and "Per WHO guidance" in env["answer"]
    assert "**Answer**" not in env["answer"]                   # in-depth-only artifact, no Answer section


def test_response_format_is_only_applied_on_the_synthesis_calls():
    # The tool-selection turns must run PLAIN (no response_format); only the
    # synthesis calls are constrained. This is the load-bearing small-model rule.
    seen = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        seen.append({"tools": bool(tools), "rf": bool(response_format)})
        return {"content": ENVELOPE} if response_format is not None else {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    # No single call mixes tools + response_format.
    assert all(not (c["tools"] and c["rf"]) for c in seen)
    # The two-call synthesis (Answer + In-Depth) — both constrained, neither carries tools.
    rf_calls = [c for c in seen if c["rf"]]
    assert len(rf_calls) == 2 and all(c["tools"] is False for c in rf_calls)


def test_orchestrator_consults_the_medical_expert_on_a_tool_call():
    calls = []

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
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


def test_kb_results_are_threaded_into_the_medical_expert():
    # The headline of the prompt-driven design: when the orchestrator calls
    # kb_search then medical_expert, the clinical model must reason WITH the
    # retrieved guidance — the KB block is built into the expert's user message
    # in code, not left to the orchestrator to copy across.
    captured = {}
    orch_turns = {"n": 0}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            return {"content": ENVELOPE}
        if model == team.llm_config.med_model:
            captured["expert_user"] = messages[-1]["content"]
            return {"content": "the chart's regimen is outdated per the guidance"}
        # Orchestrator: kb_search, then medical_expert, then done.
        orch_turns["n"] += 1
        if orch_turns["n"] == 1:
            return {"role": "assistant", "content": None, "tool_calls": [
                {"id": "k1", "function": {"name": "kb_search",
                                          "arguments": json.dumps({"query": "stavudine d4T phase-out"})}}]}
        if orch_turns["n"] == 2:
            return {"role": "assistant", "content": None, "tool_calls": [
                {"id": "e1", "function": {"name": "medical_expert",
                                          "arguments": json.dumps({"query": "is the regimen still recommended?"})}}]}
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    expert_user = captured["expert_user"]
    # The expert received the labelled reference block AND the real KB snippet text
    # retrieved by kb_search (the d4T phase-out snippet contains "stavudine").
    assert "Reference guidance" in expert_user
    assert "stavudine" in expert_user.lower()


def test_orchestrator_can_search_the_knowledge_base():
    # The orchestrator emits a kb_search tool call; the REAL KB runs (only _chat
    # is seamed) and its labelled reference snippet flows into the synthesis turn.
    captured = {}

    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            captured["synth"] = messages
            return {"content": ENVELOPE}
        already_searched = any(m.get("role") == "tool" for m in messages)
        if tools is not None and not already_searched:
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "k1", "function": {"name": "kb_search",
                                              "arguments": json.dumps({"query": "metformin first-line diabetes"})}}
                ],
            }
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    json.loads(out)  # still a valid envelope
    blob = json.dumps(captured["synth"]).lower()
    # The real corpus snippet reached synthesis, labelled as reference (not chart) data.
    assert "metformin" in blob
    assert "knowledge-base reference snippets" in blob


def test_run_team_falls_back_to_a_valid_envelope_when_synthesis_fails():
    async def fake_chat(client, model, messages, *, tools=None, response_format=None,
                        temperature=None, max_tokens=None, **kwargs):
        if response_format is not None:
            raise RuntimeError("LM Studio 400: context overflow")
        return {"content": "ok", "tool_calls": None}

    with patch.object(team, "_chat", side_effect=fake_chat):
        out = run(team.run_team(MESSAGES, response_format=RESP_FORMAT))

    env = json.loads(out)
    # Always a schema-valid envelope, even on failure.
    assert set(env.keys()) >= {"answer", "citations", "blocks"}
    assert env["citations"] == [] and env["blocks"] == []


def test_v1_models_advertises_the_levels():
    client = TestClient(app)
    r = client.get("/v1/models")
    assert r.status_code == 200
    ids = [m["id"] for m in r.json()["data"]]
    # the configured levels are always advertised; _advertised_models() ALSO appends a dynamic
    # indepth-only:<router-model> leg per router model when the router is reachable. Assert
    # containment + that any extras are those dynamic legs — env-robust (router up or down).
    levels = levels_loader.level_ids()
    assert set(levels) <= set(ids)
    assert all(
        i in levels or i.startswith(("indepth-only:", "answer-only:", "answer:", "answer-review:"))
        for i in ids
    )


def test_v1_models_advertises_staged_capability_not_just_id_prefix():
    # Gate 10: clients must route by this field, never by pattern-matching the id string.
    client = TestClient(app)
    r = client.get("/v1/models")
    by_id = {m["id"]: m for m in r.json()["data"]}
    assert by_id["single-12b-checked"]["staged"] is True
    # parity is explicitly a single-shot (non-staged) relay target, never the phased engine
    assert by_id["med-agent-team-parity"]["staged"] is False
    # a dynamic low-level leg (never staged) and an unresolvable/raw id both fail soft to False
    assert by_id["answer-review:qwen2.5-14b"]["staged"] is False


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
            json={"model": "med-agent-team-med", "messages": MESSAGES, "response_format": RESP_FORMAT},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["model"] == "med-agent-team-med"
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
