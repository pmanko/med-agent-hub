"""Executable acceptance checks for exact deterministic context budgeting."""

import asyncio
import json
from dataclasses import replace

import pytest

from server import engine, team
from server.context_sources import (
    ContextBudget,
    EvidenceLedger,
    EvidenceRecord,
    InsufficientContextError,
    fit_message_history,
    select_context,
)
from server.levels_loader import get_profile
from tests.factories import patient_source_registry


class ExactWordCounter:
    async def count(self, _model: str, text: str) -> int:
        return len(text.split())


def _record(source_id: str, text: str, *, mandatory: bool = False) -> EvidenceRecord:
    return EvidenceRecord(
        stable_id=source_id,
        source="fixture",
        source_priority=10,
        resource_type="Observation",
        resource_uuid=source_id,
        date="2026-01-01",
        text=text,
        mandatory=mandatory,
    )


def test_exact_boundary_preserves_the_full_chart_text():
    ledger = EvidenceLedger((_record("r1", "one two"),), original_text="[1] one two\n")
    view = asyncio.run(
        select_context(
            ledger,
            question="one",
            model="fixture-model",
            budget=ContextBudget(context_window=5, reserved_output_tokens=1),
            counter=ExactWordCounter(),
            fixed_text="fixed",
        )
    )
    assert view.mode == "full"
    assert view.input_tokens == view.input_limit == 4
    assert view.render() == "[1] one two\n"


def test_oversized_selection_preserves_canonical_indices_and_trace_reasons():
    ledger = EvidenceLedger(
        (
            _record("safety", "allergy evidence", mandatory=True),
            _record("large", "unrelated words that do not fit"),
            _record("target", "code WT-71"),
        )
    )
    view = asyncio.run(
        select_context(
            ledger,
            question="Find WT-71",
            model="fixture-model",
            budget=ContextBudget(context_window=8, reserved_output_tokens=1),
            counter=ExactWordCounter(),
            fixed_text="fixed",
        )
    )
    assert view.mode == "selected"
    assert view.record_indices == (1, 3)
    assert "[1] allergy evidence" in view.render()
    assert "[3] code WT-71" in view.render()
    assert view.input_tokens <= view.input_limit
    assert [(item.stable_id, item.reason) for item in view.excluded] == [
        ("large", "token_budget_after_ranked")
    ]


def test_mandatory_overflow_returns_insufficient_context():
    ledger = EvidenceLedger((_record("safety", "mandatory evidence", mandatory=True),))
    with pytest.raises(InsufficientContextError) as caught:
        asyncio.run(
            select_context(
                ledger,
                question="question",
                model="fixture-model",
                budget=ContextBudget(context_window=2, reserved_output_tokens=1),
                counter=ExactWordCounter(),
                fixed_text="fixed",
            )
        )
    assert caught.value.mandatory_ids == ("safety",)


def test_prior_turn_citations_are_stripped_before_current_source_ledger_resolution():
    messages = [
        {"role": "user", "content": "What was the earlier weight?"},
        {"role": "assistant", "content": "The earlier weight was 70 kg [1]."},
        {"role": "user", "content": "What does the current chart show?"},
    ]

    history = asyncio.run(
        fit_message_history(
            messages,
            model="fixture-model",
            budget=ContextBudget(context_window=100, reserved_output_tokens=10),
            counter=ExactWordCounter(),
            fixed_renderer=lambda candidate: "\n".join(
                str(message["content"]) for message in candidate
            ),
        )
    )

    assert history.stripped_citation_tokens == 1
    assert history.messages[1]["content"] == "The earlier weight was 70 kg ."
    assert "[1]" not in history.messages[1]["content"]


class ExactChatCounter(ExactWordCounter):
    def __init__(self, input_tokens: int = 5):
        self.input_tokens = input_tokens
        self.payloads = []

    async def count_chat(self, model, payload):
        self.payloads.append((model, payload))
        return self.input_tokens


class FakeResponse:
    status_code = 200

    def json(self):
        return {"choices": [{"message": {"content": "ok"}}]}


class FakeClient:
    def __init__(self):
        self.requests = []

    async def post(self, url, *, json, headers, timeout):
        self.requests.append((url, json, headers, timeout))
        return FakeResponse()


def test_actual_chat_request_is_exactly_counted_and_output_is_capped():
    counter = ExactChatCounter(input_tokens=80)
    policy = team.ChatBudgetPolicy(
        counter=counter, context_window=100, reserved_output_tokens=20
    )
    token = team.activate_chat_budget(policy)
    client = FakeClient()
    try:
        asyncio.run(
            team._chat(
                client,
                "fixture-model",
                [{"role": "user", "content": "hello"}],
                max_tokens=999,
            )
        )
    finally:
        team.reset_chat_budget(token)

    assert counter.payloads[0][1]["messages"] == [{"role": "user", "content": "hello"}]
    assert counter.payloads[0][1]["max_tokens"] == 20
    assert client.requests[0][1]["max_tokens"] == 20
    assert policy.measurements == [
        {
            "model": "fixture-model",
            "input_tokens": 80,
            "output_tokens": 20,
            "context_window": 100,
        }
    ]


def test_actual_chat_request_overflow_is_rejected_before_backend_call():
    counter = ExactChatCounter(input_tokens=81)
    policy = team.ChatBudgetPolicy(
        counter=counter, context_window=100, reserved_output_tokens=20
    )
    token = team.activate_chat_budget(policy)
    client = FakeClient()
    try:
        with pytest.raises(InsufficientContextError, match="exceeding"):
            asyncio.run(
                team._chat(
                    client,
                    "fixture-model",
                    [{"role": "user", "content": "hello"}],
                )
            )
    finally:
        team.reset_chat_budget(token)

    assert client.requests == []


def test_grounding_batches_split_against_the_actual_chat_budget():
    class PairCounter:
        async def count_chat(self, _model, payload):
            prompt = payload["messages"][0]["content"]
            return prompt.count("PAIR ") * 10

    class GroundingResponse:
        status_code = 200

        def __init__(self, count):
            self.count = count

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {"verdicts": ["YES"] * self.count}
                            )
                        }
                    }
                ]
            }

    class GroundingClient:
        def __init__(self):
            self.batch_sizes = []

        async def post(self, _url, *, json, **_kwargs):
            count = json["messages"][0]["content"].count("PAIR ")
            self.batch_sizes.append(count)
            return GroundingResponse(count)

    policy = team.ChatBudgetPolicy(
        counter=PairCounter(), context_window=35, reserved_output_tokens=5
    )
    token = team.activate_chat_budget(policy)
    client = GroundingClient()
    try:
        verdicts = asyncio.run(
            team._bounded_entailment_verdicts(
                client,
                "fixture-model",
                [(f"source {index}", f"claim {index}") for index in range(5)],
            )
        )
    finally:
        team.reset_chat_budget(token)

    assert verdicts == [True] * 5
    assert client.batch_sizes == [2, 3]
    assert [item["input_tokens"] for item in policy.measurements] == [50, 20, 30]


def test_team_derived_context_triggers_exact_answer_context_reselection():
    mappings = [
        {
            "resourceType": "Observation",
            "resourceUuid": f"obs-{index}",
            "date": f"2026-01-0{index}",
            "text": f"(2026-01-0{index}) Weight: {70 + index} kg",
        }
        for index in range(1, 4)
    ]
    registry = patient_source_registry(
        "".join(f"[{index}] {row['text']}\n" for index, row in enumerate(mappings, 1)),
        mappings,
    )

    class PayloadWordCounter(ExactWordCounter):
        async def count_chat(self, _model, payload):
            return len(
                " ".join(
                    str(message.get("content") or "") for message in payload["messages"]
                ).split()
            )

    profile = replace(
        get_profile("team-med-checked"),
        context_window=1300,
        reserved_output_tokens=20,
    )
    state = engine._State(messages=[{"role": "user", "content": "What is the weight?"}])
    request = engine.ExecutionRequest(
        profile=profile,
        messages=state.messages,
        patient="patient-1",
        source_registry=registry,
        token_counter=PayloadWordCounter(),
    )

    asyncio.run(engine._prepare_context(request, state))
    before = len(state.view.records)
    assert "knowledge-base" in state.ledger.source_names
    assert before == len(state.ledger.records)
    extra_words = state.view.input_limit - state.view.input_tokens + 5
    state.gathered += "\n\n" + ("derived " * extra_words)
    asyncio.run(engine._select_answer_context(request, state))

    assert len(state.view.records) < before


def test_single_product_profile_supplies_knowledge_to_answer_and_indepth():
    mappings = [
        {
            "resourceType": "DrugOrder",
            "resourceUuid": "stavudine-order",
            "date": "2026-01-07",
            "text": "(2026-01-07) Drug order: Stavudine",
        }
    ]
    registry = patient_source_registry("[1] (2026-01-07) Drug order: Stavudine\n", mappings)
    messages = [
        {"role": "user", "content": "What medications is this patient taking?"},
        {
            "role": "assistant",
            "content": "The patient is taking stavudine, nevirapine, and lamivudine [1].",
        },
        {
            "role": "user",
            "content": "How does the regimen compare with WHO recommendations?",
        },
    ]
    profile = get_profile("single-12b-checked")
    state = engine._State(messages=messages)
    request = engine.ExecutionRequest(
        profile=profile,
        messages=state.messages,
        patient="patient-1",
        source_registry=registry,
        token_counter=ExactWordCounter(),
    )

    asyncio.run(engine._prepare_context(request, state))

    assert state.ledger.source_names == ("test-patient", "knowledge-base")
    assert "Stavudine (d4T) is no longer recommended" in state.chart

    _view, _messages, indepth_chart = asyncio.run(
        engine._select_indepth_context(
            request,
            state,
            "The checked answer compares the regimen with WHO guidance.",
        )
    )
    assert "Stavudine (d4T) is no longer recommended" in indepth_chart


def test_indepth_refits_context_without_mutating_answer_view_or_ledger():
    mappings = [
        {
            "resourceType": "Observation",
            "resourceUuid": f"obs-{index}",
            "date": f"2026-01-0{index}",
            "text": f"(2026-01-0{index}) Weight: {70 + index} kg",
        }
        for index in range(1, 4)
    ]
    registry = patient_source_registry(
        "".join(f"[{index}] {row['text']}\n" for index, row in enumerate(mappings, 1)),
        mappings,
    )

    class StageCounter:
        async def count(self, _model, text):
            return text.count("\n[") + 1

        async def count_chat(self, _model, payload):
            content = "\n".join(
                str(message.get("content") or "")
                for message in payload["messages"]
            )
            records = sum(
                1
                for line in content.splitlines()
                if len(line) > 2 and line[0] == "[" and line[1].isdigit()
            )
            fixed = 2 if "=== DIRECT ANSWER" in content else 1
            return records + fixed

    profile = replace(
        get_profile("single-e4b-checked"),
        supplemental_sources=(),
        context_window=5,
        reserved_output_tokens=1,
    )
    state = engine._State(
        messages=[{"role": "user", "content": "What is the weight trend?"}]
    )
    request = engine.ExecutionRequest(
        profile=profile,
        messages=state.messages,
        patient="patient-1",
        source_registry=registry,
        token_counter=StageCounter(),
    )

    asyncio.run(engine._prepare_context(request, state))
    assert state.view is not None and state.view.mode == "full"
    assert len(state.view.records) == len(state.ledger.records) == 3

    indepth_view, indepth_messages, indepth_chart = asyncio.run(
        engine._select_indepth_context(request, state, "The weight declined.")
    )

    assert indepth_view.mode == "selected"
    assert len(indepth_view.records) == 2
    assert indepth_view.input_tokens <= indepth_view.input_limit
    assert len(state.view.records) == len(state.ledger.records) == 3
    record_lines = lambda text: sum(
        1
        for line in text.splitlines()
        if len(line) > 2 and line[0] == "[" and line[1].isdigit()
    )
    assert record_lines(state.chart) == 3
    assert record_lines(indepth_chart) == 2
    assert any(
        "Patient records (most recent first):" in str(message.get("content") or "")
        and str(message.get("content") or "").count("\n[") == 2
        for message in indepth_messages
    )
    context_step = state.steps[-1]
    assert context_step["stage"] == "indepth"
    assert all(item["stable_id"] and item["reason"] for item in context_step["included"])
    assert all(item["stable_id"] and item["reason"] for item in context_step["excluded"])


def test_indepth_synthesis_review_and_retry_match_the_exact_backend_guard():
    mappings = [
        {
            "resourceType": "Observation",
            "resourceUuid": f"obs-{index}",
            "date": f"2026-01-0{index}",
            "text": f"(2026-01-0{index}) Finding: value {index}",
        }
        for index in range(1, 4)
    ]
    registry = patient_source_registry(
        "".join(f"[{index}] {row['text']}\n" for index, row in enumerate(mappings, 1)),
        mappings,
    )

    class StageExactCounter:
        async def count(self, _model, text):
            return len(text.split())

        async def count_chat(self, _model, payload):
            content = "\n".join(
                str(message.get("content") or "")
                for message in payload["messages"]
            )
            records = sum(
                1
                for line in content.splitlines()
                if len(line) > 2 and line[0] == "[" and line[1].isdigit()
            )
            schema = (
                (payload.get("response_format") or {})
                .get("json_schema", {})
                .get("name")
            )
            if schema == "in_depth":
                fixed = 5 if "retry-feedback" in content else 3
            elif schema == "indepth_verdict":
                fixed = 4
            elif schema == "rewrite_verdict":
                fixed = 4
            else:
                fixed = 1
            return records + fixed

    base_profile = get_profile("single-e4b-checked")
    profile = replace(
        base_profile,
        supplemental_sources=(),
        models={
            **base_profile.models,
            "indepth": "writer-model",
            "review": "reviewer-model",
        },
        context_window=8,
        reserved_output_tokens=2,
    )
    counter = StageExactCounter()
    state = engine._State(
        messages=[{"role": "user", "content": "What are the findings?"}]
    )
    request = engine.ExecutionRequest(
        profile=profile,
        messages=state.messages,
        patient="patient-1",
        source_registry=registry,
        token_counter=counter,
    )
    asyncio.run(engine._prepare_context(request, state))
    answer = "The findings are documented."
    initial_view, initial_messages, _ = asyncio.run(
        engine._select_indepth_context(request, state, answer)
    )
    answer_review_chart = asyncio.run(
        engine._select_answer_review_context(request, state, answer, [])
    )
    review_chart = asyncio.run(
        engine._select_indepth_review_context(
            request, state, answer, ["Finding 1 [1]."]
        )
    )
    retry_extra = [
        {"role": "assistant", "content": '{"claims":["Finding 1 [1]."]}'},
        {"role": "user", "content": "retry-feedback"},
    ]
    retry_view, retry_messages, _ = asyncio.run(
        engine._select_indepth_context(
            request,
            state,
            answer,
            extra_msgs=retry_extra,
            stage="indepth_retry",
        )
    )

    assert initial_view.mode == "full" and len(initial_view.records) == 3
    assert sum(
        1 for line in answer_review_chart.splitlines() if line.startswith("[")
    ) == 2
    assert sum(1 for line in review_chart.splitlines() if line.startswith("[")) == 2
    assert retry_view.mode == "selected" and len(retry_view.records) == 1

    class StageResponse:
        status_code = 200

        def __init__(self, content):
            self.content = content

        def json(self):
            return {"choices": [{"message": {"content": self.content}}]}

    class StageClient:
        def __init__(self):
            self.requests = []

        async def post(self, _url, *, json, **_kwargs):
            self.requests.append(json)
            schema = json["response_format"]["json_schema"]["name"]
            if schema == "indepth_verdict":
                return StageResponse('{"drop":[],"issues":""}')
            return StageResponse('{"claims":["Finding 1 [1]."]}')

    client = StageClient()
    policy = team.ChatBudgetPolicy(
        counter=counter, context_window=8, reserved_output_tokens=2
    )
    token = team.activate_chat_budget(policy)
    try:
        asyncio.run(
            team._synthesize_indepth(
                client,
                "writer-model",
                initial_messages,
                engine._prompt(profile, "indepth", "synthesis-indepth"),
                state.gathered,
                answer,
                temperature=0,
                max_tokens=None,
                repeat_penalty=None,
                dry=None,
            )
        )
        asyncio.run(
            team._validate_indepth_verdict(
                client,
                "reviewer-model",
                chart=review_chart,
                gathered=state.gathered,
                answer_text=answer,
                claims=["Finding 1 [1]."],
                max_tokens=None,
                temperature=0,
                repeat_penalty=None,
                dry=None,
                validation_prompt=str(
                    profile.prompts.get("review") or "validation-rewrite"
                ),
            )
        )
        asyncio.run(
            team._synthesize_indepth(
                client,
                "writer-model",
                retry_messages,
                engine._prompt(profile, "indepth", "synthesis-indepth"),
                state.gathered,
                answer,
                temperature=0,
                max_tokens=None,
                repeat_penalty=None,
                dry=None,
                extra_msgs=retry_extra,
            )
        )
    finally:
        team.reset_chat_budget(token)

    assert len(client.requests) == 3
    assert [row["model"] for row in policy.measurements] == [
        "writer-model",
        "reviewer-model",
        "writer-model",
    ]
    assert [row["input_tokens"] for row in policy.measurements] == [6, 6, 6]
