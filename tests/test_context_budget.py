"""Executable acceptance checks for exact deterministic context budgeting."""

import asyncio
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
