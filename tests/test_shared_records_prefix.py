"""Every stage of one answer starts with the same context message, so the router reads it once."""

import asyncio

from server import engine
from server import team as stages
from server.context_sources import ContextBudget
from server.levels_loader import get_profile
from tests.factories import patient_source_registry

ACCOUNT = {
    "source": "openmrs_session",
    "assigned_roles": ["Organizational: Nurse"],
    "session_location": {"uuid": "location-1", "name": "Outpatient Clinic"},
}
ANSWER = "The latest weight is 71 kg [1]."
CLAIMS = ["Weight is stable [1]."]


class ExactWordCounter:
    async def count(self, _model: str, text: str) -> int:
        return len(text.split())

    async def count_records(self, _model: str, texts) -> tuple[int, ...]:
        return tuple(len(text.split()) for text in texts)


def _prepared():
    mappings = [
        {
            "resourceType": "Observation",
            "resourceUuid": f"obs-{day}",
            "date": f"2026-01-{day}",
            "text": f"(2026-01-{day}) Weight: {40 + day} kg",
        }
        for day in (25, 20, 15)
    ]
    chart = "".join(f"[{i}] {m['text']}\n" for i, m in enumerate(mappings, start=1))
    state = engine._State(messages=[{"role": "user", "content": "What is the latest weight?"}])
    request = engine.ExecutionRequest(
        profile=get_profile("single-12b-checked"),
        messages=state.messages,
        patient="patient-1",
        context={"account_context": ACCOUNT},
        source_registry=patient_source_registry(chart, mappings),
        token_counter=ExactWordCounter(),
    )
    asyncio.run(engine._prepare_context(request, state))
    return request, state


def test_every_stage_starts_with_the_same_evidence_and_records_and_keeps_its_instructions_after_them():
    request, state = _prepared()
    _view, indepth, _chart = asyncio.run(engine._select_indepth_context(request, state, ANSWER))
    review = stages._answer_validation_messages(
        chart=asyncio.run(engine._select_answer_review_context(request, state, ANSWER, [])),
        gathered=state.gathered,
        answer_text=ANSWER,
    )
    indepth_review = stages._indepth_validation_messages(
        chart=asyncio.run(engine._select_indepth_review_context(request, state, ANSWER, CLAIMS)),
        gathered=state.gathered,
        answer_text=ANSWER,
        claims=CLAIMS,
    )
    records = stages.records_message(state.chart, state.gathered)

    assert "Requester:" not in records["content"]
    assert state.messages[0]["content"] == (
        records["content"] + "\n\nRequester: Organizational: Nurse at Outpatient Clinic"
    )
    assert indepth[0] == state.messages[0]
    assert review[0] == indepth_review[0] == records
    assert state.gathered and records["content"].startswith(state.gathered)
    for messages in (state.messages, indepth, review, indepth_review):
        text = "\n".join(str(m.get("content")) for m in messages)
        assert text.count(state.gathered) == 1
    assert [s["stage"] for s in state.steps if s.get("mode") == "answer_records"] == [
        "indepth",
        "answer_review",
        "indepth_review",
    ]


def test_a_stage_whose_prompt_does_not_fit_the_answers_records_selects_its_own():
    request, state = _prepared()
    full = asyncio.run(engine._select_indepth_review_context(request, state, ANSWER, CLAIMS))
    fitted_tokens = state.steps[-1]["input_tokens"]
    state.context_budget = ContextBudget(context_window=fitted_tokens - 1, reserved_output_tokens=0)
    state.steps.clear()

    chart = asyncio.run(engine._select_indepth_review_context(request, state, ANSWER, CLAIMS))

    assert full == state.chart
    assert not [s for s in state.steps if s.get("mode") == "answer_records"]
    assert chart != state.chart
