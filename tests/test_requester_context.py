"""The signed-in account ChartSearchAI reports reaches the model as one requester line."""

import asyncio

from server import engine
from server.levels_loader import get_profile
from tests.factories import patient_source_registry

ACCOUNT = {
    "source": "openmrs_session",
    "user_uuid": "signed-in-account",
    "assigned_roles": ["Organizational: Nurse"],
    "effective_roles": ["Authenticated", "Organizational: Nurse"],
    "session_location": {"uuid": "location-1", "name": "Outpatient Clinic"},
    "locale": "en-KE",
}


class ExactWordCounter:
    async def count(self, _model: str, text: str) -> int:
        return len(text.split())

    async def count_records(self, _model: str, texts) -> tuple[int, ...]:
        return tuple(len(text.split()) for text in texts)


def _prepared(context):
    mappings = [
        {
            "resourceType": "Observation",
            "resourceUuid": "obs-1",
            "date": "2026-01-25",
            "text": "(2026-01-25) Weight: 71 kg",
        }
    ]
    registry = patient_source_registry("[1] (2026-01-25) Weight: 71 kg\n", mappings)
    state = engine._State(messages=[{"role": "user", "content": "What is the latest weight?"}])
    request = engine.ExecutionRequest(
        profile=get_profile("single-12b-checked"),
        messages=state.messages,
        patient="patient-1",
        context=context,
        source_registry=registry,
        token_counter=ExactWordCounter(),
    )
    asyncio.run(engine._prepare_context(request, state))
    _view, indepth_messages, _chart = asyncio.run(
        engine._select_indepth_context(request, state, "The latest weight is 71 kg [1].")
    )
    return state, indepth_messages


def _contents(messages):
    return "\n".join(str(message.get("content") or "") for message in messages)


def test_the_answer_and_indepth_messages_and_the_trace_name_the_requester():
    state, indepth_messages = _prepared({"account_context": ACCOUNT})

    line = "Requester: Organizational: Nurse at Outpatient Clinic"
    assert _contents(state.messages).count(line) == 1
    assert _contents(indepth_messages).count(line) == 1
    assert {"role": "requester", "line": line} in state.steps
    for private in ("signed-in-account", "Authenticated", "location-1", "en-KE"):
        assert private not in _contents(state.messages)


def test_without_a_session_account_nothing_about_the_requester_is_added():
    for context in (None, {}, {"account_context": {**ACCOUNT, "source": "unavailable"}}):
        state, indepth_messages = _prepared(context)

        assert "Requester:" not in _contents(state.messages) + _contents(indepth_messages)
        assert not [step for step in state.steps if step.get("role") == "requester"]
