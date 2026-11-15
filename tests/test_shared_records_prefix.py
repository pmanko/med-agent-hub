"""Every stage of one answer starts with the same context message, so the router reads it once."""

import asyncio

from server import engine
from server import team as stages
from server.context_sources import ContextBudget
from server.levels_loader import compile_execution_spec
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
        profile=compile_execution_spec("gemma-4-12b", {"mode": "verified"}),
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


def test_temporal_facts_prefix_is_invariant_to_appended_reference_or_safety_records():
    from server.temporal import build_temporal_facts, render_temporal_facts

    base_chart = (
        "Patient records (most recent first):\n"
        "[1] (2026-06-15) Weight (kg): 71 kg\n"
        "[2] (2025-11-10) Drug order: Lamivudine 300 MG. Action: NEW\n"
    )
    with_supplemental = (
        base_chart
        + "[3] KnowledgeReference WHO HIV Guidelines: First-line ART guidance.\n"
        + "[4] Drug reference — Lamivudine (3TC; NRTI). Standard dose: 300 mg daily.\n"
        + "[5] Safety finding — Lamivudine: renal dose adjustment note.\n"
        + "[6] Active drug order: Lamivudine 300 MG\n"
    )
    base_rendered = render_temporal_facts(build_temporal_facts(base_chart, "2026-06-20"))
    supp_rendered = render_temporal_facts(build_temporal_facts(with_supplemental, "2026-06-20"))
    assert base_rendered == supp_rendered


def test_prefix_message_stays_byte_identical_across_questions_and_turns_with_supplemental_and_selected_records():
    mappings = [
        {
            "resourceType": "DrugOrder",
            "resourceUuid": "order-d4t",
            "date": "2026-01-26",
            "text": "(2026-01-26) Drug order: Stavudine 30 MG. Action: NEW. Order status: in force",
        },
        {
            "resourceType": "DrugOrder",
            "resourceUuid": "order-3tc",
            "date": "2026-01-26",
            "text": "(2026-01-26) Drug order: Lamivudine 150 MG. Action: NEW. Order status: in force",
        },
        *[
            {
                "resourceType": "Observation",
                "resourceUuid": f"obs-{idx}",
                "date": f"2026-01-{10 + idx:02d}",
                "text": f"(2026-01-{10 + idx:02d}) Finding — Weight (kg): {60 + idx} kg",
            }
            for idx in range(1, 10)
        ],
        {
            "resourceType": "Observation",
            "resourceUuid": "obs-return",
            "date": "2026-01-26",
            "text": "(2026-01-26) Return visit date: 2026-02-23",
        },
        {
            "resourceType": "AllergyIntolerance",
            "resourceUuid": "allergy-sulfa",
            "date": "2025-09-09",
            "text": "(2025-09-09) Allergy: Sulfonamides — Rash (severe)",
        },
        {
            "resourceType": "Patient",
            "resourceUuid": "patient-zabella",
            "date": "2025-09-09",
            "text": "(2025-09-09) Patient: Zabella Talai Halambe. Female. Born 1998-05-30 (28 years old)",
        },
    ]
    raw_records = [
        {
            "resourceType": m["resourceType"],
            "resourceUuid": m["resourceUuid"],
            "date": m["date"],
            "text": m["text"],
            "metadata": {"drug_name": "Stavudine"} if m["resourceUuid"] == "order-d4t" else {},
        }
        for m in mappings
    ]
    chart = "".join(f"[{i}] {m['text']}\n" for i, m in enumerate(mappings, start=1))
    registry = patient_source_registry(chart, mappings, raw_records)
    profile = compile_execution_spec("gemma-4-12b", {"mode": "verified"})

    # Turn 1: Regimen + WHO guideline question (triggers KnowledgeReference + DrugReference)
    state_t1 = engine._State(
        messages=[
            {
                "role": "user",
                "content": "How does her stavudine regimen compare with WHO recommendations?",
            }
        ]
    )
    req_t1 = engine.ExecutionRequest(
        profile=profile,
        messages=state_t1.messages,
        patient="patient-zabella",
        context={"account_context": ACCOUNT},
        source_registry=registry,
        token_counter=ExactWordCounter(),
    )
    asyncio.run(engine._prepare_context(req_t1, state_t1))

    # Turn 2: Multi-turn follow-up about weight and return visit (different selected records & KB)
    state_t2 = engine._State(
        messages=[
            {
                "role": "user",
                "content": "How does her stavudine regimen compare with WHO recommendations?",
            },
            {
                "role": "assistant",
                "content": "She is taking stavudine and lamivudine [1][2], and WHO recommends phasing out stavudine.",
            },
            {
                "role": "user",
                "content": "What is her latest weight and return visit date?",
            },
        ]
    )
    req_t2 = engine.ExecutionRequest(
        profile=profile,
        messages=state_t2.messages,
        patient="patient-zabella",
        context={"account_context": ACCOUNT},
        source_registry=registry,
        token_counter=ExactWordCounter(),
    )
    asyncio.run(engine._prepare_context(req_t2, state_t2))

    # Turn 3: Age/birth question that specifically selects the Patient record
    state_t3 = engine._State(
        messages=[
            *state_t2.messages[1:],
            {
                "role": "assistant",
                "content": "Her latest weight is 69 kg and return visit date is 2026-02-23.",
            },
            {
                "role": "user",
                "content": "How old is Zabella and when was she born?",
            },
        ]
    )
    req_t3 = engine.ExecutionRequest(
        profile=profile,
        messages=state_t3.messages,
        patient="patient-zabella",
        context={"account_context": ACCOUNT},
        source_registry=registry,
        token_counter=ExactWordCounter(),
    )
    asyncio.run(engine._prepare_context(req_t3, state_t3))

    # msg[0] must be 100% byte-for-byte identical across Turn 1, Turn 2, and Turn 3
    assert str(state_t1.messages[0]["content"]) == str(state_t2.messages[0]["content"])
    assert str(state_t1.messages[0]["content"]) == str(state_t3.messages[0]["content"])
    assert "Allergy: Sulfonamides" in state_t1.messages[0]["content"]
    assert "KnowledgeReference" not in state_t1.messages[0]["content"]
    assert "Drug reference" not in state_t1.messages[0]["content"]

    # Question-specific records and KnowledgeReference/DrugReference live in the trailing turn message
    t1_trailing = stages.with_gathered("Answer instruction.", state_t1.gathered, state_t1.messages)
    t2_trailing = stages.with_gathered("Answer instruction.", state_t2.gathered, state_t2.messages)
    t3_trailing = stages.with_gathered("Answer instruction.", state_t3.gathered, state_t3.messages)
    assert "[1] (2026-01-26) Drug order: Stavudine" in t1_trailing
    assert "KnowledgeReference" in t1_trailing
    assert "Return visit date: 2026-02-23" in t2_trailing
    assert "Patient: Zabella Talai Halambe" in t3_trailing

    # Review and In-Depth validation messages on Turn 1 share the exact same prefix msg[0]
    review_msgs = stages._answer_validation_messages(
        chart=state_t1.chart,
        gathered=state_t1.gathered,
        answer_text=ANSWER,
    )
    indepth_review_msgs = stages._indepth_validation_messages(
        chart=state_t1.chart,
        gathered=state_t1.gathered,
        answer_text=ANSWER,
        claims=CLAIMS,
    )
    assert str(review_msgs[0]["content"]) == str(indepth_review_msgs[0]["content"])
    assert "[1] (2026-01-26) Drug order: Stavudine" in review_msgs[1]["content"]
    assert "[1] (2026-01-26) Drug order: Stavudine" in indepth_review_msgs[1]["content"]


def test_select_answer_context_reserves_multi_turn_headroom_for_large_temporal_prefix():
    from dataclasses import replace
    from server.context_sources import ContextBudget, EvidenceLedger, EvidenceRecord

    records = tuple(
        EvidenceRecord(
            stable_id=f"rec-{idx}",
            source="test-patient",
            source_priority=10,
            resource_type="Observation",
            resource_uuid=f"uuid-{idx}",
            date="2026-01-20",
            text=f"(2026-01-20) Finding — weight check {idx}: " + ("word " * 180),
        )
        for idx in range(1, 30)
    ) + (
        EvidenceRecord(
            stable_id="rec-irrelevant",
            source="test-patient",
            source_priority=10,
            resource_type="Observation",
            resource_uuid="uuid-irrelevant",
            date=None,
            text="Unrelated administrative note",
        ),
    )
    profile = replace(
        compile_execution_spec("gemma-4-12b", {"mode": "quick"}),
        context_window=20480,
        reserved_output_tokens=4096,
    )
    budget = ContextBudget(
        context_window=profile.context_window,
        reserved_output_tokens=profile.reserved_output_tokens,
    )
    state = engine._State(
        messages=[{"role": "user", "content": "Show weight check history"}],
        ledger=EvidenceLedger(records),
        gathered="T" * 16500,
        token_counter=ExactWordCounter(),
        context_budget=budget,
    )
    req = engine.ExecutionRequest(
        profile=profile,
        messages=state.messages,
        patient="patient-1",
        token_counter=state.token_counter,
    )

    asyncio.run(engine._select_answer_context(req, state))

    assert state.view is not None
    assert state.view.input_limit == budget.input_limit
    assert state.view.input_tokens <= budget.input_limit - engine._MULTI_TURN_HEADROOM_TOKENS

