from __future__ import annotations

import asyncio
import json
from dataclasses import replace

from server import engine
from server.levels_loader import get_profile
from tests.factories import patient_source_registry


def test_product_profile_defaults_temporal_anchor_to_wall_clock(monkeypatch):
    monkeypatch.delenv("HUB_ANCHOR", raising=False)
    request = engine.ExecutionRequest(
        profile=get_profile("single-e4b-checked"),
        messages=[{"role": "user", "content": "Question"}],
    )

    assert engine._temporal_anchor(request) == "wall_clock"


def test_fixed_evaluation_anchor_overrides_product_wall_clock_default(monkeypatch):
    monkeypatch.setenv("HUB_ANCHOR", "2026-06-20")
    request = engine.ExecutionRequest(
        profile=get_profile("single-e4b-checked"),
        messages=[{"role": "user", "content": "Question"}],
    )

    assert engine._temporal_anchor(request) == "2026-06-20"


def test_low_level_leg_keeps_latest_record_default_without_an_explicit_anchor(monkeypatch):
    monkeypatch.delenv("HUB_ANCHOR", raising=False)
    request = engine.ExecutionRequest(
        profile=get_profile("answer:gemma-4-12b"),
        messages=[{"role": "user", "content": "Question"}],
    )

    assert engine._temporal_anchor(request) is None


def test_product_context_resolves_temporal_date_once_for_drug_safety_and_facts(
    monkeypatch,
):
    from server import team, temporal

    class TinyCounter:
        async def count(self, _model, _text):
            return 1

    registry = patient_source_registry(
        "[1] (2026-07-10) Encounter: Visit\n",
        [
            {
                "resourceType": "Encounter",
                "resourceUuid": "enc-1",
                "date": "2026-07-10",
                "text": "(2026-07-10) Encounter: Visit",
            }
        ],
    )
    resolve_calls = []
    drug_dates = []
    fact_dates = []
    real_build = temporal.build_temporal_facts

    def resolve(anchor, chart, *, timezone_name=None):
        resolve_calls.append((anchor, timezone_name, chart))
        return "2026-07-10"

    def prepare_drugs(chart, mappings, _records, _question, reference_date, _enabled):
        drug_dates.append(reference_date)
        return chart, mappings, None

    def build_facts(chart, reference_date, *, anchor_mode=None):
        fact_dates.append(reference_date)
        return real_build(chart, reference_date, anchor_mode=anchor_mode)

    monkeypatch.setenv("HUB_TIMEZONE", "Pacific/Honolulu")
    monkeypatch.setattr(temporal, "resolve_anchor", resolve)
    monkeypatch.setattr(temporal, "build_temporal_facts", build_facts)
    monkeypatch.setattr(team, "_prepare_drug_safety", prepare_drugs)
    profile = replace(
        get_profile("single-e4b-checked"),
        context_window=2048,
        reserved_output_tokens=64,
    )
    request = engine.ExecutionRequest(
        profile=profile,
        messages=[{"role": "user", "content": "When was the visit?"}],
        patient="patient-1",
        source_registry=registry,
        token_counter=TinyCounter(),
    )
    state = engine._State(messages=[dict(item) for item in request.messages])

    asyncio.run(engine._prepare_context(request, state))

    assert len(resolve_calls) == 1
    assert resolve_calls[0][:2] == ("wall_clock", "Pacific/Honolulu")
    assert drug_dates == ["2026-07-10"]
    assert fact_dates == ["2026-07-10"]
    assert state.reference_date == "2026-07-10"


def test_blocking_adapter_drains_the_same_async_stage_engine(monkeypatch):
    request = engine.ExecutionRequest(
        profile=get_profile("answer:gemma-4-12b"),
        messages=[{"role": "user", "content": "Question"}],
    )
    calls = []

    async def fake_engine(actual_request):
        calls.append(actual_request)
        yield "result", '{"answer":"ok","citations":[],"blocks":[]}'

    monkeypatch.setattr(engine, "_execute_stages", fake_engine)

    assert asyncio.run(engine.drain_profile(request)) == (
        '{"answer":"ok","citations":[],"blocks":[]}'
    )
    assert calls == [request]


def test_duplicate_legacy_execution_entrypoints_are_removed():
    from server import team

    assert not hasattr(team, "run_team")
    assert not hasattr(team, "run_team_stream")
    assert not hasattr(team, "run_team_stage_drain")


def test_prompt_selection_does_not_shrink_temporal_gate_evidence():
    class EvidenceCounter:
        async def count(self, _model, text):
            return sum(
                1
                for line in text.splitlines()
                if len(line) > 2 and line[0] == "[" and line[1].isdigit()
            )

    mappings = [
        {
            "resourceType": "Observation",
            "resourceUuid": f"obs-{index}",
            "date": f"2026-01-0{index}",
            "text": (f"(2026-01-0{index}) Finding - Weight (kg): " f"{69 + index} kg"),
        }
        for index in range(1, 4)
    ]
    raw = [
        {
            "resourceType": item["resourceType"],
            "resourceUuid": item["resourceUuid"],
            "date": item["date"],
            "text": item["text"],
            "metadata": {"mandatory_context": index == 1},
        }
        for index, item in enumerate(mappings, 1)
    ]
    registry = patient_source_registry(
        "".join(
            f"[{index}] {item['text']}\n" for index, item in enumerate(mappings, 1)
        ),
        mappings,
        raw,
    )
    profile = replace(
        get_profile("single-e4b-checked"),
        context_window=3,
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
        token_counter=EvidenceCounter(),
    )

    asyncio.run(engine._prepare_context(request, state))

    assert state.view is not None and state.view.mode == "selected"
    assert len(state.view.records) == 2
    assert "test:Observation:obs-1" in state.view.included_ids
    assert state.view.record_indices == (1, 3)
    assert "[1]" in state.chart and "[3]" in state.chart and "[2]" not in state.chart
    assert [mapping["index"] for mapping in state.mappings] == [1, 2, 3]
    assert state.temporal_facts is not None
    weight_series = next(
        series
        for series in state.temporal_facts["numeric_series"]
        if "weight" in series["concept"].lower()
    )
    assert len(weight_series["points"]) == 3


def test_product_request_cannot_disable_answer_or_indepth_temporal_enforcement(
    monkeypatch,
):
    from server import team

    class TinyCounter:
        async def count(self, _model, _text):
            return 1

    registry = patient_source_registry(
        "[1] (2026-01-01) Encounter: Visit\n",
        [
            {
                "resourceType": "Encounter",
                "resourceUuid": "enc-1",
                "date": "2026-01-01",
                "text": "(2026-01-01) Encounter: Visit",
            }
        ],
    )

    async def fake_answer(*_args, **_kwargs):
        return "The documented visit was 2026-01-01 [1].", [1], []

    async def fake_review(_client, **kwargs):
        return (
            kwargs["answer_text"],
            kwargs["citations"],
            kwargs["blocks"],
            {"level": "green", "note": ""},
        )

    async def fake_indepth(*_args, **_kwargs):
        return ["The chart records a visit on 2026-01-01."], {
            "level": "green",
            "note": "",
        }

    async def fake_ground(_client, _model, _answer, references, _mappings):
        return references

    monkeypatch.setattr(team, "_synthesize_answer", fake_answer)
    monkeypatch.setattr(team, "_validate_and_refine_answer", fake_review)
    monkeypatch.setattr(team, "_gen_indepth", fake_indepth)
    monkeypatch.setattr(team, "_ground_references", fake_ground)
    monkeypatch.setattr(team, "_write_trace", lambda *_args, **_kwargs: None)

    request = engine.ExecutionRequest(
        profile=get_profile("single-e4b-checked"),
        messages=[{"role": "user", "content": "When was the visit?"}],
        patient="patient-1",
        context={"temporal": False, "temporal_gate": "off"},
        source_registry=registry,
        token_counter=TinyCounter(),
    )

    async def collect():
        return [
            (name, json.loads(data))
            async for name, data in engine.execute_profile(request)
        ]

    events = dict(asyncio.run(collect()))
    assert events["answer_done"]["temporalGate"]["mode"] == "enforce"
    assert events["done"]["temporalGate"]["mode"] == "enforce"
    assert events["done"]["inDepth"]["validation"]["mode"] == "enforce"


def test_product_pipeline_fallback_records_enforced_temporal_gate(monkeypatch):
    from server import team

    class TinyCounter:
        async def count(self, _model, _text):
            return 1

    registry = patient_source_registry(
        "[1] (2026-01-01) Encounter: Visit\n",
        [
            {
                "resourceType": "Encounter",
                "resourceUuid": "enc-1",
                "date": "2026-01-01",
                "text": "(2026-01-01) Encounter: Visit",
            }
        ],
    )
    traces = []

    async def fail_answer(*_args, **_kwargs):
        raise RuntimeError("writer failed")

    monkeypatch.setattr(team, "_synthesize_answer", fail_answer)
    monkeypatch.setattr(
        team, "_write_trace", lambda *_args, **kwargs: traces.append(kwargs)
    )

    request = engine.ExecutionRequest(
        profile=get_profile("single-e4b-checked"),
        messages=[{"role": "user", "content": "When was the visit?"}],
        patient="patient-1",
        source_registry=registry,
        token_counter=TinyCounter(),
    )

    async def collect():
        return [
            (name, json.loads(data))
            async for name, data in engine.execute_profile(request)
        ]

    events = dict(asyncio.run(collect()))
    gate = events["done"]["temporalGate"]
    assert gate["mode"] == "enforce"
    assert gate["applied"] == "fallback"
    assert events["done"]["answerValidation"]["status"] == "needs_review"
    assert traces[0]["temporal_gate"] == gate


def test_oversized_bare_inline_chart_is_replaced_not_duplicated():
    from server.context_sources import is_chart_message

    class ChartLineCounter:
        async def count(self, _model, text):
            return sum(
                1
                for line in text.splitlines()
                if len(line) > 2 and line[0] == "[" and line[1].isdigit()
            )

    chart = "Patient: 28-year-old Female\n" + "".join(
        f"[{index}] (2026-01-0{index}) Finding - Weight: {69 + index} kg\n"
        for index in range(1, 4)
    )
    profile = replace(
        get_profile("single-e4b-checked"),
        context_window=3,
        reserved_output_tokens=1,
    )
    state = engine._State(
        messages=[
            {"role": "system", "content": "system"},
            {"role": "user", "content": chart},
            {"role": "user", "content": "What is the latest weight?"},
        ]
    )
    request = engine.ExecutionRequest(
        profile=profile,
        messages=state.messages,
        token_counter=ChartLineCounter(),
    )

    asyncio.run(engine._prepare_context(request, state))

    chart_messages = [
        message for message in state.messages if is_chart_message(message)
    ]
    assert len(chart_messages) == 1
    assert state.view is not None and state.view.mode == "selected"
    assert "Patient: 28-year-old Female" in chart_messages[0]["content"]
    assert chart_messages[0]["content"].count("\n[") == len(state.view.records)
