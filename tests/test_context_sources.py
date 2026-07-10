from __future__ import annotations

from dataclasses import dataclass

import pytest

from server.context_sources import (
    ContextBudget,
    ContextRequest,
    ContextSourceError,
    EvidenceLedger,
    EvidenceRecord,
    InlineChartSource,
    InsufficientContextError,
    SourceRegistry,
    select_context,
)


class WordTokenCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def count(self, model: str, text: str) -> int:
        self.calls.append((model, text))
        return len(text.split())


def _messages(chart: str = "") -> list[dict[str, str]]:
    messages = [{"role": "system", "content": "system"}]
    if chart:
        messages.append(
            {
                "role": "user",
                "content": "Patient records (most recent first):\n" + chart,
            }
        )
    messages.append({"role": "user", "content": "What is the latest weight?"})
    return messages


@pytest.mark.asyncio
async def test_inline_context_works_without_querystore_configuration():
    chart = "[1] (2026-01-01) Weight: 70 kg\n[2] (2025-01-01) Weight: 68 kg\n"
    registry = SourceRegistry([InlineChartSource()])

    ledger = await registry.build_ledger(ContextRequest(messages=_messages(chart)))

    assert ledger.source_names == ("inline",)
    assert ledger.render() == chart
    assert [record.stable_id for record in ledger.records] == ["inline:1", "inline:2"]


@pytest.mark.asyncio
async def test_patient_without_a_patient_source_or_inline_chart_fails_explicitly():
    registry = SourceRegistry([InlineChartSource()])

    with pytest.raises(ContextSourceError) as caught:
        await registry.build_ledger(
            ContextRequest(patient="patient-1", messages=_messages())
        )

    assert caught.value.code == "context_source_unavailable"
    assert caught.value.source == "auto"


@dataclass
class AlternateSource:
    name: str = "alternate"
    priority: int = 20
    supports_patient: bool = True

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        return EvidenceLedger(
            records=(
                EvidenceRecord(
                    stable_id=f"alternate:{request.patient}",
                    source="alternate",
                    source_priority=self.priority,
                    resource_type="Observation",
                    resource_uuid="obs-1",
                    date="2026-01-02",
                    text="(2026-01-02) Weight: 71 kg",
                ),
            )
        )


@pytest.mark.asyncio
async def test_mock_alternate_patient_source_uses_the_same_contract():
    registry = SourceRegistry([InlineChartSource(), AlternateSource()])

    ledger = await registry.build_ledger(
        ContextRequest(patient="patient-1", messages=_messages(), source="alternate")
    )

    assert ledger.source_names == ("alternate",)
    assert ledger.records[0].resource_uuid == "obs-1"


def _ledger(*records: EvidenceRecord) -> EvidenceLedger:
    return EvidenceLedger(records=records)


@pytest.mark.asyncio
async def test_small_chart_is_preserved_byte_for_byte_with_exact_counting():
    ledger = _ledger(
        EvidenceRecord("r1", "inline", 10, "Obs", "1", "2026-01-02", "(2026-01-02) Weight: 71 kg"),
        EvidenceRecord("r2", "inline", 10, "Obs", "2", "2025-01-02", "(2025-01-02) Weight: 69 kg"),
    )
    counter = WordTokenCounter()

    view = await select_context(
        ledger,
        question="latest weight",
        model="gemma-e4b",
        budget=ContextBudget(context_window=100, reserved_output_tokens=10),
        counter=counter,
        fixed_text="system question",
    )

    assert view.mode == "full"
    assert view.render() == ledger.render()
    assert view.excluded == ()
    assert counter.calls


@pytest.mark.asyncio
async def test_oversized_selection_is_stable_and_records_reasons():
    ledger = _ledger(
        EvidenceRecord("safety", "drug", 100, "Safety", "s", "2020-01-01", "Allergy: ibuprofen", mandatory=True),
        EvidenceRecord("exact", "inline", 10, "Obs", "e", "2024-01-01", "Weight: 71 kg code WT-71"),
        EvidenceRecord("recent", "inline", 10, "Encounter", "r", "2026-01-01", "Routine visit"),
        EvidenceRecord("old", "inline", 10, "Encounter", "o", "2019-01-01", "Old routine visit"),
    )
    counter = WordTokenCounter()
    budget = ContextBudget(context_window=19, reserved_output_tokens=4)

    first = await select_context(
        ledger, question="What is weight WT-71?", model="gemma-e4b",
        budget=budget, counter=counter, fixed_text="fixed prompt",
    )
    second = await select_context(
        ledger, question="What is weight WT-71?", model="gemma-e4b",
        budget=budget, counter=counter, fixed_text="fixed prompt",
    )

    assert first == second
    assert first.mode == "selected"
    assert first.included_ids[:2] == ("safety", "exact")
    assert first.excluded
    assert all(item.reason for item in first.excluded)


@pytest.mark.asyncio
async def test_mandatory_context_overflow_abstains_instead_of_truncating():
    ledger = _ledger(
        EvidenceRecord(
            "safety", "drug", 100, "Safety", "s", "2020-01-01",
            "mandatory safety evidence that cannot be dropped", mandatory=True,
        )
    )

    with pytest.raises(InsufficientContextError) as caught:
        await select_context(
            ledger,
            question="question",
            model="gemma-e4b",
            budget=ContextBudget(context_window=5, reserved_output_tokens=2),
            counter=WordTokenCounter(),
            fixed_text="fixed prompt",
        )

    assert caught.value.code == "insufficient_context"
    assert caught.value.mandatory_ids == ("safety",)
