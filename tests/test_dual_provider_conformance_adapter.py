"""Hub-side adapter for the shared dual-provider conformance fixture
(dual-provider-conformance.v1.json, synced locally under tests/conformance/).

Per specs/artifacts/planning/openmrs-dual-provider-conformance-contract.md's Red-First Test
Procedure: each owning repository adds a test that consumes the fixture directly (not a
hand-duplicated reimplementation of its scenarios) so a fixture change or an implementation
regression shows up here without needing a second, independently-authored test.

Run: `pytest tests/test_dual_provider_conformance_adapter.py` (see targets/med-agent-hub's own
test running instructions for the dependency set — matches the existing test_temporal.py suite).
"""

import asyncio
import json
from pathlib import Path

import pytest

from server import temporal
from server.context_sources import (
    ContextBudget,
    EvidenceLedger,
    EvidenceRecord,
    InsufficientContextError,
    select_context,
)

FIXTURE = Path(__file__).resolve().parent / "conformance" / "dual-provider-conformance.v1.json"


def _load_cases(family: str):
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return payload[family]


def _case(family: str, case_id: str) -> dict:
    for case in _load_cases(family):
        if case["id"] == case_id:
            return case
    raise AssertionError(f"no {family} fixture case {case_id!r}")


class _ExactWordCounter:
    """Word-count-as-token-count test double, matching test_context_budget.py's own."""

    async def count(self, _model: str, text: str) -> int:
        return len(text.split())

    async def count_records(self, _model: str, texts) -> tuple:
        return tuple(len(text.split()) for text in texts)


def _temporal_facts_for(case: dict) -> dict:
    date_ledger = [{"iso": iso} for iso in case.get("ledger_dates", [])]
    numeric_series = []
    if "numeric_series" in case:
        points = [
            {"date": point["date"], "value": point["value"]}
            for point in case["numeric_series"]
        ]
        numeric_series.append(
            {
                "concept": case["concept"],
                "points": points,
                "trend_supported": len(points) >= 2,
                "direction": None,
            }
        )
    return {
        "date_output_contract": {},
        "date_ledger": date_ledger,
        "numeric_series": numeric_series,
        "appointment_candidates": {},
    }


@pytest.mark.parametrize("case", _load_cases("temporal_gate"), ids=lambda case: case["id"])
def test_temporal_gate_fixture_case(case):
    facts = _temporal_facts_for(case)

    result = temporal.run_temporal_gate("", case["answer"], [], facts, "enforce")

    assert result["status"] == case["expected_status"], (
        f"{case['id']}: expected status {case['expected_status']!r}, got {result['status']!r} "
        f"(checks: {result['checks']})"
    )


def test_mandatory_overflow_abstains_per_fixture():
    """context.mandatory-overflow-abstains: mandatory evidence alone exceeding the budget must
    raise insufficient_context, never a silent truncation. select_context is the real production
    entry point (server/context_sources.py) — this only supplies real inputs sized from the
    fixture's own budget_tokens/mandatory_tokens numbers, not a hand-duplicated reimplementation.
    """
    case = _case("context_policy", "context.mandatory-overflow-abstains")
    assert case["expected"] == "insufficient_context"

    mandatory_text = " ".join(["word"] * case["mandatory_tokens"])
    ledger = EvidenceLedger((
        EvidenceRecord(
            stable_id="mandatory-1",
            source="fixture",
            source_priority=10,
            resource_type="Observation",
            resource_uuid="mandatory-1",
            date="2026-01-01",
            text=mandatory_text,
            mandatory=True,
        ),
    ))

    with pytest.raises(InsufficientContextError) as caught:
        asyncio.run(
            select_context(
                ledger,
                question="question",
                model="fixture-model",
                budget=ContextBudget(context_window=case["budget_tokens"], reserved_output_tokens=0),
                counter=_ExactWordCounter(),
                fixed_text="",
            )
        )

    assert caught.value.mandatory_ids == ("mandatory-1",)
