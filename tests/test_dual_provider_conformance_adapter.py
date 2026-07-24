"""Hub-side adapter for the shared dual-provider conformance fixture
(dual-provider-conformance.v1.json, synced locally under tests/conformance/).

Per specs/artifacts/planning/openmrs-dual-provider-conformance-contract.md's Red-First Test
Procedure: each owning repository adds a test that consumes the fixture directly (not a
hand-duplicated reimplementation of its scenarios) so a fixture change or an implementation
regression shows up here without needing a second, independently-authored test.

Run: `pytest tests/test_dual_provider_conformance_adapter.py` (see targets/med-agent-hub's own
test running instructions for the dependency set — matches the existing test_temporal.py suite).
"""

import json
from pathlib import Path

import pytest

from server import temporal

FIXTURE = Path(__file__).resolve().parent / "conformance" / "dual-provider-conformance.v1.json"


def _load_cases(family: str):
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return payload[family]


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
