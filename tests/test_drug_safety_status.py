"""Hub-side adapter for the shared drug_safety_status conformance fixture.

Drives server.drug_safety.check_answer_safety() against the fixture's 3 scenarios (per the
conformance contract's Temporal, Citation, and Safety Contract: checked/limited/unavailable are
honest states — neither an empty warning list nor a missing source package implies checked). The
fixture's mapping_complete/exposure_complete/execution_complete flags are conceptual; this test
translates each case into the closest concrete real-code scenario:

- complete-check-is-checked: a loaded dataset + a resolved patient context, all three check
  categories enabled.
- partial-check-is-limited: dataset and context are both available, but the caller only asked for
  a subset of the checks (warn_interactions=False) — a deliberately partial, specifically
  described check, not a silent failure.
- missing-package-is-unavailable: no dataset and no patient context at all, mirroring
  team.py._compute_safety_warnings's real "no patient ref / querystore retrieval failed" path.
"""

import json
from pathlib import Path

from server import drug_safety as ds

FIXTURE = Path(__file__).resolve().parent / "conformance" / "dual-provider-conformance.v1.json"


def _case(case_id: str) -> dict:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for candidate in payload["drug_safety_status"]:
        if candidate["id"] == case_id:
            return candidate
    raise AssertionError(f"no drug_safety_status fixture case {case_id!r}")


def test_complete_check_is_checked():
    case = _case("drug-safety.complete-check-is-checked")
    dataset = ds.load_dataset()
    context = ds.PatientClinicalContext(age_years=30)

    result = ds.check_answer_safety("Ibuprofen 200 mg as needed.", None, context, dataset)

    assert result.status == case["expected_status"], (
        f"{case['id']}: expected {case['expected_status']!r}, got {result.status!r}")


def test_partial_check_is_limited():
    case = _case("drug-safety.partial-check-is-limited")
    dataset = ds.load_dataset()
    context = ds.PatientClinicalContext(age_years=30)

    result = ds.check_answer_safety(
        "Ibuprofen 200 mg as needed.", None, context, dataset, warn_interactions=False)

    assert result.status == case["expected_status"], (
        f"{case['id']}: expected {case['expected_status']!r}, got {result.status!r}")


def test_missing_source_package_is_unavailable():
    case = _case("drug_safety.missing-package-is-unavailable")

    result = ds.check_answer_safety("Ibuprofen 200 mg as needed.", None, None, None)

    assert result.status == case["expected_status"], (
        f"{case['id']}: expected {case['expected_status']!r}, got {result.status!r}")


def test_checked_status_still_surfaces_real_warnings():
    """The status is orthogonal to warning content — a checked result can still flag something."""
    dataset = ds.load_dataset()
    context = ds.PatientClinicalContext(age_years=5)

    result = ds.check_answer_safety(
        "Ibuprofen 600 mg every 6 hours can be given for pain.", None, context, dataset)

    assert result.status == "checked"
    assert any(w.type == "overdose" for w in result.warnings)
