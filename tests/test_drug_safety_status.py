"""Hub-side adapter for the shared drug_safety_status conformance fixture.

Drives server.drug_safety.check_answer_safety() directly from every completeness field in the
shared fixture. The adapter must not substitute an unrelated disabled-check toggle for a missing
medication mapping: package review, mapping coverage, exposure coverage, and execution are distinct
parts of the safety result contract.
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


def _dataset(case: dict) -> ds.DrugReferenceDataset:
    loaded = ds.load_dataset()
    return ds.DrugReferenceDataset(
        loaded.entries,
        loaded.cross_reactivity_groups,
        package_id=case["source_package"],
        source_format="json",
        source_version="fixture-v1",
        provenance={"fixture": FIXTURE.name},
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        cross_reactivity_review_state=ds.REVIEW_CLINICALLY_APPROVED,
    )


def _context(case: dict) -> ds.PatientClinicalContext:
    return ds.PatientClinicalContext(
        age_years=30,
        mapping_complete=case["mapping_complete"],
        exposure_complete=case["exposure_complete"],
        active_order_count=1,
        mapped_active_order_count=1 if case["mapping_complete"] else 0,
    )


def test_complete_check_is_checked():
    case = _case("drug-safety.complete-check-is-checked")
    dataset = _dataset(case)
    context = _context(case)

    result = ds.check_answer_safety("Ibuprofen 200 mg as needed.", None, context, dataset)

    assert result.status == case["expected_status"], (
        f"{case['id']}: expected {case['expected_status']!r}, got {result.status!r}")


def test_partial_check_is_limited():
    case = _case("drug-safety.partial-check-is-limited")
    dataset = _dataset(case)
    context = _context(case)

    result = ds.check_answer_safety("Ibuprofen 200 mg as needed.", None, context, dataset)

    assert result.status == case["expected_status"], (
        f"{case['id']}: expected {case['expected_status']!r}, got {result.status!r}")


def test_missing_source_package_is_unavailable():
    case = _case("drug_safety.missing-package-is-unavailable")

    result = ds.check_answer_safety("Ibuprofen 200 mg as needed.", None, None, None)

    assert result.status == case["expected_status"], (
        f"{case['id']}: expected {case['expected_status']!r}, got {result.status!r}")


def test_checked_status_still_surfaces_real_warnings():
    """The status is orthogonal to warning content — a checked result can still flag something."""
    case = _case("drug-safety.complete-check-is-checked")
    dataset = _dataset(case)
    context = ds.PatientClinicalContext(
        age_years=5,
        mapping_complete=True,
        exposure_complete=True,
    )

    result = ds.check_answer_safety(
        "Ibuprofen 600 mg every 6 hours can be given for pain.", None, context, dataset)

    assert result.status == "checked"
    assert any(w.type == "overdose" for w in result.warnings)


def test_unreviewed_seed_is_limited_and_cannot_emit_product_warnings():
    dataset = ds.load_dataset()
    context = ds.PatientClinicalContext(
        age_years=5,
        mapping_complete=True,
        exposure_complete=True,
    )

    result = ds.check_answer_safety(
        "Ibuprofen 600 mg every 6 hours can be given for pain.", None, context, dataset)

    assert result.status == "limited"
    assert result.warnings == []
    assert result.package["review_state"] == ds.REVIEW_PROPOSED
    assert result.coverage == {
        "mapping_complete": True,
        "exposure_complete": True,
        "execution_complete": True,
        "active_order_count": 0,
        "mapped_active_order_count": 0,
    }
    assert result.identity_confidence == "high"
    assert "source_not_clinically_approved" in result.issues


def test_mapping_and_exposure_failures_are_reported_separately():
    case = _case("drug-safety.complete-check-is-checked")
    dataset = _dataset(case)
    context = ds.PatientClinicalContext(
        age_years=30,
        mapping_complete=False,
        exposure_complete=False,
        active_order_count=2,
        mapped_active_order_count=1,
    )

    result = ds.check_answer_safety("No medication recommendation.", None, context, dataset)

    assert result.status == "limited"
    assert result.coverage["mapping_complete"] is False
    assert result.coverage["exposure_complete"] is False
    assert result.identity_confidence == "limited"
    assert result.issues == ["mapping_incomplete", "exposure_incomplete"]


def test_result_serializes_one_canonical_safety_check_object():
    case = _case("drug-safety.complete-check-is-checked")
    result = ds.check_answer_safety(
        "No medication recommendation.", None, _context(case), _dataset(case))

    assert result.to_dict() == {
        "schema_version": "drug_safety.v1",
        "status": "checked",
        "warnings": [],
        "package": {
            "id": "reviewed-package-v1",
            "source_format": "json",
            "version": "fixture-v1",
                "provenance": {"fixture": FIXTURE.name},
                "review_state": "clinically_approved",
                "cross_reactivity_review_state": "clinically_approved",
            },
        "coverage": {
            "mapping_complete": True,
            "exposure_complete": True,
            "execution_complete": True,
            "active_order_count": 1,
            "mapped_active_order_count": 1,
        },
        "identity_confidence": "high",
        "issues": [],
    }
