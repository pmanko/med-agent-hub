"""Hub-side adapter for the shared drug_safety_status conformance fixture.

Drives server.drug_safety.check_answer_safety() directly from every completeness field in the
shared fixture. The adapter must not substitute an unrelated disabled-check toggle for a missing
medication mapping: package review, mapping coverage, exposure coverage, and execution are distinct
parts of the safety result contract.
"""

import json
from pathlib import Path

import pytest

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
            "issues": [],
            "cross_reactivity_review_state": "clinically_approved",
            "cross_reactivity": {
                "id": "in-memory-cross-reactivity",
                "source_format": "memory",
                "version": None,
                "provenance": {},
                "review_state": "clinically_approved",
                "issues": [],
            },
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


def test_malformed_nested_rules_are_removed_before_validation():
    issues = []
    entries = ds._entries_from_document(
        {
            "entries": [
                {
                    "id": "unsafe",
                    "name": "Unsafe Drug",
                    "aliases": ["unsafe"],
                    "atcCodes": ["NOT-ATC"],
                    "interactions": [{"token": ""}],
                },
                {
                    "id": "safe",
                    "name": "Safe Drug",
                    "aliases": ["safe"],
                    "atcCodes": ["M01AE01"],
                },
            ]
        },
        issues,
    )

    assert [entry.id for entry in entries] == ["unsafe", "safe"]
    assert entries[0].atc_codes == []
    assert entries[0].interactions == []
    assert "source_data_partially_invalid" in issues


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("minYears", "2"),
        ("minYears", 2.0),
        ("maxYears", True),
        ("mgPerKgMin", "1"),
        ("mgPerKgMax", False),
        ("maxDailyDoseMg", float("inf")),
        ("maxDailyDoseMg", 10**1000),
    ],
)
def test_age_band_numbers_require_the_same_json_types_as_java(field, value):
    band = {
        "minYears": 2,
        "maxYears": 11,
        "mgPerKgMin": 5,
        "mgPerKgMax": 10.5,
        "maxDailyDoseMg": 1200,
    }
    band[field] = value
    issues = []

    entries = ds._entries_from_document(
        {
            "entries": [
                {
                    "id": "ibuprofen",
                    "name": "Ibuprofen",
                    "aliases": ["ibuprofen"],
                    "ageBands": [band],
                }
            ]
        },
        issues,
    )

    assert entries[0].age_bands == []
    assert "source_data_partially_invalid" in issues


def test_age_band_bounds_must_fit_the_shared_32_bit_contract():
    issues = []
    entries = ds._entries_from_document(
        {
            "entries": [
                {
                    "id": "ibuprofen",
                    "name": "Ibuprofen",
                    "ageBands": [
                        {
                            "minYears": 4_294_967_298,
                            "maxYears": 4_294_967_307,
                        }
                    ],
                }
            ]
        },
        issues,
    )

    assert entries[0].age_bands == []
    assert "source_data_partially_invalid" in issues


def test_default_interaction_severity_floor_matches_java(monkeypatch):
    monkeypatch.delenv("DRUG_SAFETY_MIN_INTERACTION_SEVERITY", raising=False)
    entries = ds._entries_from_document(
        {
            "entries": [
                {
                    "id": "test-drug",
                    "name": "Test Drug",
                    "aliases": ["test drug"],
                    "interactions": [
                        {"token": "unknown partner", "severity": "Unknown"},
                        {"token": "minor partner", "severity": "Minor"},
                        {"token": "curated partner"},
                    ],
                }
            ]
        }
    )
    dataset = ds.DrugReferenceDataset(
        entries,
        package_id="reviewed-package",
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        cross_reactivity_review_state=ds.REVIEW_CLINICALLY_APPROVED,
    )
    context = ds.PatientClinicalContext(
        age_years=40,
        active_drug_names={"unknown partner", "minor partner", "curated partner"},
    )

    result = ds.check_answer_safety("Test Drug may be used.", None, context, dataset)
    details = [warning.detail for warning in result.warnings]

    assert not any("unknown partner" in detail for detail in details)
    assert any("minor partner" in detail for detail in details)
    assert any("curated partner" in detail for detail in details)
    rendered = ds._render_entry(entries[0], age=None, rules_usable=True)
    assert "unknown partner" not in rendered
    assert "minor partner" in rendered
    assert "curated partner" in rendered


def test_malformed_interaction_severity_is_rejected_but_absent_is_unrated():
    issues = []
    entries = ds._entries_from_document(
        {
            "entries": [
                {
                    "id": "test-drug",
                    "name": "Test Drug",
                    "interactions": [
                        {"token": "misspelled", "severity": "Majro"},
                        {"token": "numeric", "severity": 3},
                        {"token": "blank", "severity": ""},
                        {"token": "unrated"},
                        {"token": "rated", "severity": "Major"},
                    ],
                }
            ]
        },
        issues,
    )

    assert [interaction.token for interaction in entries[0].interactions] == [
        "unrated",
        "rated",
    ]
    assert entries[0].interactions[0].severity is None
    assert entries[0].interactions[1].severity == "Major"
    assert "source_data_partially_invalid" in issues
    dataset = ds.DrugReferenceDataset(
        entries,
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        source_issues=issues,
    )
    assert not dataset.primary_rules_usable()


def test_present_invalid_severity_never_bypasses_the_floor():
    entry = ds.DrugReferenceEntry(
        id="test-drug",
        name="Test Drug",
        aliases=["test drug"],
        interactions=[
            ds.Interaction(token="misspelled", severity="Majro"),
            ds.Interaction(token="unrated"),
        ],
    )
    context = ds.PatientClinicalContext(
        age_years=40,
        active_drug_names={"misspelled", "unrated"},
    )
    dataset = ds.DrugReferenceDataset(
        [entry],
        package_id="reviewed-package",
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        cross_reactivity_review_state=ds.REVIEW_CLINICALLY_APPROVED,
    )

    result = ds.check_answer_safety("Test Drug may be used.", None, context, dataset)
    details = [warning.detail for warning in result.warnings]

    assert not any("misspelled" in detail for detail in details)
    assert any("unrated" in detail for detail in details)
    rendered = ds._render_entry(entry, age=None, rules_usable=True)
    assert "misspelled" not in rendered
    assert "unrated" in rendered


def test_approved_json_without_package_identity_cannot_emit_warnings(tmp_path):
    primary = tmp_path / "primary.json"
    primary.write_text(
        json.dumps(
            {
                "reviewState": "clinically_approved",
                "entries": [
                    {
                        "id": "ibuprofen",
                        "name": "Ibuprofen",
                        "aliases": ["ibuprofen"],
                        "atcCodes": ["M01AE01"],
                        "ageBands": [
                            {"minYears": 2, "maxYears": 11, "maxDailyDoseMg": 1200}
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    relationships = tmp_path / "relationships.json"
    relationships.write_text(
        json.dumps(
            {
                "packageId": "reviewed-relationships",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
                "groups": [],
            }
        ),
        encoding="utf-8",
    )
    dataset = ds.load_dataset(
        str(primary), source_format="json", cross_reactivity_path=str(relationships)
    )

    result = ds.check_answer_safety(
        "Ibuprofen 600 mg every 6 hours can be given for pain.",
        None,
        ds.PatientClinicalContext(age_years=5),
        dataset,
    )

    assert result.status == "limited"
    assert result.warnings == []
    assert "source_package_identity_incomplete" in result.issues


def test_approved_relationships_without_identity_cannot_emit_relationship_warnings(tmp_path):
    primary = tmp_path / "primary.json"
    primary.write_text(
        json.dumps(
            {
                "packageId": "reviewed-medication-rules",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
                "entries": [
                    {
                        "id": "ibuprofen",
                        "name": "Ibuprofen",
                        "aliases": ["ibuprofen"],
                        "atcCodes": ["M01AE01"],
                        "ageBands": [
                            {"minYears": 2, "maxYears": 11, "maxDailyDoseMg": 1200}
                        ],
                    },
                    {
                        "id": "naproxen",
                        "name": "Naproxen",
                        "aliases": ["naproxen"],
                        "atcCodes": ["M01AE02"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    relationships = tmp_path / "relationships.json"
    relationships.write_text(
        json.dumps(
            {
                "reviewState": "clinically_approved",
                "groups": [
                    {"name": "NSAID relationship", "atcPrefixes": ["M01AE"]}
                ],
            }
        ),
        encoding="utf-8",
    )
    dataset = ds.load_dataset(
        str(primary), source_format="json", cross_reactivity_path=str(relationships)
    )
    context = ds.PatientClinicalContext(age_years=5, allergy_tokens={"naproxen"})

    result = ds.check_answer_safety(
        "Ibuprofen 600 mg every 6 hours can be given for pain.", None, context, dataset
    )

    assert result.status == "limited"
    assert any(warning.type == ds.TYPE_OVERDOSE for warning in result.warnings)
    assert not any(warning.type == ds.TYPE_CONTRAINDICATION for warning in result.warnings)
    assert "cross_reactivity_package_identity_incomplete" in result.issues


def test_malformed_primary_diagnostic_survives_unavailable_result():
    dataset = ds.DrugReferenceDataset(
        [],
        [],
        package_id="invalid-primary",
        source_format="json",
        source_version="1",
        provenance={"source": "test formulary"},
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        source_issues=["source_data_invalid"],
    )

    result = ds.check_answer_safety(
        "No medication recommendation.",
        None,
        ds.PatientClinicalContext(age_years=30),
        dataset,
    )

    assert result.status == "unavailable"
    assert "source_data_invalid" in result.issues
    assert "source_unavailable" in result.issues


def test_missing_primary_entries_is_invalid_not_an_empty_approved_package(tmp_path):
    primary = tmp_path / "primary.json"
    primary.write_text(
        json.dumps(
            {
                "packageId": "reviewed-medication-rules",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
            }
        ),
        encoding="utf-8",
    )

    dataset = ds.load_dataset(
        str(primary), source_format="json", cross_reactivity_path="none"
    )
    result = ds.check_answer_safety(
        "No medication recommendation.",
        None,
        ds.PatientClinicalContext(age_years=30),
        dataset,
    )

    assert "source_data_invalid" in dataset.source_issues
    assert not dataset.primary_rules_usable()
    assert result.status == "unavailable"
    assert "source_data_invalid" in result.issues
    assert "source_unavailable" in result.issues


def test_missing_relationship_groups_is_invalid_not_an_empty_approved_package(tmp_path):
    primary = tmp_path / "primary.json"
    primary.write_text(
        json.dumps(
            {
                "packageId": "reviewed-medication-rules",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
                "entries": [
                    {
                        "id": "ibuprofen",
                        "name": "Ibuprofen",
                        "aliases": ["ibuprofen"],
                        "atcCodes": ["M01AE01"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    relationships = tmp_path / "relationships.json"
    relationships.write_text(
        json.dumps(
            {
                "packageId": "reviewed-relationships",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
            }
        ),
        encoding="utf-8",
    )

    dataset = ds.load_dataset(
        str(primary), source_format="json", cross_reactivity_path=str(relationships)
    )
    result = ds.check_answer_safety(
        "No medication recommendation.",
        None,
        ds.PatientClinicalContext(age_years=30),
        dataset,
    )

    assert "cross_reactivity_data_invalid" in dataset.cross_reactivity_issues
    assert not dataset.relationship_rules_usable()
    assert result.status == "limited"
    assert "cross_reactivity_data_invalid" in result.issues
