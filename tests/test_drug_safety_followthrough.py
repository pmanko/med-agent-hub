"""Parity contracts ported from ChartSearchAI upstream commit 5223f92.

These tests keep the durable drug-safety behavior in med-agent-hub while Java remains a thin
relay. They intentionally exercise Querystore's actual ``obs`` record shape rather than an
OpenMRS service-layer object.
"""

import json

from server import drug_safety as ds


def _atc_dataset(*, with_groups=True):
    entries = [
        ds.DrugReferenceEntry(
            id="M01AE01", name="Ibuprofen", aliases=["ibuprofen"], atc_codes=["M01AE01"],
            age_bands=[ds.AgeBand(12, 120, mg_per_kg_max=10, max_daily_dose_mg=2400)],
        ),
        ds.DrugReferenceEntry(
            id="M01AE02", name="Naproxen", aliases=["naproxen"], atc_codes=["M01AE02"],
        ),
        ds.DrugReferenceEntry(
            id="N02BA01", name="Acetylsalicylic acid",
            aliases=["acetylsalicylic acid", "aspirin"], atc_codes=["N02BA01"],
        ),
        ds.DrugReferenceEntry(
            id="J01CA04", name="Amoxicillin", aliases=["amoxicillin"], atc_codes=["J01CA04"],
        ),
    ]
    groups = [
        ds.CrossReactivityGroup(
            name="NSAID", atc_prefixes=["M01AE", "N02BA"],
            note="cross-branch NSAID hypersensitivity",
        )
    ] if with_groups else []
    return ds.DrugReferenceDataset(
        entries,
        cross_reactivity_groups=groups,
        package_id="approved-test-atc-rules",
        source_format="test",
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        cross_reactivity_review_state=ds.REVIEW_CLINICALLY_APPROVED,
    )


def _ctx(*, weight=None, atc=None, allergies=None):
    return ds.PatientClinicalContext(
        age_years=40,
        weight_kg=weight,
        active_drug_atc_codes=set(atc or []),
        allergy_tokens=set(allergies or []),
    )


def _has(warnings, warning_type, drug):
    return any(w.type == warning_type and drug.lower() in w.drug.lower() for w in warnings)


def test_bundled_cross_reactivity_data_loads_nsaid_seed_group():
    dataset = ds.load_dataset()
    groups = dataset.cross_reactivity_groups
    nsaid = next(group for group in groups if group.name == "NSAID")
    assert nsaid.normalized_prefixes() == {"M01AE", "N02BA"}
    assert dataset.cross_reactivity_review_state == ds.REVIEW_PROPOSED


def test_approved_drug_package_cannot_activate_proposed_cross_reactivity_rules():
    dataset = ds.DrugReferenceDataset(
        [
            ds.DrugReferenceEntry(
                id="M01AE01", name="Ibuprofen", aliases=["ibuprofen"], atc_codes=["M01AE01"]
            ),
            ds.DrugReferenceEntry(
                id="N02BA01", name="Aspirin", aliases=["aspirin"], atc_codes=["N02BA01"]
            ),
        ],
        cross_reactivity_groups=[
            ds.CrossReactivityGroup(name="NSAID", atc_prefixes=["M01AE", "N02BA"])
        ],
        package_id="approved-drug-package",
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        cross_reactivity_review_state=ds.REVIEW_PROPOSED,
    )

    result = ds.check_answer_safety(
        "Ibuprofen may be used.",
        "Can this patient use ibuprofen?",
        _ctx(allergies={"aspirin"}),
        dataset,
    )

    assert result.status == ds.STATUS_LIMITED
    assert result.warnings == []
    assert "cross_reactivity_not_clinically_approved" in result.issues


def test_approved_drug_package_cannot_activate_proposed_same_atc_class_rules():
    dataset = ds.DrugReferenceDataset(
        [
            ds.DrugReferenceEntry(
                id="M01AE01", name="Ibuprofen", aliases=["ibuprofen"], atc_codes=["M01AE01"]
            ),
            ds.DrugReferenceEntry(
                id="M01AE02", name="Naproxen", aliases=["naproxen"], atc_codes=["M01AE02"]
            ),
        ],
        package_id="approved-drug-package",
        review_state=ds.REVIEW_CLINICALLY_APPROVED,
        cross_reactivity_review_state=ds.REVIEW_PROPOSED,
    )

    result = ds.check_answer_safety(
        "Naproxen may be used.",
        "Can this patient use naproxen?",
        _ctx(allergies={"ibuprofen"}, atc={"M01AE01"}),
        dataset,
    )

    assert result.status == ds.STATUS_LIMITED
    assert result.warnings == []
    assert "cross_reactivity_not_clinically_approved" in result.issues


def test_weight_aware_per_dose_limit_fires_below_daily_ceiling():
    warnings = ds.validate_answer(
        "Ibuprofen 600 mg every 8 hours can be given.",
        "What ibuprofen dose can she get?",
        _ctx(weight=50),
        _atc_dataset(),
    )
    matching = [w for w in warnings if w.type == ds.TYPE_OVERDOSE and w.drug == "Ibuprofen"]
    assert len(matching) == 1
    assert "10 mg/kg" in matching[0].detail
    assert "50 kg" in matching[0].detail


def test_weight_aware_limit_does_not_fire_when_per_dose_is_within_limit():
    warnings = ds.validate_answer(
        "Ibuprofen 400 mg every 8 hours can be given.", None, _ctx(weight=50), _atc_dataset(),
    )
    assert not _has(warnings, ds.TYPE_OVERDOSE, "ibuprofen")


def test_daily_ceiling_and_weight_ceiling_emit_one_overdose_warning():
    warnings = ds.validate_answer(
        "Ibuprofen 800 mg every 6 hours can be given.", None, _ctx(weight=50), _atc_dataset(),
    )
    matching = [w for w in warnings if w.type == ds.TYPE_OVERDOSE and w.drug == "Ibuprofen"]
    assert len(matching) == 1
    assert "mg/day" in matching[0].detail


def test_latest_fresh_weight_is_read_from_querystore_obs_records():
    records = [
        {"resourceType": "obs", "date": "2026-06-01", "metadata": {
            "concept_uuid": ds.DEFAULT_WEIGHT_CONCEPT_UUID, "value_numeric": 100.0, "units": "kg"}},
        {"resourceType": "obs", "date": "2026-06-19", "metadata": {
            "concept_uuid": ds.DEFAULT_WEIGHT_CONCEPT_UUID, "value_numeric": 50.0, "units": "kg"}},
        {"resourceType": "obs", "date": "2026-06-20", "metadata": {
            "concept_uuid": "not-weight", "value_numeric": 1.0, "units": "kg"}},
    ]
    context = ds.build_patient_context(records, "2026-06-20", _atc_dataset())
    assert context.weight_kg == 50.0


def test_weight_observation_without_explicit_kilogram_units_is_ignored():
    records = [
        {"resourceType": "obs", "date": "2026-06-19", "metadata": {
            "concept_uuid": ds.DEFAULT_WEIGHT_CONCEPT_UUID, "value_numeric": 110.0, "units": "lb"}},
        {"resourceType": "obs", "date": "2026-06-20", "metadata": {
            "concept_uuid": ds.DEFAULT_WEIGHT_CONCEPT_UUID, "value_numeric": 50.0}},
    ]
    context = ds.build_patient_context(records, "2026-06-20", _atc_dataset())
    assert context.weight_kg is None


def test_stale_weight_and_none_sentinel_do_not_drive_dose_check():
    record = {"resourceType": "obs", "date": "2025-01-01", "metadata": {
        "concept_uuid": ds.DEFAULT_WEIGHT_CONCEPT_UUID, "value_numeric": 50.0, "units": "kg"}}
    assert ds.build_patient_context([record], "2026-06-20", _atc_dataset()).weight_kg is None
    assert ds.build_patient_context(
        [{**record, "date": "2026-06-20"}], "2026-06-20", _atc_dataset(),
        weight_concept_uuid=" NoNe ",
    ).weight_kg is None


def test_cross_branch_allergy_and_order_warn_via_curated_group():
    dataset = _atc_dataset()
    allergy_warnings = ds.validate_answer(
        "Acetylsalicylic acid is a reasonable option.", None,
        _ctx(allergies=["ibuprofen"]), dataset,
    )
    assert _has(allergy_warnings, ds.TYPE_CONTRAINDICATION, "acetylsalicylic")
    assert "NSAID" in allergy_warnings[0].detail

    order_warnings = ds.validate_answer(
        "Ibuprofen could help with the pain.", None, _ctx(atc=["N02BA01"]), dataset,
    )
    assert _has(order_warnings, ds.TYPE_INTERACTION, "ibuprofen")
    assert any("NSAID" in warning.detail for warning in order_warnings)


def test_same_subgroup_wins_over_group_and_same_drug_is_not_duplicate():
    dataset = _atc_dataset()
    subgroup = ds.validate_answer(
        "Naproxen could be considered.", None, _ctx(allergies=["ibuprofen"]), dataset,
    )
    matching = [w for w in subgroup if w.type == ds.TYPE_CONTRAINDICATION and w.drug == "Naproxen"]
    assert len(matching) == 1
    assert "M01AE" in matching[0].detail

    same_drug = ds.validate_answer(
        "Ibuprofen 200 mg is already charted.", None, _ctx(atc=["M01AE01"]), dataset,
    )
    assert not _has(same_drug, ds.TYPE_INTERACTION, "ibuprofen")


def test_cross_branch_behavior_requires_groups_data():
    warnings = ds.validate_answer(
        "Acetylsalicylic acid is a reasonable option.", None,
        _ctx(allergies=["ibuprofen"]), _atc_dataset(with_groups=False),
    )
    assert not _has(warnings, ds.TYPE_CONTRAINDICATION, "acetylsalicylic")


def test_group_related_active_order_is_injected_for_question_drug():
    text, _ = ds.inject_drug_references(
        "chart\n", [], "is acetylsalicylic acid safe?", 40, _atc_dataset(),
        active_order_atc_codes={"M01AE01"},
    )
    assert "Drug reference — Acetylsalicylic acid" in text
    assert "Drug reference — Ibuprofen" in text


def test_unusable_relationship_package_cannot_select_an_active_order_reference():
    dataset = _atc_dataset()
    dataset.cross_reactivity_review_state = ds.REVIEW_PROPOSED
    dataset.cross_reactivity_package["review_state"] = ds.REVIEW_PROPOSED

    text, _ = ds.inject_drug_references(
        "chart\n",
        [],
        "is acetylsalicylic acid safe?",
        40,
        dataset,
        active_order_atc_codes={"M01AE01"},
    )

    assert "Drug reference — Acetylsalicylic acid" in text
    assert "Drug reference — Ibuprofen" not in text


def test_prose_warnings_parse_and_render_without_becoming_rules(tmp_path):
    path = tmp_path / "drugs.json"
    path.write_text(json.dumps({
        "packageId": "approved-drugs",
        "version": "1",
        "source": "test formulary",
        "reviewState": "clinically_approved", "entries": [{
        "id": "aspirin", "name": "Aspirin", "aliases": ["aspirin"],
        "warnings": ["Risk of Reye syndrome in children"],
    }]}), encoding="utf-8")
    dataset = ds.load_dataset(str(path), source_format="json", cross_reactivity_path="none")
    text, _ = ds.inject_drug_references("chart\n", [], "is aspirin safe?", 5, dataset)
    assert "Warnings: Risk of Reye syndrome in children." in text
    assert ds.validate_answer("Aspirin may be used.", None, _ctx(), dataset) == []


def test_malformed_sources_and_runtime_failures_are_fail_safe(tmp_path):
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{not-json", encoding="utf-8")
    dataset = ds.load_dataset(str(malformed), source_format="json", cross_reactivity_path="none")
    assert dataset.entries == []

    class BrokenDataset:
        entries = []
        cross_reactivity_groups = []

        def find_by_query(self, _text):
            raise RuntimeError("boom")

    text, mappings = ds.inject_drug_references("chart\n", [], "ibuprofen?", 40, BrokenDataset())
    assert (text, mappings) == ("chart\n", [])
    assert ds.validate_answer("Ibuprofen 600 mg.", None, _ctx(), BrokenDataset()) == []


def test_missing_cross_reactivity_package_makes_an_approved_check_limited(tmp_path):
    path = tmp_path / "approved-drugs.json"
    path.write_text(json.dumps({
        "packageId": "approved-drugs",
        "version": "1",
        "source": "test formulary",
        "reviewState": "clinically_approved",
        "entries": [{"id": "ibuprofen", "name": "Ibuprofen", "aliases": ["ibuprofen"]}],
    }), encoding="utf-8")

    dataset = ds.load_dataset(
        str(path), source_format="json", cross_reactivity_path=str(tmp_path / "missing.json")
    )
    result = ds.check_answer_safety("Ibuprofen may be used.", None, _ctx(), dataset)

    assert result.status == ds.STATUS_LIMITED
    assert "cross_reactivity_source_unavailable" in result.issues
    assert result.package["cross_reactivity"]["id"].endswith("missing.json")
    assert result.package["cross_reactivity"]["source_format"] == "json"
    assert result.package["cross_reactivity"]["review_state"] == ds.REVIEW_PROPOSED


def test_cross_reactivity_package_has_independent_identity_and_provenance(tmp_path):
    drugs = tmp_path / "approved-drugs.json"
    groups = tmp_path / "approved-groups.json"
    drugs.write_text(json.dumps({
        "packageId": "approved-drugs",
        "version": "1",
        "source": "test formulary",
        "reviewState": "clinically_approved",
        "entries": [{"id": "ibuprofen", "name": "Ibuprofen", "aliases": ["ibuprofen"]}],
    }), encoding="utf-8")
    groups.write_text(json.dumps({
        "packageId": "approved-relationships",
        "version": "2026.08",
        "source": "review board",
        "reviewState": "clinically_approved",
        "groups": [{"name": "NSAID", "atcPrefixes": ["M01AE", "N02BA"]}],
    }), encoding="utf-8")

    dataset = ds.load_dataset(
        str(drugs), source_format="json", cross_reactivity_path=str(groups)
    )
    relationship_package = dataset.package_metadata()["cross_reactivity"]

    assert relationship_package == {
        "id": "approved-relationships",
        "source_format": "json",
        "version": "2026.08",
        "provenance": {"dataset": "approved-groups.json", "source": "review board"},
        "review_state": ds.REVIEW_CLINICALLY_APPROVED,
        "issues": [],
    }


def test_null_rule_elements_are_removed_without_losing_record_identity(tmp_path):
    path = tmp_path / "partly-malformed.json"
    path.write_text(json.dumps({
        "packageId": "partly-malformed",
        "version": "1",
        "source": "test formulary",
        "reviewState": "clinically_approved", "entries": [
        {"id": None, "name": "missing id", "aliases": ["drop-me"]},
        {"id": "missing-name", "name": " ", "aliases": ["drop-me-too"]},
        {
            "id": "mangled", "name": "Mangled", "aliases": [None, "mangled"],
            "atcCodes": [None, " ", "J01CA04"],
            "warnings": [None, " ", "Real warning survives"],
            "contraindications": [None, {"type": "condition", "token": "test condition"}],
            "interactions": [None, {"note": "note-only interaction"}],
        },
    ]}), encoding="utf-8")
    dataset = ds.load_dataset(str(path), source_format="json", cross_reactivity_path="none")
    assert [entry.id for entry in dataset.entries] == ["mangled"]

    text, mappings = ds.inject_drug_references("chart\n", [], "is mangled safe?", 40, dataset)
    assert len(mappings) == 1
    assert "ATC J01CA04" in text
    assert "Informational research classification only" in text
    assert "Real warning survives" not in text
    assert "test condition" not in text
    assert "note-only interaction" not in text
    assert "null" not in text.lower()

    result = ds.check_answer_safety("Mangled may be used.", None, _ctx(), dataset)
    assert result.status == ds.STATUS_LIMITED
    assert "source_data_partially_invalid" in result.issues


def test_scalar_list_fields_and_broad_atc_prefixes_are_rejected(tmp_path):
    drugs = tmp_path / "scalar-lists.json"
    groups = tmp_path / "scalar-groups.json"
    drugs.write_text(
        json.dumps(
            {
                "packageId": "scalar-lists",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
                "entries": [
                    {
                        "id": "mangled",
                        "name": "Mangled",
                        "aliases": "ibuprofen",
                        "atcCodes": "M01AE01",
                        "warnings": "not a list",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    groups.write_text(
        json.dumps(
            {
                "groups": [
                    {"name": "scalar", "atcPrefixes": "M01AE"},
                    {"name": "too broad", "atcPrefixes": ["M"]},
                ]
            }
        ),
        encoding="utf-8",
    )

    dataset = ds.load_dataset(
        str(drugs), source_format="json", cross_reactivity_path=str(groups)
    )

    assert dataset.entries[0].aliases == []
    assert dataset.entries[0].atc_codes == []
    assert dataset.entries[0].warnings == []
    assert dataset.cross_reactivity_groups == []
    assert dataset.find_by_query("Can I use ibuprofen?") == []

    result = ds.check_answer_safety("Mangled may be used.", None, _ctx(), dataset)
    assert result.status == ds.STATUS_LIMITED
    assert "source_data_partially_invalid" in result.issues
    assert "cross_reactivity_data_partially_invalid" in result.issues


def test_malformed_age_band_preserves_valid_sibling_but_blocks_package_warnings(tmp_path):
    path = tmp_path / "bad-age-band.json"
    path.write_text(
        json.dumps(
            {
                "packageId": "bad-age-band",
                "version": "1",
                "source": "test formulary",
                "reviewState": "clinically_approved",
                "entries": [
                    {
                        "id": "ibuprofen",
                        "name": "Ibuprofen",
                        "aliases": ["ibuprofen"],
                        "ageBands": [
                            {
                                "minYears": "not-a-number",
                                "maxYears": 120,
                                "mgPerKgMax": 10,
                            }
                        ],
                        "interactions": [
                            {
                                "token": "warfarin",
                                "note": "increased bleeding risk",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    dataset = ds.load_dataset(
        str(path), source_format="json", cross_reactivity_path="none"
    )
    context = ds.PatientClinicalContext(
        age_years=40,
        active_drug_names={"warfarin"},
    )

    warnings = ds.validate_answer(
        "Ibuprofen 600 mg may be used.", None, context, dataset
    )

    assert dataset.entries[0].age_bands == []
    assert len(dataset.entries[0].interactions) == 1
    assert warnings == []

    result = ds.check_answer_safety(
        "Ibuprofen 600 mg may be used.", None, context, dataset
    )
    assert result.status == ds.STATUS_LIMITED
    assert "source_data_partially_invalid" in result.issues
    assert result.warnings == []
