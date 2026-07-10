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
    return ds.DrugReferenceDataset(entries, cross_reactivity_groups=groups)


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
    groups = ds.load_dataset().cross_reactivity_groups
    nsaid = next(group for group in groups if group.name == "NSAID")
    assert nsaid.normalized_prefixes() == {"M01AE", "N02BA"}


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


def test_prose_warnings_parse_and_render_without_becoming_rules(tmp_path):
    path = tmp_path / "drugs.json"
    path.write_text(json.dumps({"entries": [{
        "id": "aspirin", "name": "Aspirin", "aliases": ["aspirin"],
        "warnings": [None, "", "Risk of Reye syndrome in children"],
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


def test_null_rule_elements_render_best_effort_without_literal_null(tmp_path):
    path = tmp_path / "partly-malformed.json"
    path.write_text(json.dumps({"entries": [
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
    assert "Real warning survives" in text
    assert "test condition" in text
    assert "note-only interaction" in text
    assert "null" not in text.lower()
