"""Parity tests for the WHO-ATC drug-reference source in server/drug_safety.py, translated from
chartsearchai's deleted AtcDrugReferenceSourceTest.java (reconstructed from commit 65e3c08^).

The ATC adapter consumes a WHO ATC classification export (`<atcCode><whitespace><name>`, all levels)
and emits one classification entry per level-5 substance. ATC is a classification, not a rulebook —
entries carry NO dosing/interaction/contraindication rules; safety comes from ATC-CLASS reasoning
(an allergy or active order sharing a drug's ATC level-4 subgroup). See ADR Decision 24.
"""

import os

from server import drug_safety as ds

_ATC_SAMPLE = os.path.join(os.path.dirname(__file__), "fixtures", "atc-sample.tsv")


def _parse_sample():
    return ds._load_atc_entries(_ATC_SAMPLE)


def _by_code(entries, code):
    return next((e for e in entries if e.id == code), None)


def test_emits_one_entry_per_level5_substance_only():
    # 6 level-5 substances in the sample; the group rows (M01A, M01AE, …) are NOT entries.
    entries = _parse_sample()
    assert len(entries) == 6
    assert all(len(e.id) == 7 for e in entries)


def test_parses_lowercase_atc_codes_by_normalising(tmp_path):
    # RxNorm/ATC crosswalk exports are not always upper case; a lowercase code must be parsed
    # (normalised to upper case), not silently dropped, leaving the whole dataset empty.
    f = tmp_path / "lower.tsv"
    f.write_text("m01ae\tPropionic acid derivatives\nm01ae01\tIbuprofen\n", encoding="utf-8")
    ibuprofen = _by_code(ds._load_atc_entries(str(f)), "M01AE01")
    assert ibuprofen is not None, "a lowercase ATC code must be parsed (normalised), not dropped"
    assert "M01AE01" in ibuprofen.atc_codes
    assert ibuprofen.drug_class == "Propionic acid derivatives"


def test_ignores_seven_char_tokens_that_are_not_valid_atc_codes():
    # "ABCDEFG" is 7 chars but not a valid ATC level-5 code — it must NOT become a drug entry.
    assert _by_code(_parse_sample(), "ABCDEFG") is None


def test_substance_carries_name_code_and_alias_for_matching():
    ibuprofen = _by_code(_parse_sample(), "M01AE01")
    assert ibuprofen is not None
    assert ibuprofen.name == "Ibuprofen"
    assert "M01AE01" in ibuprofen.atc_codes
    assert ibuprofen.matches_text("is ibuprofen safe?"), "the name should match as a whole-word alias"


def test_drug_class_is_derived_from_the_nearest_parent_group():
    # M01AE01 -> nearest parent group M01AE = "Propionic acid derivatives".
    assert _by_code(_parse_sample(), "M01AE01").drug_class == "Propionic acid derivatives"


def test_same_class_drugs_share_a_drug_class():
    # ibuprofen (M01AE01) and naproxen (M01AE02) are both M01AE -> one class rule covers both.
    entries = _parse_sample()
    assert _by_code(entries, "M01AE01").drug_class == _by_code(entries, "M01AE02").drug_class


def test_aspirin_is_not_in_the_same_atc_class_as_ibuprofen():
    # Honest boundary: NSAID cross-reactivity spans ATC branches. Aspirin (N02BA01, salicylates) is a
    # different ATC class than ibuprofen (M01AE01, propionic NSAIDs), so ATC membership alone won't link them.
    entries = _parse_sample()
    assert _by_code(entries, "N02BA01").drug_class != _by_code(entries, "M01AE01").drug_class
    assert _by_code(entries, "N02BA01").drug_class == "Salicylic acid and derivatives"


def test_atc_entries_carry_no_dosing_interaction_or_contraindication_rules():
    # ATC is a classification, not a rulebook — these stay empty; only the ATC code is carried.
    ibuprofen = _by_code(_parse_sample(), "M01AE01")
    assert ibuprofen.age_bands == []
    assert ibuprofen.interactions == []
    assert ibuprofen.contraindications == []
    assert ibuprofen.atc_codes


def test_missing_or_unreadable_path_fails_safe_to_empty():
    # Fail-safe like the Java source: a missing dataset degrades to [] so the answer path never breaks.
    assert ds._load_atc_entries("/nonexistent/atc-dataset.tsv") == []


def test_load_dataset_dispatches_to_the_atc_source_by_format():
    # The selection seam: source_format="atc" routes load_dataset through the ATC parser.
    dataset = ds.load_dataset(path=_ATC_SAMPLE, source_format="atc")
    assert dataset.lookup_by_token("ibuprofen") is not None
    assert dataset.lookup_by_token("ibuprofen").drug_class == "Propionic acid derivatives"


def test_atc_sourced_entries_drive_class_level_safety_end_to_end():
    # The whole point of the ATC source: entries carry NO curated contraindication rules, yet a
    # class-level warning still fires because naproxen (M01AE02) and the patient's ibuprofen
    # (M01AE01) allergy share the ATC level-4 subgroup M01AE. Proves the rule-less classification
    # turns into safety warnings via ATC-class reasoning — through the real validate_answer path.
    dataset = ds.load_dataset(path=_ATC_SAMPLE, source_format="atc")
    context = ds.PatientClinicalContext(age_years=40, allergy_tokens={"ibuprofen"})
    warnings = ds.validate_answer("Naproxen could be considered for this patient.", None, context, dataset)
    assert any(w.type == "contraindication" and "naproxen" in w.drug.lower() for w in warnings), \
        f"expected a class-level contraindication on naproxen; got {[w.to_dict() for w in warnings]}"
