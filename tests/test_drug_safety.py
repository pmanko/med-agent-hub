"""Parity tests for server/drug_safety.py, translated from chartsearchai's Java suite:
DrugSafetyValidatorTest.java, drug-safety-eval.json, and DrugReferenceInjectorTest.java. Each
case is chosen because it pins a specific parsing/matching rule (clause scoping, alias-owns-dose,
ATC subgroup boundaries, age-band gating) that a naive reimplementation would get wrong.
"""

import pytest

from server import drug_safety as ds


# ---------------------------------------------------------------------------
# Fixtures: real bundled dataset + a small ATC-only dataset (mirrors the Java
# atc-sample.tsv fixture: entries that carry ATC codes but NO curated rules).
# ---------------------------------------------------------------------------

@pytest.fixture()
def dataset():
    return ds.load_dataset()


@pytest.fixture()
def atc_dataset():
    entries = [
        ds.DrugReferenceEntry(id="M01AE01", name="Ibuprofen", drug_class="Propionic acid derivatives",
                               aliases=["ibuprofen"], atc_codes=["M01AE01"]),
        ds.DrugReferenceEntry(id="M01AE02", name="Naproxen", drug_class="Propionic acid derivatives",
                               aliases=["naproxen"], atc_codes=["M01AE02"]),
        ds.DrugReferenceEntry(id="N02BA01", name="Acetylsalicylic acid", drug_class="Salicylic acid and derivatives",
                               aliases=["acetylsalicylic acid", "aspirin"], atc_codes=["N02BA01"]),
    ]
    return ds.DrugReferenceDataset(entries)


def ctx(age=None, drugs=None, allergies=None, conditions=None, atc=None):
    return ds.PatientClinicalContext(
        age_years=age,
        active_drug_names=set(drugs or []),
        active_drug_atc_codes=set(atc or []),
        allergy_tokens=set(allergies or []),
        condition_tokens=set(conditions or []),
    )


def has(warnings, type_, drug_contains):
    return any(w.type == type_ and drug_contains.lower() in w.drug.lower() for w in warnings)


def detail_contains(warnings, type_, drug, *needles):
    for w in warnings:
        if w.type != type_ or w.drug.lower() != drug.lower():
            continue
        if all(n.lower() in w.detail.lower() for n in needles):
            return True
    return False


# ---------------------------------------------------------------------------
# Overdose parsing (DrugSafetyValidatorTest)
# ---------------------------------------------------------------------------

def test_overdose_flagged_when_daily_total_exceeds_max(dataset):
    warnings = ds.validate_answer("Ibuprofen 600 mg every 6 hours can be given for pain.",
                                   None, ctx(age=5), dataset)
    assert has(warnings, "overdose", "ibuprofen")


def test_overdose_uses_every_n_hours_frequency(dataset):
    warnings = ds.validate_answer("Give ibuprofen 500 mg every 8 hours.", None, ctx(age=5), dataset)
    assert has(warnings, "overdose", "ibuprofen")


def test_dose_under_max_not_flagged(dataset):
    warnings = ds.validate_answer("Ibuprofen 200 mg three times a day is appropriate.",
                                   None, ctx(age=5), dataset)
    assert not has(warnings, "overdose", "ibuprofen")


def test_interaction_flagged_against_active_order(dataset):
    warnings = ds.validate_answer("Ibuprofen could help with the pain.", None,
                                   ctx(age=40, drugs=["warfarin"]), dataset)
    assert has(warnings, "interaction", "ibuprofen")


def test_contraindication_flagged_against_allergy(dataset):
    warnings = ds.validate_answer("Ibuprofen 200 mg as needed.", None,
                                   ctx(age=40, allergies=["nsaid"]), dataset)
    assert has(warnings, "contraindication", "ibuprofen")


def test_no_false_positive_when_answer_needs_no_reference(dataset):
    warnings = ds.validate_answer("The patient's most recent blood pressure is 120/80 mmHg [1].",
                                   None, ctx(age=40, drugs=["warfarin"], allergies=["nsaid"]), dataset)
    assert warnings == []


@pytest.mark.parametrize("window,expected", [
    ("one tablet every 6 hours", 4),
    ("every 8 hours", 3),
    ("twice daily", 2),
    ("three times a day", 3),
    ("as needed for pain", 0),
])
def test_frequency_parsing_maps_every_n_hours_to_doses_per_day(window, expected):
    assert ds.frequency_per_day(window) == expected


def test_overdose_not_attributed_to_a_drug_named_in_a_neighbouring_clause(dataset):
    warnings = ds.validate_answer(
        "Ibuprofen may help with pain; paracetamol 1000 mg every 6 hours is an alternative.",
        None, ctx(age=5), dataset)
    assert not has(warnings, "overdose", "ibuprofen")


def test_overdose_frequency_does_not_bleed_across_sentences(dataset):
    warnings = ds.validate_answer(
        "Ibuprofen 600 mg was administered. Paracetamol every 6 hours was also charted.",
        None, ctx(age=5), dataset)
    assert not has(warnings, "overdose", "ibuprofen")


def test_stated_reference_ceiling_is_not_read_as_a_prescribed_dose(dataset):
    warnings = ds.validate_answer(
        "For ibuprofen, the maximum 2400 mg per day should not be exceeded.",
        None, ctx(age=5), dataset)
    assert not has(warnings, "overdose", "ibuprofen")


def test_frequency_word_forms_require_word_boundaries():
    assert ds.frequency_per_day("for abdominal discomfort") == 0
    assert ds.frequency_per_day("ibuprofen 200 mg bd") == 2


def test_decimal_dose_is_not_split_by_the_clause_delimiter(dataset):
    warnings = ds.validate_answer("Ibuprofen 333.5 mg every 6 hours.", None, ctx(age=5), dataset)
    assert has(warnings, "overdose", "ibuprofen")


def test_real_single_drug_overdose_still_flagged_after_anchoring(dataset):
    warnings = ds.validate_answer("Ibuprofen 800 mg every 6 hours.", None, ctx(age=5), dataset)
    assert has(warnings, "overdose", "ibuprofen")


# ---------------------------------------------------------------------------
# Class-based (ATC) safety reasoning
# ---------------------------------------------------------------------------

def test_class_contraindication_across_same_atc_subgroup(atc_dataset):
    warnings = ds.validate_answer("Naproxen could be considered for this patient.", None,
                                   ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    assert has(warnings, "contraindication", "naproxen")
    assert detail_contains(warnings, "contraindication", "Naproxen", "ibuprofen", "M01AE")


def test_class_contraindication_for_recorded_allergy_to_the_named_drug(atc_dataset):
    warnings = ds.validate_answer("Ibuprofen 200 mg as needed.", None,
                                   ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    assert has(warnings, "contraindication", "ibuprofen")


def test_contraindication_fires_when_question_names_drug_but_answer_does_not(atc_dataset):
    warnings = ds.validate_answer(
        "The patient has an allergy to NSAID (drug allergen).",
        "Is ibuprofen contraindicated for her?",
        ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    assert has(warnings, "contraindication", "ibuprofen")


def test_contraindication_fires_from_question_even_when_answer_is_empty(atc_dataset):
    warnings = ds.validate_answer("", "Is ibuprofen safe for her?",
                                   ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    assert has(warnings, "contraindication", "ibuprofen")


def test_question_driven_check_respects_atc_branch_boundary_no_false_positive(atc_dataset):
    warnings = ds.validate_answer(
        "The patient has an allergy to NSAID (drug allergen).",
        "Is acetylsalicylic acid a good option for her?",
        ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    assert not has(warnings, "contraindication", "acetylsalicylic")


def test_drug_in_both_question_and_answer_warns_only_once(atc_dataset):
    warnings = ds.validate_answer("Ibuprofen 200 mg as needed.", "Is ibuprofen safe for her?",
                                   ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    contra = [w for w in warnings if w.type == "contraindication" and w.drug.lower() == "ibuprofen"]
    assert len(contra) == 1


def test_class_contraindication_not_raised_across_different_atc_branch(atc_dataset):
    warnings = ds.validate_answer("Acetylsalicylic acid is a reasonable option here.", None,
                                   ctx(age=40, allergies=["ibuprofen"]), atc_dataset)
    assert not has(warnings, "contraindication", "acetylsalicylic")


def test_class_interaction_flagged_for_same_class_active_order(atc_dataset):
    warnings = ds.validate_answer("Ibuprofen could help with the pain.", None,
                                   ctx(age=40, atc=["M01AE02"]), atc_dataset)
    assert has(warnings, "interaction", "ibuprofen")
    assert detail_contains(warnings, "interaction", "Ibuprofen", "naproxen", "M01AE")


def test_class_interaction_not_raised_when_active_order_is_the_same_drug(atc_dataset):
    warnings = ds.validate_answer("Ibuprofen 200 mg is already charted.", None,
                                   ctx(age=40, atc=["M01AE01"]), atc_dataset)
    assert not has(warnings, "interaction", "ibuprofen")


def test_class_interaction_for_same_class_order_not_in_dataset_names_the_bare_code(atc_dataset):
    warnings = ds.validate_answer("Ibuprofen could help with the pain.", None,
                                   ctx(age=40, atc=["M01AE99"]), atc_dataset)
    assert detail_contains(warnings, "interaction", "Ibuprofen", "M01AE99")


def test_class_interaction_not_raised_for_different_class_active_order(atc_dataset):
    warnings = ds.validate_answer("Ibuprofen could help with the pain.", None,
                                   ctx(age=40, atc=["J01CA04"]), atc_dataset)
    assert not has(warnings, "interaction", "ibuprofen")


def test_duplicate_allergy_aliases_produce_a_single_contraindication(dataset):
    warnings = ds.validate_answer("Ibuprofen 200 mg as needed.", None,
                                   ctx(age=40, allergies=["advil", "brufen"]), dataset)
    ibu = [w for w in warnings if w.type == "contraindication" and w.drug.lower() == "ibuprofen"]
    assert len(ibu) == 1


# ---------------------------------------------------------------------------
# drug-safety-eval.json parity cases
# ---------------------------------------------------------------------------

EVAL_CASES = [
    ("dosing-overdose-flag", "What is a safe ibuprofen dose for this 5-year-old?",
     "Ibuprofen 600 mg every 6 hours can be given for pain relief.", 5, [], [], [], ["overdose"]),
    ("dosing-within-limit-no-warning", "What is a safe ibuprofen dose for this 5-year-old?",
     "Ibuprofen 200 mg three times a day is appropriate.", 5, [], [], [], []),
    ("interaction-warning", "Can I add ibuprofen for this patient's pain?",
     "Ibuprofen could be considered for the patient's pain.", 40, ["warfarin"], [], [], ["interaction"]),
    ("contraindication-allergy", "Is ibuprofen safe for this patient?",
     "Ibuprofen 200 mg as needed should help.", 40, [], ["nsaid"], [], ["contraindication"]),
    ("no-false-positive", "What is the patient's latest blood pressure?",
     "The patient's most recent blood pressure is 120/80 mmHg [1].", 40, ["warfarin"], ["nsaid"], [], []),
    ("contraindication-from-question-when-answer-omits-drug", "Is ibuprofen contraindicated for this patient?",
     "The patient has a documented NSAID hypersensitivity on file.", 40, [], ["nsaid"], [], ["contraindication"]),
]


@pytest.mark.parametrize("case_id,question,answer,age,drugs,allergies,conditions,expected_types", EVAL_CASES,
                          ids=[c[0] for c in EVAL_CASES])
def test_drug_safety_eval_parity(dataset, case_id, question, answer, age, drugs, allergies, conditions,
                                  expected_types):
    warnings = ds.validate_answer(answer, question,
                                   ctx(age=age, drugs=drugs, allergies=allergies, conditions=conditions),
                                   dataset)
    assert sorted(w.type for w in warnings) == sorted(expected_types)


# ---------------------------------------------------------------------------
# Injection rendering (DrugReferenceInjectorTest)
# ---------------------------------------------------------------------------

def test_question_driven_injection_appends_citable_record(dataset):
    text, mappings = ds.inject_drug_references(
        "Patient: 5-year-old Male\n\n[1] BP 120/80\n",
        [{"index": 1, "resourceType": "obs", "resourceUuid": "obs-uuid-1", "date": None, "text": "BP 120/80"}],
        "what is the safe dose of ibuprofen?", 5, dataset)
    assert len(mappings) == 2
    injected = mappings[1]
    assert injected["index"] == 2
    assert injected["resourceType"] == "drug_reference"
    assert injected["resourceUuid"] == "ibuprofen"
    assert "[2] Drug reference — Ibuprofen" in text


def test_dosing_rendered_for_matching_age_band(dataset):
    _, mappings = ds.inject_drug_references(
        "chart\n", [], "ibuprofen dose?", 5, dataset)
    injected_text = mappings[0]["text"]
    assert "ages 2-11" in injected_text
    assert "1200 mg/day" in injected_text


def test_dosing_omitted_when_age_unknown(dataset):
    _, mappings = ds.inject_drug_references("chart\n", [], "ibuprofen dose?", None, dataset)
    injected_text = mappings[0]["text"]
    assert "Dosing for ages" not in injected_text
    assert "Contraindicated with:" in injected_text


def test_no_match_returns_chart_unchanged(dataset):
    text, mappings = ds.inject_drug_references("chart\n", [], "how is the patient doing?", 5, dataset)
    assert text == "chart\n"
    assert mappings == []


def test_silent_question_does_not_inject_active_orders(dataset):
    text, mappings = ds.inject_drug_references(
        "chart\n", [], "summarise the plan", 5, dataset, active_order_atc_codes={"M01AE01"})
    assert text == "chart\n"
    assert mappings == []


def test_unrelated_active_order_is_not_injected_for_a_drug_specific_query(dataset):
    text, mappings = ds.inject_drug_references(
        "chart\n", [], "is gentamicin safe to prescribe?", 40, dataset,
        active_order_atc_codes={"M01AE01"})
    assert "Drug reference — Gentamicin" in text
    assert "Drug reference — Ibuprofen" not in text


def test_related_active_order_is_still_injected_for_a_drug_specific_query(atc_dataset):
    text, mappings = ds.inject_drug_references(
        "chart\n", [], "is naproxen safe to prescribe?", 40, atc_dataset,
        active_order_atc_codes={"M01AE01"})
    assert "Drug reference — Naproxen" in text
    assert "Drug reference — Ibuprofen" in text


def test_renders_atc_classification_entry_with_no_rule_sections():
    entry = ds.DrugReferenceEntry(id="M01AE01", name="Ibuprofen", drug_class="Propionic acid derivatives",
                                   aliases=["ibuprofen"], atc_codes=["M01AE01"])
    one_entry_dataset = ds.DrugReferenceDataset([entry])
    _, mappings = ds.inject_drug_references("chart\n", [], "what is the ibuprofen dose?", 5, one_entry_dataset)
    injected_text = mappings[0]["text"]
    assert "Drug reference — Ibuprofen" in injected_text
    assert "Propionic acid derivatives" in injected_text
    assert "ATC M01AE01" in injected_text
    assert "Dosing for ages" not in injected_text
    assert "Contraindicated with:" not in injected_text
    assert "Interactions:" not in injected_text
