"""Unit tests for Slice 3: WAHS+ 4-Layer Knowledge Base & Pre-LLM Clinical Normalizers."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict

import pytest

from scripts.build_wahs_eval_fixtures import (
    inject_lamar_multisource_perturbations,
    inject_maurice_multisource_perturbations,
)
from server import kb
from server.context_sources import (
    ContextBudget,
    ContextRequest,
    SourceRegistry,
    StaticKnowledgeSource,
    select_context,
)
from server.fhir import (
    FhirBundleContextSource,
    ResilientTokenCounter,
    annotate_immunization_anomalies,
    audit_cohort_code_coverage,
    classify_sensitivity,
    classify_vaccine_group,
    condense_bundle_to_budget,
    extract_bundle,
    extract_chart_kb_codes,
    get_acip_rules,
    get_clinical_exclusivity_and_privacy,
    get_rxnorm_lookup,
    resolve_condition_code,
    resolve_immunization_code,
    resolve_lab_code,
    resolve_medication_code,
)
from server.fhir.kb_lookup import reset_kb_lookup_cache
from tests.test_fhir_condenser import _make_synthetic_wahs_bundle


def test_wahs_kb_four_layers_load_and_resolve_codes() -> None:
    """All 4 WAHS+ KB layers load with provenance and resolve SNOMED, RxNorm, CVX, and LOINC codes."""
    acip = get_acip_rules()
    rxnorm = get_rxnorm_lookup()
    clin = get_clinical_exclusivity_and_privacy()

    assert acip.get("schema_version") == "wahs_kb_acip_rules.v1"
    assert rxnorm.get("schema_version") == "wahs_kb_rxnorm_lookup.v1"
    assert clin.get("schema_version") == "wahs_kb_clinical_exclusivity_and_privacy.v1"

    # Layer 1: ACIP CVX lookup (including 5-month minimum interval on HPV 2-dose rule)
    hpv = resolve_immunization_code("62", "HPV, quadrivalent")
    assert hpv["resolved_by"] == "cvx_code"
    assert hpv["vaccine_group"] == "HPV"
    assert hpv["two_dose_max_initiation_age_years"] == 14
    assert hpv["min_two_dose_interval_months"] == 5
    assert "5 months" in hpv["series_rule"]

    zoster_live = resolve_immunization_code("121", "zoster vaccine, live")
    assert zoster_live["resolved_by"] == "cvx_code"
    assert zoster_live["vaccine_group"] == "Zoster-Live"
    assert zoster_live["max_recommended_doses"] == 1

    # Layer 2: RxNorm brand↔generic & therapeutic class lookup + unknown-code display/alias resolution
    plavix = resolve_medication_code("213169", "Plavix 75 MG Oral Tablet [clopidogrel]")
    clop = resolve_medication_code("309362", "Clopidogrel 75 MG Oral Tablet")
    assert plavix["ingredient"] == "clopidogrel"
    assert clop["ingredient"] == "clopidogrel"
    assert plavix["therapeutic_class"] == "p2y12_antiplatelet"

    lipitor = resolve_medication_code("617311", "Lipitor 40 MG Oral Tablet [atorvastatin]")
    simva = resolve_medication_code("312961", "Simvastatin 20 MG Oral Tablet")
    assert lipitor["therapeutic_class"] == "statin_lipid_lowering"
    assert simva["therapeutic_class"] == "statin_lipid_lowering"

    # Unknown-code display/brand resolution (hub-56-6)
    humulin = resolve_medication_code("", "Humulin 70/30 Injectable Suspension")
    assert humulin["resolved_by"] == "ingredient_match"
    assert humulin["ingredient"] == "insulin isophane/regular"
    pen_v = resolve_medication_code("", "Penicillin V Potassium 500 MG Oral Tablet")
    assert pen_v["resolved_by"] == "ingredient_match"
    assert pen_v["ingredient"] in {"penicillin v", "penicillin v potassium"}
    for _code, entry in (rxnorm.get("rxnorm_codes") or {}).items():
        by_disp = resolve_medication_code("", str(entry["display"]))
        assert by_disp["resolved_by"] == "ingredient_match"
        assert by_disp["ingredient"] == entry["ingredient"]

    # Layer 3: Clinical stage exclusivity, SDOH privacy, and LOINC consumer reference
    ckd3 = resolve_condition_code("433144002", "Chronic kidney disease stage 3 (disorder)")
    assert ckd3["exclusivity_group"] == "ckd_stage"
    assert ckd3["stage_rank"] == 3
    assert "30–59" in ckd3["plain_description"]

    # Nonspecific condition displays must NOT substring-match into staged or sensitive entries (hub-56-0)
    unspec_ckd = resolve_condition_code("", "Chronic kidney disease")
    assert unspec_ckd["resolved_by"] == "heuristic_fallback"
    assert unspec_ckd["stage_rank"] is None
    assert unspec_ckd["exclusivity_group"] is None
    unspec_dm = resolve_condition_code("", "diabetes mellitus")
    assert unspec_dm["resolved_by"] == "heuristic_fallback"
    assert unspec_dm["exclusivity_group"] is None
    assert resolve_condition_code("", "Chronic kidney disease stage 3")["stage_rank"] == 3

    crim = resolve_condition_code("266948004", "Has a criminal record (finding)")
    assert crim["sensitivity"] == "SDOH/PRIVACY-SENSITIVE"
    assert "redact" in crim.get("sharing_guidance", "").lower()

    egfr = resolve_lab_code("33914-3", "Glomerular filtration rate (eGFR)")
    assert egfr["resolved_by"] == "loinc_code"
    assert "60–89" in egfr["reference_range"]

    # Specimen-different or clearance lab displays must NOT substring-match to serum creatinine/glucose (hub-56-1)
    urine_cr = resolve_lab_code("", "Creatinine [Mass/volume] in Urine")
    assert urine_cr["resolved_by"] == "heuristic_fallback"
    cr_clear = resolve_lab_code("", "Creatinine clearance")
    assert cr_clear["resolved_by"] == "heuristic_fallback"
    urine_glu = resolve_lab_code("", "Urine glucose")
    assert urine_glu["resolved_by"] == "heuristic_fallback"
    serum_cr = resolve_lab_code("", "Serum creatinine")
    assert serum_cr["resolved_by"] == "display_match"
    assert serum_cr["loinc"] == "2160-0"


def test_pack_and_normalizer_rule_agreement_and_hpv_interval() -> None:
    """Structured JSON packs and `server/fhir/normalizers.py` agree entry-by-entry (hub-56-2, hub-56-12)."""
    acip = get_acip_rules()
    clin = get_clinical_exclusivity_and_privacy()

    for cvx_code, entry in (acip.get("cvx_groups") or {}).items():
        expected_group = entry["vaccine_group"]
        assert classify_vaccine_group(cvx_code, "") == expected_group
        assert classify_vaccine_group("", entry["display"]) == expected_group

    for snomed_code, entry in (clin.get("snomed_conditions") or {}).items():
        expected_sens = entry.get("sensitivity")
        assert classify_sensitivity(snomed_code, entry["display"]) == expected_sens

    # Under-15 HPV starter with < 5 months between dose 1 and dose 2 NEEDS a 3rd dose (no redundant-dose alert)
    short_interval_hpv = [
        {"resourceUuid": "hpv-1", "code": "62", "display": "HPV, quadrivalent", "date": "2018-01-10", "group": "HPV"},
        {"resourceUuid": "hpv-2", "code": "62", "display": "HPV, quadrivalent", "date": "2018-03-10", "group": "HPV"},
        {"resourceUuid": "hpv-3", "code": "62", "display": "HPV, quadrivalent", "date": "2018-09-15", "group": "HPV"},
    ]
    assert "hpv-3" not in annotate_immunization_anomalies(short_interval_hpv, "2006-05-01")

    # Under-15 HPV starter with >= 5 months between dose 1 and dose 2 + 3rd dose IS flagged
    valid_two_dose_then_extra = [
        {"resourceUuid": "hpv-1", "code": "62", "display": "HPV, quadrivalent", "date": "2018-01-10", "group": "HPV"},
        {"resourceUuid": "hpv-2", "code": "62", "display": "HPV, quadrivalent", "date": "2018-07-15", "group": "HPV"},
        {"resourceUuid": "hpv-3", "code": "62", "display": "HPV, quadrivalent", "date": "2019-01-20", "group": "HPV"},
    ]
    anoms = annotate_immunization_anomalies(valid_two_dose_then_extra, "2006-05-01")
    assert "hpv-3" in anoms
    assert "5 months" in anoms["hpv-3"]


def test_wahs_corpus_hybrid_code_and_bm25_search_without_stream1_regression(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`kb.search(..., corpus='wahs')` requires code-based ranking for code-priority & code-only queries (hub-56-10..15)."""
    # Stream 1 default corpus remains untouched
    s1_hits = kb.search("metformin first-line diabetes")
    assert s1_hits
    assert s1_hits[0]["id"] == "metformin-first-line-t2dm"

    # Code-only retrieval and priority ordering (fails if codes are ignored or converted to unordered set)
    no_code_hits = kb.search("", k=2, corpus="wahs", codes=[])
    assert no_code_hits == []

    code_prio_a = kb.search("", k=2, corpus="wahs", codes=["213169", "433144002"])
    assert [h["id"] for h in code_prio_a] == [
        "wahs-medrec-clopidogrel-plavix",
        "wahs-ckd-staging-exclusivity",
    ]
    code_prio_b = kb.search("", k=2, corpus="wahs", codes=["433144002", "213169"])
    assert [h["id"] for h in code_prio_b] == [
        "wahs-ckd-staging-exclusivity",
        "wahs-medrec-clopidogrel-plavix",
    ]

    # Competing text query where text matches both HPV and Zoster snippets, and supplied code selects the winner
    with_hpv_code = kb.search("vaccine schedule dose", k=1, corpus="wahs", codes=["62"])
    with_zoster_code = kb.search("vaccine schedule dose", k=1, corpus="wahs", codes=["121"])
    assert with_hpv_code and with_hpv_code[0]["id"] == "wahs-acip-hpv-age15-rule"
    assert with_zoster_code and with_zoster_code[0]["id"] == "wahs-acip-zoster-live-vs-recombinant"
    assert "5 months" in with_hpv_code[0]["text"]

    # Validation of corpus names and k limits (hub-56-13)
    assert kb.search("diabetes", k=0, corpus="wahs") == []
    for bad_k in (-1, "2", 1.5, True, None):
        with pytest.raises(ValueError):
            kb.search("diabetes", k=bad_k, corpus="wahs")  # type: ignore[arg-type]
    for bad_corpus in ("unknown", "stream99", None):
        with pytest.raises(ValueError):
            kb.search("diabetes", k=1, corpus=bad_corpus)  # type: ignore[arg-type]

    # Missing-file recovery without permanent empty cache (hub-56-14)
    missing_corpus = tmp_path / "temp_wahs.jsonl"
    monkeypatch.setattr(kb, "_WAHS_CORPUS_PATH", str(missing_corpus))
    kb.reset_cache()
    assert kb.search("clopidogrel", k=1, corpus="wahs") == []
    missing_corpus.write_text(
        '{"id": "recovered-1", "title": "Recovered Snippet", "text": "Clopidogrel recovery test", '
        '"source": "Test", "version": "1", "url": "https://example.org", "license": "CC0", "codes": ["309362"]}\n',
        encoding="utf-8",
    )
    recovered = kb.search("clopidogrel", k=1, corpus="wahs")
    assert recovered and recovered[0]["id"] == "recovered-1"
    monkeypatch.undo()
    kb.reset_cache()
    reset_kb_lookup_cache()


def test_deterministic_pre_tagging_and_cohort_code_coverage() -> None:
    """Extracted items and condensed records carry `kb_resolution` metadata and code-resolved conflict tags (hub-56-7..9)."""
    base_bundle: Dict[str, Any] = _make_synthetic_wahs_bundle()
    # Add two code-only Clopidogrel/Plavix orders (with empty display) to verify code-resolved duplicate detection (hub-56-7)
    base_bundle["entry"].extend(
        [
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-clop-code-only",
                    "status": "active",
                    "authoredOn": "2023-11-28T09:00:00Z",
                    "medicationCodeableConcept": {"coding": [{"code": "309362"}]},
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-plavix-code-only",
                    "status": "active",
                    "authoredOn": "2023-11-29T09:00:00Z",
                    "medicationCodeableConcept": {"coding": [{"code": "213169"}]},
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-simva-base",
                    "status": "active",
                    "authoredOn": "2023-11-20T09:00:00Z",
                    "medicationCodeableConcept": {
                        "coding": [{"code": "312961", "display": "Simvastatin 20 MG Oral Tablet"}]
                    },
                }
            },
        ]
    )
    perturbed_bundle, _ = inject_maurice_multisource_perturbations(base_bundle)
    perturbed_bundle, _ = inject_lamar_multisource_perturbations(perturbed_bundle)
    extracted = extract_bundle(perturbed_bundle)

    cov = audit_cohort_code_coverage(extracted)
    assert cov["resolution_rate"] == 1.0, f"Unresolved items: {cov['unresolved']}"
    assert cov["exact_code_resolved"] + cov["display_or_alias_resolved"] == cov["total_checked"]
    assert cov["exact_code_resolved"] >= 50
    assert cov["display_or_alias_resolved"] >= 1

    # Standalone unknown lab_observation must be counted in audit_cohort_code_coverage (hub-56-8)
    unknown_lab_bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-unknown-lab",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"code": "99999-9", "display": "Experimental Unmapped Assay"}]},
                    "valueQuantity": {"value": 42.0, "unit": "U/L"},
                }
            }
        ],
    }
    unknown_cov = audit_cohort_code_coverage(extract_bundle(unknown_lab_bundle))
    assert unknown_cov["total_checked"] == 1
    assert unknown_cov["total_resolved"] == 0
    assert unknown_cov["resolution_rate"] == 0.0
    assert len(unknown_cov["unresolved"]) == 1

    # Assert kb_resolution metadata is attached on extracted conditions, medications, immunizations, panels, and vitals (hub-56-9)
    for sec, expected_resolver in (
        ("condition_active", "snomed_code"),
        ("med_active", "rxnorm_code"),
        ("immunization", "cvx_code"),
        ("lab_panel", "loinc_code"),
        ("vital_sign", "loinc_code"),
    ):
        sec_items = [it for it in extracted.items if it.section == sec]
        assert sec_items, f"Expected items in section {sec}"
        for it in sec_items:
            kb_res = it.metadata.get("kb_resolution")
            assert isinstance(kb_res, dict) and kb_res.get("resolved_by") == expected_resolver

    # Verify kb_resolution survives into condensed records
    condensed = condense_bundle_to_budget(perturbed_bundle, target_tokens=8192)
    assert any(
        isinstance((r.get("metadata") or {}).get("kb_resolution"), dict)
        for r in condensed.records
    )

    # Verify code-only Clopidogrel/Plavix orders received duplicate_therapy conflict notes
    code_only_meds = {
        it.resource_uuid: it
        for it in extracted.items
        if it.resource_uuid in {"med-clop-code-only", "med-plavix-code-only"}
    }
    assert len(code_only_meds) == 2
    for med_it in code_only_meds.values():
        assert med_it.conflict_note and "duplicate_therapy" in med_it.conflict_note
        assert "ingredient_group: clopidogrel (p2y12_antiplatelet); duplicate_therapy:" in med_it.text


def test_static_knowledge_source_integrates_with_fhir_bundle_context_source() -> None:
    """`StaticKnowledgeSource` shares `fhir_source`, auto-selects `wahs`, and redacts SDOH codes in `external_share` (hub-56-3..5, 10)."""
    base_bundle = _make_synthetic_wahs_bundle()
    perturbed_bundle, _ = inject_maurice_multisource_perturbations(base_bundle)

    # 1. Auto-selected FHIR source (request.source=None) + shared bundle provider (no bundles_by_patient on StaticKnowledgeSource)
    fhir_source = FhirBundleContextSource(
        bundles_by_patient={"pat-1": perturbed_bundle},
        target_tokens=4096,
    )
    kb_source = StaticKnowledgeSource()
    registry = SourceRegistry([fhir_source, kb_source])

    # Use a generic question whose text does NOT mention Plavix/CKD so only chart-extracted codes can retrieve those KB rows
    request = ContextRequest(
        messages=[
            {
                "role": "user",
                "content": "Please review my chart for any reconciliation or staging issues.",
            }
        ],
        patient="pat-1",
        source=None,
        supplemental_sources=("knowledge-base",),
        question="Please review my chart for any reconciliation or staging issues.",
    )
    ledger = asyncio.run(registry.build_ledger(request))
    kb_records = [r for r in ledger.records if r.resource_type == "KnowledgeReference"]
    assert len(kb_records) >= 2
    kb_uuids = {r.resource_uuid for r in kb_records}
    assert "wahs-ckd-staging-exclusivity" in kb_uuids or "wahs-medrec-clopidogrel-plavix" in kb_uuids

    counter = ResilientTokenCounter(router_counter=None)
    view = asyncio.run(
        select_context(
            ledger,
            question=request.question,
            model="gemma-4-12b",
            budget=ContextBudget(context_window=8192, reserved_output_tokens=2048),
            counter=counter,
        )
    )
    rendered = view.render()
    assert "KnowledgeReference" in str(view.mappings())
    assert "KDIGO" in rendered or "Plavix" in rendered

    # 2. External-share posture must NOT leak criminal-record / IPV SDOH codes or KB guidance (hub-56-5)
    extracted = extract_bundle(perturbed_bundle)
    self_codes = extract_chart_kb_codes(extracted, sharing_posture="patient_self")
    ext_codes = extract_chart_kb_codes(extracted, sharing_posture="external_share")
    assert "266948004" in self_codes
    assert "266948004" not in ext_codes

    ext_fhir = FhirBundleContextSource(
        bundles_by_patient={"pat-1": perturbed_bundle},
        target_tokens=4096,
        sharing_posture="external_share",
    )
    ext_registry = SourceRegistry([ext_fhir, StaticKnowledgeSource()])
    ext_ledger = asyncio.run(ext_registry.build_ledger(request))
    ext_text = ext_ledger.render().lower()
    for forbidden in ("criminal record", "intimate partner", "wahs-sdoh-privacy-sensitive-sharing"):
        assert forbidden not in ext_text
