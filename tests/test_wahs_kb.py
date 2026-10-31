"""Unit tests for Slice 3: WAHS+ 4-Layer Knowledge Base & Pre-LLM Clinical Normalizers."""

from __future__ import annotations

import asyncio
from typing import Any, Dict

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
    audit_cohort_code_coverage,
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
from tests.test_fhir_condenser import _make_synthetic_wahs_bundle


def test_wahs_kb_four_layers_load_and_resolve_codes() -> None:
    """All 4 WAHS+ KB layers load with provenance and resolve SNOMED, RxNorm, CVX, and LOINC codes."""
    acip = get_acip_rules()
    rxnorm = get_rxnorm_lookup()
    clin = get_clinical_exclusivity_and_privacy()

    assert acip.get("schema_version") == "wahs_kb_acip_rules.v1"
    assert rxnorm.get("schema_version") == "wahs_kb_rxnorm_lookup.v1"
    assert clin.get("schema_version") == "wahs_kb_clinical_exclusivity_and_privacy.v1"

    # Layer 1: ACIP CVX lookup
    hpv = resolve_immunization_code("62", "HPV, quadrivalent")
    assert hpv["resolved_by"] == "cvx_code"
    assert hpv["vaccine_group"] == "HPV"
    assert hpv["two_dose_max_initiation_age_years"] == 14

    zoster_live = resolve_immunization_code("121", "zoster vaccine, live")
    assert zoster_live["resolved_by"] == "cvx_code"
    assert zoster_live["vaccine_group"] == "Zoster-Live"
    assert zoster_live["max_recommended_doses"] == 1

    # Layer 2: RxNorm brand↔generic & therapeutic class lookup
    plavix = resolve_medication_code("213169", "Plavix 75 MG Oral Tablet [clopidogrel]")
    clop = resolve_medication_code("309362", "Clopidogrel 75 MG Oral Tablet")
    assert plavix["ingredient"] == "clopidogrel"
    assert clop["ingredient"] == "clopidogrel"
    assert plavix["therapeutic_class"] == "p2y12_antiplatelet"

    lipitor = resolve_medication_code("617311", "Lipitor 40 MG Oral Tablet [atorvastatin]")
    simva = resolve_medication_code("312961", "Simvastatin 20 MG Oral Tablet")
    assert lipitor["therapeutic_class"] == "statin_lipid_lowering"
    assert simva["therapeutic_class"] == "statin_lipid_lowering"

    # Layer 3: Clinical stage exclusivity, SDOH privacy, and LOINC consumer reference
    ckd3 = resolve_condition_code("433144002", "Chronic kidney disease stage 3 (disorder)")
    assert ckd3["exclusivity_group"] == "ckd_stage"
    assert ckd3["stage_rank"] == 3
    assert "30–59" in ckd3["plain_description"]

    crim = resolve_condition_code("266948004", "Has a criminal record (finding)")
    assert crim["sensitivity"] == "SDOH/PRIVACY-SENSITIVE"
    assert "redact" in crim.get("sharing_guidance", "").lower()

    egfr = resolve_lab_code("33914-3", "Glomerular filtration rate (eGFR)")
    assert egfr["resolved_by"] == "loinc_code"
    assert "60–89" in egfr["reference_range"]


def test_wahs_corpus_hybrid_code_and_bm25_search_without_stream1_regression() -> None:
    """`kb.search(..., corpus='wahs')` retrieves WAHS snippets while default `kb.search` preserves Stream 1."""
    # Stream 1 default corpus remains untouched
    s1_hits = kb.search("metformin first-line diabetes")
    assert s1_hits
    assert s1_hits[0]["id"] == "metformin-first-line-t2dm"

    # WAHS corpus retrieves CKD staging, ACIP HPV, and Plavix/Clopidogrel reconciliation snippets
    ckd_hits = kb.search(
        "chronic kidney disease stage 1 stage 2 stage 3 conflict",
        k=3,
        corpus="wahs",
        codes=["431855005", "433144002"],
    )
    assert ckd_hits
    assert ckd_hits[0]["id"] == "wahs-ckd-staging-exclusivity"

    hpv_hits = kb.search(
        "HPV vaccine 3 doses started at age 11 schedule",
        k=3,
        corpus="wahs",
        codes=["62"],
    )
    assert hpv_hits
    assert hpv_hits[0]["id"] == "wahs-acip-hpv-age15-rule"

    med_hits = kb.search(
        "duplicate blood thinner Clopidogrel Plavix and Simvastatin Lipitor",
        k=3,
        corpus="wahs",
        codes=["309362", "213169", "312961", "617311"],
    )
    hit_ids = {h["id"] for h in med_hits}
    assert "wahs-medrec-clopidogrel-plavix" in hit_ids
    assert "wahs-medrec-statin-class-duplication" in hit_ids


def test_deterministic_pre_tagging_and_cohort_code_coverage() -> None:
    """Extracted items carry `kb_resolution` metadata and pre-annotated conflict/ingredient tags."""
    base_bundle: Dict[str, Any] = _make_synthetic_wahs_bundle()
    # Add active Clopidogrel and Simvastatin so Maurice perturbation triggers brand/generic & statin class conflicts
    base_bundle["entry"].extend(
        [
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-clop-base",
                    "status": "active",
                    "authoredOn": "2023-11-28T09:00:00Z",
                    "medicationCodeableConcept": {
                        "coding": [{"code": "309362", "display": "Clopidogrel 75 MG Oral Tablet"}]
                    },
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

    # Verify pre-annotated tags on conflicting medications, conditions, and immunizations
    med_texts = [it.text for it in extracted.items if it.section == "med_active"]
    assert any(
        "ingredient_group: clopidogrel (p2y12_antiplatelet); duplicate_therapy:" in t
        for t in med_texts
    )
    assert any(
        "ingredient_group: atorvastatin (statin_lipid_lowering); duplicate_therapy:" in t
        for t in med_texts
    )
    assert any(
        "ingredient_group: lisinopril (ace_inhibitor); duplicate_therapy:" in t
        for t in med_texts
    )

    cond_texts = [it.text for it in extracted.items if it.section == "condition_active"]
    assert any("stage_conflict: CKD" in t and "simultaneously marked ACTIVE" in t for t in cond_texts)
    assert any("stage_conflict: Prediabetes marked ACTIVE" in t for t in cond_texts)
    assert any("[sensitivity: SDOH/PRIVACY-SENSITIVE]" in t for t in cond_texts)

    imm_texts = [it.text for it in extracted.items if it.section == "immunization"]
    assert any("acip_schedule_note:" in t for t in imm_texts)


def test_static_knowledge_source_integrates_with_fhir_bundle_context_source() -> None:
    """`StaticKnowledgeSource` automatically uses `wahs` corpus and chart codes when paired with a FHIR bundle."""
    base_bundle = _make_synthetic_wahs_bundle()
    perturbed_bundle, _ = inject_maurice_multisource_perturbations(base_bundle)

    fhir_source = FhirBundleContextSource(
        bundles_by_patient={"pat-1": perturbed_bundle},
        target_tokens=4096,
    )
    kb_source = StaticKnowledgeSource(
        bundles_by_patient={"pat-1": perturbed_bundle},
    )
    registry = SourceRegistry([fhir_source, kb_source])

    request = ContextRequest(
        messages=[
            {
                "role": "user",
                "content": "Why are both Clopidogrel and Plavix listed as active medications, and why are multiple CKD stages active?",
            }
        ],
        patient="pat-1",
        source="fhir",
        supplemental_sources=("knowledge-base",),
        question="Why are both Clopidogrel and Plavix listed as active medications, and why are multiple CKD stages active?",
    )
    ledger = asyncio.run(registry.build_ledger(request))
    kb_records = [r for r in ledger.records if r.resource_type == "KnowledgeReference"]
    assert len(kb_records) >= 2
    kb_uuids = {r.resource_uuid for r in kb_records}
    assert "wahs-medrec-clopidogrel-plavix" in kb_uuids or "wahs-ckd-staging-exclusivity" in kb_uuids

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
    assert "Plavix" in rendered
