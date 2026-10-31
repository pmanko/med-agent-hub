"""Tests for WAHS+ clinical analyzers, grounded Q&A responder, and FastAPI demo routes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from server.fhir import (
    analyze_data_quality_and_conflicts,
    analyze_duplicates_and_bloat,
    analyze_patterns_and_summaries,
    answer_wahs_question,
    condense_bundle_to_budget,
    extract_bundle,
)
from server.main import app
from server.wahs_demo import _builtin_sample_bundles


def test_wahs_analyzers_detect_bloat_conflicts_and_trajectories() -> None:
    bundle = _builtin_sample_bundles()["Maurice742_Auer97_sample"]
    extracted = extract_bundle(bundle)
    condensed = condense_bundle_to_budget(
        extracted, target_tokens=8192, sharing_posture="patient_self"
    )

    bloat = analyze_duplicates_and_bloat(extracted, condensed)
    assert bloat["analysis_type"] == "duplicates_and_bloat"
    assert bloat["summary_metrics"]["total_bundle_entries"] == len(bundle["entry"])
    assert len(bloat["cards"]) >= 3

    dq = analyze_data_quality_and_conflicts(
        extracted, condensed, sharing_posture="patient_self"
    )
    assert dq["analysis_type"] == "data_quality_and_conflicts"
    rule_ids = {f["kb_rule_id"] for f in dq["findings"]}
    assert "wahs-kb-ckd-staging-01" in rule_ids
    assert "wahs-kb-glycemic-exclusivity-01" in rule_ids
    assert "wahs-kb-rx-brand-generic-01" in rule_ids
    assert "wahs-kb-rx-duplicate-statin-01" in rule_ids
    assert "wahs-kb-iis-dedup-01" in rule_ids
    assert "wahs-kb-acip-zoster-01" in rule_ids
    assert "wahs-kb-sdoh-privacy-01" in rule_ids

    # Every finding with citations must map to valid [N] indices in condensed.mappings
    valid_indices = {int(m["index"]) for m in condensed.mappings}
    for finding in dq["findings"]:
        for cite in finding["citations"]:
            assert int(cite["index"]) in valid_indices
            assert cite["resource_uuid"]

    patterns = analyze_patterns_and_summaries(
        extracted, condensed, sharing_posture="patient_self"
    )
    assert patterns["analysis_type"] == "patterns_and_summaries"
    concepts = {t["concept"] for t in patterns["trajectories"]}
    assert any("Glomerular filtration rate" in c for c in concepts)
    assert any("Hemoglobin A1c" in c for c in concepts)
    assert "Nov 20–28, 2023" in patterns["highlight_encounter"]["title"]
    assert (
        len(patterns["directed_summary"]["sensitive_sdoh_included"]) >= 1
    )

    # Verify external_share redacts all sensitive SDOH
    condensed_ext = condense_bundle_to_budget(
        extracted, target_tokens=8192, sharing_posture="external_share"
    )
    patterns_ext = analyze_patterns_and_summaries(
        extracted, condensed_ext, sharing_posture="external_share"
    )
    assert patterns_ext["directed_summary"]["sensitive_sdoh_included"] == []
    assert patterns_ext["directed_summary"]["redacted_privacy_count"] >= 1
    assert "intimate partner abuse" not in condensed_ext.chart_text.lower()


def test_answer_wahs_question_grounds_claims_and_abstains_on_unrecorded_facts() -> None:
    bundle = _builtin_sample_bundles()["Maurice742_Auer97_sample"]
    extracted = extract_bundle(bundle)
    condensed = condense_bundle_to_budget(
        extracted, target_tokens=8192, sharing_posture="patient_self"
    )

    # 1. Answerable medication duplicate question
    ans_meds = answer_wahs_question(
        extracted,
        "What duplicate medications or statin conflicts are on my active list?",
        condensed,
    )
    assert ans_meds["abstained"] is False
    assert "Plavix" in ans_meds["answer"]
    assert "Clopidogrel" in ans_meds["answer"]
    assert "Lipitor" in ans_meds["answer"]
    assert "Simvastatin" in ans_meds["answer"]
    assert len(ans_meds["citations"]) >= 2

    # 2. Unanswerable abstention probes
    for probe in (
        "Does this patient have a documented Penicillin allergy or recent colonoscopy?",
        "What was the result of Maurice's APOE4 genetic test and brain MRI?",
        "What was his left ventricular ejection fraction (LVEF %) on echocardiogram?",
        "What was his peanut-specific IgE level and epinephrine auto-injector dose?",
    ):
        res = answer_wahs_question(extracted, probe, condensed)
        assert res["abstained"] is True, f"Expected abstention for probe: {probe}"
        assert "Not Recorded in Chart" in res["answer"]

    # 3. External share privacy abstention on sensitive SDOH query
    condensed_ext = condense_bundle_to_budget(
        extracted, target_tokens=8192, sharing_posture="external_share"
    )
    res_priv = answer_wahs_question(
        extracted,
        "Does this patient have a history of intimate partner violence or substance abuse?",
        condensed_ext,
        sharing_posture="external_share",
    )
    assert res_priv["abstained"] is True
    assert "Privacy-Protected" in res_priv["answer"]


def test_wahs_fastapi_routes_patients_analyze_chat_and_demo() -> None:
    client = TestClient(app)

    r_patients = client.get("/wahs/api/patients")
    assert r_patients.status_code == 200
    patients = r_patients.json()["patients"]
    assert len(patients) >= 1
    pid = patients[0]["id"]

    r_analyze = client.post(
        "/wahs/api/analyze",
        json={
            "patient_id": pid,
            "target_tokens": 4096,
            "sharing_posture": "patient_self",
        },
    )
    assert r_analyze.status_code == 200
    body = r_analyze.json()
    assert body["condensation"]["actual_tokens"] <= 4096
    assert "duplicates_and_bloat" in body["analyses"]
    assert "data_quality_and_conflicts" in body["analyses"]
    assert "patterns_and_summaries" in body["analyses"]

    r_chat = client.post(
        "/wahs/api/chat",
        json={
            "patient_id": pid,
            "target_tokens": 4096,
            "sharing_posture": "patient_self",
            "question": "Why are there conflicting Chronic Kidney Disease stages?",
        },
    )
    assert r_chat.status_code == 200
    chat_body = r_chat.json()
    assert chat_body["abstained"] is False
    assert "CKD" in chat_body["answer"] or "Kidney" in chat_body["answer"]
    assert len(chat_body["citations"]) >= 1

    r_demo = client.get("/wahs/demo")
    assert r_demo.status_code == 200
    assert "WAHS+ Clinical Chart Condenser" in r_demo.text
