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
    extract_bundle_lossless,
    get_lab_history,
    read_clinical_note,
    run_chart_hygiene_and_reconciliation_skills,
    search_chart_entries,
)
from server.main import app
from server.wahs_demo import _builtin_sample_bundles
from tests.test_fhir_condenser import _make_synthetic_wahs_bundle


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
        assert finding.get("patient_plain_summary")
        assert finding.get("what_to_ask_doctor")
        assert finding.get("clinician_technical_note")
        for cite in finding["citations"]:
            assert int(cite["index"]) in valid_indices
            assert cite["resource_uuid"]

    patterns = analyze_patterns_and_summaries(
        extracted, condensed, sharing_posture="patient_self"
    )
    assert patterns["analysis_type"] == "patterns_and_summaries"
    assert "privacy_diff" in patterns["directed_summary"]
    assert len(patterns["directed_summary"]["privacy_diff"]["retained_in_external_share"]) >= 4
    assert len(patterns["directed_summary"]["privacy_diff"]["removed_in_external_share"]) >= 1
    concepts = {t["concept"] for t in patterns["trajectories"]}
    assert any("Glomerular filtration rate" in c for c in concepts)
    assert any("Hemoglobin A1c" in c for c in concepts)
    assert "Nov 20–28, 2023" in patterns["highlight_encounter"]["title"]
    assert (
        len(patterns["directed_summary"]["sensitive_sdoh_included"]) >= 1
    )

    # Verify external_share redacts all sensitive SDOH across condensed text, directed summary, and DQ findings
    condensed_ext = condense_bundle_to_budget(
        extracted, target_tokens=8192, sharing_posture="external_share"
    )
    patterns_ext = analyze_patterns_and_summaries(
        extracted, condensed_ext, sharing_posture="external_share"
    )
    dq_ext = analyze_data_quality_and_conflicts(
        extracted, condensed_ext, sharing_posture="external_share"
    )
    assert patterns_ext["directed_summary"]["sensitive_sdoh_included"] == []
    assert patterns_ext["directed_summary"]["redacted_privacy_count"] >= 1
    assert "intimate partner abuse" not in condensed_ext.chart_text.lower()
    ext_cond_displays = [c["display"].lower() for c in patterns_ext["directed_summary"]["active_conditions"]]
    assert not any("intimate partner" in d for d in ext_cond_displays)
    for finding in dq_ext["findings"]:
        assert "intimate partner" not in finding["description"].lower()


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
    assert ans_meds["patient_category"] == "1. Medication & Pill-Bottle Clarity"
    assert ans_meds["patient_plain_summary"]
    assert len(ans_meds["what_to_ask_doctor"]) >= 1
    assert ans_meds["clinician_technical_note"]
    assert ans_meds["source_mode"] in ("ips_safety_core", "full_chart_lookup")
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


def test_wahs_overflow_lookup_tools_recover_deep_history_and_enforce_privacy() -> None:
    bundle = _make_synthetic_wahs_bundle()
    extracted = extract_bundle(bundle)

    # 1. Verify search_chart_entries enforces external_share privacy redaction
    search_self = search_chart_entries(
        extracted, query="criminal record", sharing_posture="patient_self"
    )
    search_ext = search_chart_entries(
        extracted, query="criminal record", sharing_posture="external_share"
    )
    assert search_self["total_matches"] >= 1
    assert search_ext["total_matches"] == 0
    assert search_ext["redacted_for_privacy_count"] >= 1

    # 2. Verify get_lab_history retrieves longitudinal eGFR series
    lab_hist = get_lab_history(extracted, "Glomerular filtration rate")
    assert lab_hist["summary"]["total_measurements"] >= 2
    assert len(lab_hist["results"]) >= 2

    # 3. Verify read_clinical_note retrieves deduplicated clinical notes
    note_res = read_clinical_note(extracted, query="diuretics")
    assert note_res["tool"] == "read_clinical_note"
    assert len(note_res["results"]) >= 1

    # 4. Verify hybrid overflow lookup recovers perioperative Cefazolin at tight budget
    condensed_tight = condense_bundle_to_budget(
        extracted, target_tokens=180, sharing_posture="patient_self"
    )
    ans_hybrid = answer_wahs_question(
        extracted,
        "What perioperative antibiotic (cefazolin) was administered during Maurice's hospital admission?",
        condensed_tight,
        target_tokens=180,
        use_lookup_tools=True,
    )
    assert "cefazolin" in ans_hybrid["answer"].lower()
    assert len(ans_hybrid["overflow_tool_calls"]) >= 1
    assert any(c["resource_uuid"] == "medadm-cefazolin" for c in ans_hybrid["citations"])


def test_lossless_structural_condensation_and_chart_hygiene_skills() -> None:
    bundle = _make_synthetic_wahs_bundle()
    # Add two intraday lab observations on the same date with different timestamps & values
    bundle["entry"].extend(
        [
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-egfr-am",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"code": "33914-3", "display": "Glomerular filtration rate"}]},
                    "effectiveDateTime": "2025-11-12T08:15:00Z",
                    "valueQuantity": {"value": 52.0, "unit": "mL/min"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-egfr-pm",
                    "status": "final",
                    "category": [{"coding": [{"code": "laboratory"}]}],
                    "code": {"coding": [{"code": "33914-3", "display": "Glomerular filtration rate"}]},
                    "effectiveDateTime": "2025-11-12T16:45:00Z",
                    "valueQuantity": {"value": 39.0, "unit": "mL/min"},
                }
            },
        ]
    )

    extracted_lossless = extract_bundle_lossless(bundle)
    # 1. Verify no pre-baked conflict annotations or privacy sensitivity tags exist in lossless mode
    for item in extracted_lossless.items:
        assert item.conflict_note is None
        assert item.sensitivity is None
        assert "[conflict_note:" not in item.text
        assert "[sensitivity:" not in item.text

    # 2. Verify intraday timestamps are preserved on Observation items and numeric points
    intraday_items = [
        it for it in extracted_lossless.items if it.resource_uuid in {"obs-egfr-am", "obs-egfr-pm"}
    ]
    assert len(intraday_items) == 2
    assert any("2025-11-12T08:15" in it.text for it in intraday_items)
    assert any("2025-11-12T16:45" in it.text for it in intraday_items)

    # 3. Verify condense_mode="lossless_structural" produces unhinted chart text and preserves intraday range
    condensed_structural = condense_bundle_to_budget(
        bundle,
        target_tokens=8192,
        condense_mode="lossless_structural",
        sharing_posture="patient_self",
    )
    assert condensed_structural.completeness_manifest["condense_mode"] == "lossless_structural"
    assert "[conflict_note:" not in condensed_structural.chart_text
    assert "[sensitivity:" not in condensed_structural.chart_text
    assert "39" in condensed_structural.chart_text and "52" in condensed_structural.chart_text

    # 4. Verify explicit chart-hygiene & reconciliation skills detect duplicates and conflicts on the lossless chart
    skill_report = run_chart_hygiene_and_reconciliation_skills(
        extracted_lossless,
        mappings=condensed_structural.mappings,
    )
    assert skill_report["total_findings"] >= 3
    categories = set(skill_report["counts_by_category"].keys())
    assert "DuplicateCandidate" in categories or "ScheduleAnomaly" in categories
    assert "ReconciliationFinding" in categories
    assert "Source" in skill_report["formatted_findings_block"]



