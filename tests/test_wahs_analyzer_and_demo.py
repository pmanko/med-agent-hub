"""Tests for WAHS+ clinical analyzers, grounded Q&A responder, and FastAPI demo routes."""

from __future__ import annotations

import os

os.environ.setdefault("HUB_BUILD_REVISION", "0123456789abcdef0123456789abcdef01234567")

from fastapi.testclient import TestClient

from server.fhir import (
    analyze_data_quality_and_conflicts,
    analyze_duplicates_and_bloat,
    analyze_patterns_and_summaries,
    answer_wahs_question,
    build_fhir_browser_index,
    build_patient_skills_and_condensation_matrix,
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
    assert "2023-11-21" in patterns["highlight_encounter"]["title"]
    assert "coronary artery bypass" in patterns["highlight_encounter"]["title"].lower()
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
        extracted, target_tokens=512, sharing_posture="patient_self"
    )
    ans_hybrid = answer_wahs_question(
        extracted,
        "What perioperative antibiotic (cefazolin) was administered during Maurice's hospital admission?",
        condensed_tight,
        target_tokens=512,
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


def test_general_us_core_chart_reconciliation_across_drug_classes_and_interactions() -> None:
    import base64

    note_b64 = base64.b64encode(
        b"Hospital Discharge Summary (2025-11-18): Acute on chronic HFrEF with hyperkalemia (K+ 5.6 mmol/L). "
        b"Hold spironolactone and potassium chloride until repeat BMP in 7 days."
    ).decode("ascii")
    us_core_bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "pat-elena-rostova",
                    "name": [{"given": ["Elena"], "family": "Rostova"}],
                    "gender": "female",
                    "birthDate": "1954-04-12",
                }
            },
            {
                "resource": {
                    "resourceType": "Encounter",
                    "id": "enc-hf-admit",
                    "status": "finished",
                    "class": {"code": "IMP"},
                    "type": [{"text": "Inpatient Cardiology Admission (HFrEF & Hyperkalemia)"}],
                    "period": {"start": "2025-11-14", "end": "2025-11-18"},
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-ckd2-uncoded",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"text": "CKD Stage 2"},
                    "onsetDateTime": "2021-05-10",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "cond-ckd3b-coded",
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "code": {"coding": [{"code": "700379002", "display": "Chronic kidney disease stage 3B (disorder)"}]},
                    "onsetDateTime": "2025-11-15",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-aldactone",
                    "status": "active",
                    "medicationCodeableConcept": {"text": "Aldactone 25 MG Oral Tablet [spironolactone]"},
                    "authoredOn": "2025-09-01",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-kcl",
                    "status": "active",
                    "medicationCodeableConcept": {"text": "Potassium Chloride 20 mEq Extended Release Oral Tablet"},
                    "authoredOn": "2025-10-05",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-eliquis",
                    "status": "active",
                    "medicationCodeableConcept": {"text": "Eliquis 5 MG Oral Tablet [apixaban]"},
                    "authoredOn": "2025-10-12",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-warfarin",
                    "status": "active",
                    "medicationCodeableConcept": {"text": "Warfarin Sodium 5 MG Oral Tablet"},
                    "authoredOn": "2025-06-20",
                }
            },
            {
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "doc-hf-discharge",
                    "status": "current",
                    "type": {"text": "Discharge Summary"},
                    "date": "2025-11-18T14:30:00Z",
                    "content": [{"attachment": {"contentType": "text/plain", "data": note_b64}}],
                }
            },
        ],
    }

    dq = analyze_data_quality_and_conflicts(us_core_bundle)
    rule_ids = {f["kb_rule_id"] for f in dq["findings"]}
    assert "wahs-kb-ckd-staging-01" in rule_ids
    assert "wahs-kb-rx-hyperkalemia-01" in rule_ids
    assert "wahs-kb-rx-dual-anticoag-01" in rule_ids
    assert "wahs-kb-note-med-contradiction-01" in rule_ids


    patterns = analyze_patterns_and_summaries(us_core_bundle)
    assert "Elena Rostova" in patterns["highlight_encounter"]["narrative"]
    assert "2025-11-14" in patterns["highlight_encounter"]["window"]

    ans = answer_wahs_question(
        us_core_bundle,
        "Are there any medication conflicts or note instructions about spironolactone, potassium, or blood thinners?",
    )
    assert ans["abstained"] is False
    assert "Aldactone" in ans["answer"]
    assert "Potassium Chloride" in ans["answer"]
    assert "Eliquis" in ans["answer"]
    assert "Warfarin" in ans["answer"]


def test_w15_fhir_browser_condensation_matrix_and_readonly_skills() -> None:
    bundle = _make_synthetic_wahs_bundle()
    # Non-destructive default in extract_bundle_lossless retains clinic room equipment with metadata
    extracted_lossless = extract_bundle_lossless(bundle)
    room_devs = [
        it for it in extracted_lossless.items if it.resource_uuid == "dev-xray-room"
    ]
    assert len(room_devs) == 1
    assert room_devs[0].metadata.get("is_room_equipment") is True

    browser = build_fhir_browser_index(bundle)
    assert browser["total_bundle_entries"] == len(bundle["entry"])
    assert browser["category_counts"]["Conditions"] >= 4
    assert browser["category_counts"]["Medications"] >= 4
    assert browser["category_counts"]["Devices"] >= 2
    assert any(r["decodedNoteText"] for r in browser["resources"] if r["category"] == "Clinical Notes")

    w15 = build_patient_skills_and_condensation_matrix(bundle)
    assert "lossless_structural" in w15["condensation_matrix"]
    assert "hierarchical" in w15["condensation_matrix"]
    for mode in ("lossless_structural", "hierarchical"):
        for budget in ("16384", "8192", "4096", "2048", "1024"):
            cell = w15["condensation_matrix"][mode][budget]
            assert cell["actual_tokens"] <= int(budget)
            assert cell["human_summary"]["what_changed_at_this_lever"]

    skills = w15["skills"]
    assert len(skills) == 9
    everyday = [s for s in skills if s["group"] == "everyday_task"]
    checks = [s for s in skills if s["group"] == "health_check"]
    assert len(everyday) == 5
    assert len(checks) == 4
    for s in skills:
        assert s["plain_english_summary"]
        assert len(s["structured_rows"]) >= 1
        assert len(s["suggested_remediation"]) >= 1


def test_w19_wahs_profiles_discovery_and_execution_surface(monkeypatch) -> None:
    from server import openai_compat
    from server.levels_loader import (
        get_wahs_profile,
        validate_wahs_profiles,
        wahs_profile_ids,
    )

    validated = validate_wahs_profiles()
    ids = wahs_profile_ids()
    assert len(validated) == len(ids)
    assert "wahs-12b-kb" in ids
    assert "wahs-12b-raw" in ids
    assert "wahs-12b-xcheck" in ids
    assert "wahs-openrouter-gemini-2.5-flash-kb" in ids

    kb_prof = get_wahs_profile("wahs-12b-kb")
    raw_prof = get_wahs_profile("wahs-12b-raw")
    assert kb_prof.policies["include_kb_findings"] is True
    assert raw_prof.policies["include_kb_findings"] is False

    # Verify condensation_matrix cells include retained_uuids
    bundle = _make_synthetic_wahs_bundle()
    w15 = build_patient_skills_and_condensation_matrix(bundle)
    cell_8k = w15["condensation_matrix"]["lossless_structural"]["8192"]
    assert len(cell_8k["retained_uuids"]) >= 5

    monkeypatch.setattr(
        openai_compat,
        "_served_backend_model_metadata",
        lambda: {"gemma-4-12b": {"id": "gemma-4-12b"}, "qwen3.5-9b": {"id": "qwen3.5-9b"}},
    )
    client = TestClient(app)

    # Without OpenRouter key: local profiles with loaded models are available, OpenRouter profiles are unavailable
    r_models = client.get("/v1/models?workflow=wahs")
    assert r_models.status_code == 200
    body = r_models.json()
    assert body["workflow"] == "wahs"
    by_id = {m["id"]: m for m in body["data"]}
    assert by_id["wahs-12b-kb"]["available"] is True
    assert by_id["wahs-12b-raw"]["available"] is True
    assert by_id["wahs-12b-xcheck"]["available"] is True
    assert "SUMMARY:" in by_id["wahs-12b-kb"]["system_prompts"]["answer"]
    assert by_id["wahs-openrouter-gemini-2.5-flash-kb"]["available"] is False

    # With X-OpenRouter-Key header: OpenRouter profiles become available
    r_or = client.get(
        "/v1/models?workflow=wahs", headers={"X-OpenRouter-Key": "sk-or-test-key"}
    )
    assert r_or.status_code == 200
    by_id_or = {m["id"]: m for m in r_or.json()["data"]}
    assert by_id_or["wahs-openrouter-gemini-2.5-flash-kb"]["available"] is True

    # Test POST /v1/chat/completions execution for a WAHS profile
    captured_calls = []

    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "SUMMARY:\nGrounded synthesis [1].\n\nQUESTIONS_FOR_DOCTOR:\n- Check Plavix vs clopidogrel.",
                        }
                    }
                ]
            }

    class DummyAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, headers=None, json=None):
            captured_calls.append({"url": url, "headers": headers, "json": json})
            return DummyResponse()

    monkeypatch.setattr(openai_compat.httpx, "AsyncClient", DummyAsyncClient)

    r_comp = client.post(
        "/v1/chat/completions",
        json={
            "model": "wahs-12b-kb",
            "messages": [{"role": "user", "content": "Check my medications."}],
            "stream": False,
        },
    )
    assert r_comp.status_code == 200
    comp_body = r_comp.json()
    assert comp_body["model"] == "wahs-12b-kb"
    assert comp_body["hub_profile"]["id"] == "wahs-12b-kb"
    assert comp_body["hub_profile"]["policies"]["include_kb_findings"] is True
    assert len(captured_calls) == 1
    assert captured_calls[0]["json"]["model"] == "gemma-4-12b"
    assert captured_calls[0]["json"]["messages"][0]["role"] == "system"
    assert "WAHS+" in captured_calls[0]["json"]["messages"][0]["content"]


def test_wahs_review_regressions_privacy_negation_lvef_and_api_validation() -> None:
    import base64
    from scripts.generate_wahs_eval_report import render_markdown_report, score_case_response

    # 1. hub-57-0: external_share redacts SDOH condition conflicts & redacted_for_privacy display
    bundle_sdoh = _builtin_sample_bundles()["Maurice742_Auer97_sample"]
    cond_ext_lossless = condense_bundle_to_budget(
        bundle_sdoh,
        target_tokens=4096,
        condense_mode="lossless_structural",
        sharing_posture="external_share",
    )
    for red in cond_ext_lossless.redacted_for_privacy:
        assert "intimate partner" not in red["display"].lower()
        assert red["display"] == "Redacted privacy-sensitive SDOH record"
    dq_ext_lossless = analyze_data_quality_and_conflicts(
        bundle_sdoh,
        cond_ext_lossless,
        sharing_posture="external_share",
    )
    for finding in dq_ext_lossless["findings"]:
        assert "intimate partner" not in finding["title"].lower()
        assert "intimate partner" not in finding["description"].lower()

    # 2. hub-57-6 & hub-57-21: Negated/conditional note hold guards & per-note med linkage
    long_tail = "TAIL_MARKER_PRESERVED_AT_CHAR_500"
    note_neg_b64 = base64.b64encode(
        (
            "Progress Note (2025-11-19): Patient stable. Do not hold warfarin; continue warfarin 5mg daily. "
            "Hold spironolactone only if potassium exceeds 5.5 mmol/L. "
            + ("Clinical narrative padding. " * 18)
            + long_tail
        ).encode("utf-8")
    ).decode("ascii")
    note_pos_b64 = base64.b64encode(
        b"Nephrology Note (2025-11-20): Acute hyperkalemia. Hold spironolactone immediately."
    ).decode("ascii")
    bundle_notes = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "pat-neg-test",
                    "name": [{"given": ["Test"], "family": "Patient"}],
                    "birthDate": "1955-01-01",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-warfarin-1",
                    "status": "active",
                    "medicationCodeableConcept": {"text": "Warfarin 5 MG Oral Tablet"},
                    "authoredOn": "2025-10-01",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "id": "med-spiro-1",
                    "status": "active",
                    "medicationCodeableConcept": {"text": "Spironolactone 25 MG Oral Tablet"},
                    "authoredOn": "2025-10-01",
                }
            },
            {
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "doc-neg-cond",
                    "status": "current",
                    "type": {"text": "Progress Note"},
                    "date": "2025-11-19T10:00:00Z",
                    "content": [{"attachment": {"contentType": "text/plain", "data": note_neg_b64}}],
                }
            },
            {
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "doc-pos-spiro",
                    "status": "current",
                    "type": {"text": "Nephrology Note"},
                    "date": "2025-11-20T10:00:00Z",
                    "content": [{"attachment": {"contentType": "text/plain", "data": note_pos_b64}}],
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "obs-lvef-55",
                    "status": "final",
                    "code": {"text": "Left ventricular ejection fraction (LVEF)"},
                    "effectiveDateTime": "2020-05-01",
                    "valueQuantity": {"value": 55, "unit": "%"},
                }
            },
        ],
    }
    dq_notes = analyze_data_quality_and_conflicts(bundle_notes)
    note_findings = [
        f for f in dq_notes["findings"] if f["kb_rule_id"] == "wahs-kb-note-med-contradiction-01"
    ]
    assert len(note_findings) == 1
    assert "doc-pos-spiro" in note_findings[0]["resource_uuids"]
    assert "med-spiro-1" in note_findings[0]["resource_uuids"]
    assert "med-warfarin-1" not in note_findings[0]["resource_uuids"]

    # 3. hub-57-10: read_clinical_note returns full decoded note text including tail beyond 320 chars
    ext_notes = extract_bundle_lossless(bundle_notes)
    read_res = read_clinical_note(ext_notes, resource_uuid="doc-neg-cond")
    assert len(read_res["results"]) == 1
    assert long_tail in read_res["results"][0]["text"]
    assert long_tail in read_res["results"][0]["full_text"]

    # 4. hub-57-9 & hub-57-17: LVEF present in bundle but omitted in tight capsule
    cond_no_lvef = condense_bundle_to_budget(bundle_notes, target_tokens=512)
    # Remove LVEF line from chart_text/mappings to simulate tight-budget omission of historical echo
    from dataclasses import replace

    cond_omitted = replace(
        cond_no_lvef,
        chart_text="\n".join(
            line for line in cond_no_lvef.chart_text.splitlines() if "lvef" not in line.lower() and "ejection" not in line.lower()
        ),
        mappings=[m for m in cond_no_lvef.mappings if m.get("resourceUuid") != "obs-lvef-55"],
    )
    ans_lvef_tools = answer_wahs_question(
        ext_notes,
        "What was the patient's left ventricular ejection fraction (LVEF)?",
        cond_omitted,
        use_lookup_tools=True,
    )
    assert ans_lvef_tools["abstained"] is False
    assert "55" in ans_lvef_tools["answer"]
    assert any(c["resource_uuid"] == "obs-lvef-55" for c in ans_lvef_tools["citations"])

    ans_lvef_static = answer_wahs_question(
        ext_notes,
        "What was the patient's left ventricular ejection fraction (LVEF)?",
        cond_omitted,
        use_lookup_tools=False,
    )
    assert ans_lvef_static["abstained"] is True
    assert "Not present in patient FHIR Bundle" not in (ans_lvef_static["abstention_reason"] or "")
    assert "Omitted" in (ans_lvef_static["abstention_reason"] or "")

    # 5. hub-57-3 & hub-57-4: score_case_response & render_markdown_report
    dummy_case = {
        "case_id": "test-case-1",
        "use_case": "A_reconciliation",
        "expect_abstention": False,
        "question": "What CKD stages conflict?",
        "expected_facts": ["CKD Stage 2", "CKD Stage 3"],
    }
    bad_ans = {"abstained": False, "answer": "Bananas are yellow [1].", "citations": [{"index": 1, "resource_uuid": "pat-neg-test"}]}
    scored_bad = score_case_response(dummy_case, bad_ans, cond_no_lvef)
    assert scored_bad["passed"] is False

    md_fail = render_markdown_report(
        {
            "code_coverage": {"overall_coverage_pct": 90.0, "resolved_instances": 9, "total_instances": 10},
            "ablation_rows": [],
            "case_results": [
                {
                    "case_id": "test-case-1",
                    "use_case": "A_reconciliation",
                    "patient_id": "Maurice742_Auer97",
                    "sharing_posture": "patient_self",
                    "prompt": "Prompt",
                    "expect_abstention": False,
                    "actual_abstention": False,
                    "passed": False,
                    "citation_count": 0,
                }
            ],
            "demo_patients": {
                f"{p}_set_b": {
                    "patient_name": p,
                    "birth_date": "1950-01-01",
                    "raw_mb": 1.0,
                    "raw_est_tokens": 1000,
                    "postures": {
                        "patient_self": {
                            "duplicates_and_bloat": {
                                "summary_metrics": {
                                    "total_bundle_entries": 10,
                                    "excluded_non_clinical_entries": 1,
                                    "deduplicated_base64_notes": 0,
                                    "raw_lab_observations": 2,
                                    "canonical_lab_analytes": 1,
                                    "blood_vs_serum_split_analytes": 0,
                                    "raw_stopped_medications": 0,
                                    "folded_stopped_med_ingredients": 0,
                                }
                            },
                            "data_quality_and_conflicts": {"total_findings": 0, "findings": []},
                            "patterns_and_summaries": {"directed_summary": {"sensitive_sdoh_included": []}},
                        },
                        "external_share": {
                            "patterns_and_summaries": {"directed_summary": {"redacted_privacy_count": 0}}
                        },
                    },
                }
                for p in ("Maurice742_Auer97", "Lamar193_Beer512", "Byron202_Emard19")
            },
        }
    )
    assert "| **FAIL** |" in md_fail

    # 6. hub-57-11 & hub-chat-condensation-mode-ignored: API input validation & condense_mode forwarding
    client = TestClient(app)
    for bad_payload in (
        {"fhir_bundle": {}},
        {"fhir_bundle": {"resourceType": "Bundle", "entry": "not-a-list"}},
        {"fhir_bundle": {"resourceType": "Bundle", "entry": []}},
        {"patient_id": "nonexistent_patient_xyz"},
    ):
        r_bad = client.post("/wahs/api/analyze", json=bad_payload)
        assert r_bad.status_code in (400, 404, 422)

    r_chat_tiered = client.post(
        "/wahs/api/chat",
        json={
            "patient_id": "Maurice742_Auer97_sample",
            "condense_mode": "tiered",
            "question": "What duplicate medications are on my list?",
        },
    )
    assert r_chat_tiered.status_code == 200
    assert r_chat_tiered.json()["tier_info"]["condense_mode"] == "tiered"

    # 7. Short Base64 data payload sanitization and max_observations cap in fhir_browser
    from server.fhir.fhir_browser import _sanitize_raw_fhir_for_browser, build_fhir_browser_index

    sanitized_short = _sanitize_raw_fhir_for_browser({"attachment": {"data": "SGVsbG8="}})
    assert sanitized_short["attachment"]["data"].startswith("[Base64 payload (8 chars)")

    browser_capped = build_fhir_browser_index(
        {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Observation",
                        "id": f"obs-{i}",
                        "status": "final",
                        "code": {"text": f"Obs {i}"},
                        "effectiveDateTime": f"2025-01-{(i % 28) + 1:02d}",
                        "valueQuantity": {"value": i, "unit": "mg/dL"},
                    }
                }
                for i in range(10)
            ],
        },
        max_observations=3,
    )
    assert len(browser_capped["resources"]) == 3
    assert browser_capped["omitted_bulk_observation_count"] == 7








