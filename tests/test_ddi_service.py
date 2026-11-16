"""Unit & clinical contract tests for standalone ddi-service."""

from fastapi.testclient import TestClient

from ddi_service.app import app


def test_health_loads_ddinter_kb():
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["drugs"] >= 2000
    assert data["interactions"] >= 290000


def test_clean_hiv_regimen_suppresses_combo_self_ddi_and_unknown_ratings():
    with TestClient(app) as client:
        prep = client.post(
            "/v1/prepare",
            json={
                "records": [
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Stavudine",
                            "concept_uuid": "625AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Lamivudine",
                            "concept_uuid": "628AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Co-trimoxazole",
                            "concept_uuid": "916AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                ],
                "question": "What medications is this patient taking?",
                "exposure_complete": True,
            },
        ).json()
        assert prep["patient_context"]["mapping_complete"] is True
        assert prep["patient_context"]["active_order_count"] == 3
        assert prep["patient_context"]["mapped_active_order_count"] == 3
        assert prep["proactive_warnings"] == []

        chk = client.post(
            "/v1/check",
            json={
                "patient_context": prep["patient_context"],
                "answer_text": "Active medications include stavudine, lamivudine, and co-trimoxazole.",
                "question": "What medications is this patient taking?",
            },
        ).json()
    assert chk["status"] == "checked"
    assert chk["warnings"] == []
    assert chk["issues"] == []


def test_detects_cross_order_stavudine_zidovudine_antagonism_and_delta_ddi():
    with TestClient(app) as client:
        prep = client.post(
            "/v1/prepare",
            json={
                "records": [
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Stavudine",
                            "concept_uuid": "625AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Lamivudine/Zidovudine",
                            "concept_uuid": "630AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Nevirapine",
                            "concept_uuid": "631AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                ],
                "question": "Can we add fluconazole for oral candidiasis?",
                "exposure_complete": True,
            },
        ).json()
        drugs_flagged = {w["drug"].lower() for w in prep["proactive_warnings"]}
        assert any("stavudine" in d and "zidovudine" in d for d in drugs_flagged)
        assert any("fluconazole" in d and "nevirapine" in d for d in drugs_flagged)
        assert "Medication safety context" in prep["medication_safety_facts"]
        assert len(prep["drug_references"]) >= 2

        chk = client.post(
            "/v1/check",
            json={
                "patient_context": prep["patient_context"],
                "answer_text": "You can prescribe fluconazole while continuing stavudine and nevirapine.",
                "question": "Can we add fluconazole?",
            },
        ).json()
    assert chk["status"] == "checked"
    chk_pairs = {w["drug"].lower() for w in chk["warnings"]}
    assert any("fluconazole" in p and "nevirapine" in p for p in chk_pairs)


def test_detects_allergy_contraindication():
    with TestClient(app) as client:
        prep = client.post(
            "/v1/prepare",
            json={
                "records": [
                    {
                        "resourceType": "allergy",
                        "metadata": {"allergen_name": "Sulfonamides"},
                    },
                    {
                        "resourceType": "drug_order",
                        "metadata": {
                            "drug_name": "Co-trimoxazole",
                            "concept_uuid": "916AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "action": "NEW",
                        },
                    },
                ],
                "question": "Any safety issues?",
                "exposure_complete": True,
            },
        ).json()
    assert any(w["type"] == "contraindication" for w in prep["proactive_warnings"])
