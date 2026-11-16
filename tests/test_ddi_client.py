"""Tests for the decoupled standalone ddi-service HTTP adapter."""

import httpx

from server import ddi_client, drug_safety, team


def test_ddi_service_disabled_when_env_unset(monkeypatch):
    monkeypatch.delenv("DDI_SERVICE_URL", raising=False)
    assert ddi_client.ddi_service_enabled() is False

    chart, mappings, ctx = team._prepare_drug_safety(
        "[1] (2026-01-01) Weight: 60 kg\n",
        [{"index": 1, "text": "Weight: 60 kg"}],
        [],
        "What medications is this patient taking?",
        "2026-01-01",
        True,
    )
    assert not isinstance(ctx, ddi_client.DdiPatientContext)
    res = team._compute_safety_check(ctx, "No active drugs documented [1].", "meds?", True)
    assert res.status in {drug_safety.STATUS_CHECKED, drug_safety.STATUS_LIMITED}


def test_prepare_and_check_delegate_to_ddi_service_and_cache(monkeypatch):
    monkeypatch.setenv("DDI_SERVICE_URL", "http://ddi.test:8090")
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/v1/prepare":
            return httpx.Response(
                200,
                json={
                    "patient_context": {
                        "age_years": 42,
                        "active_drugs": ["stavudine", "zidovudine"],
                        "active_drug_atc_codes": ["J05AF04", "J05AF01"],
                        "mapping_complete": True,
                        "exposure_complete": True,
                        "active_order_count": 2,
                        "mapped_active_order_count": 2,
                        "identity_confidence": "high",
                    },
                    "drug_references": [
                        {
                            "sourceId": "ddi:stavudine-zidovudine",
                            "drug": "stavudine + zidovudine",
                            "title": "DDI: Stavudine + Zidovudine",
                            "text": "Drug reference (stavudine + zidovudine): Major pharmacodynamic antagonism; avoid concurrent use.",
                            "provenance": {"authority": "DDInter 2.0", "version": "1.5"},
                        }
                    ],
                    "proactive_warnings": [
                        {
                            "type": "interaction",
                            "drug": "stavudine + zidovudine",
                            "detail": "Concurrent stavudine and zidovudine is antagonistic.",
                        }
                    ],
                    "medication_safety_facts": "Medication safety context (ddi-service):\n- Active interaction (Major): stavudine + zidovudine",
                    "package": {"name": "ddi-knowledge-base", "version": "1.5"},
                },
            )
        if request.url.path == "/v1/check":
            return httpx.Response(
                200,
                json={
                    "status": "checked",
                    "warnings": [
                        {
                            "type": "interaction",
                            "drug": "stavudine + zidovudine",
                            "detail": "Major interaction between stavudine and zidovudine.",
                        }
                    ],
                    "package": {"name": "ddi-knowledge-base", "version": "1.5"},
                    "coverage": {
                        "mapping_complete": True,
                        "exposure_complete": True,
                        "execution_complete": True,
                        "active_order_count": 2,
                        "mapped_active_order_count": 2,
                    },
                    "identity_confidence": "high",
                    "issues": [],
                },
            )
        raise AssertionError(f"Unexpected path: {request.url.path}")

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def client_factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(ddi_client.httpx, "Client", client_factory)

    chart, mappings, ctx = team._prepare_drug_safety(
        "[1] (2026-01-01) Drug order: Stavudine\n",
        [{"index": 1, "text": "Drug order: Stavudine"}],
        [{"resourceType": "drug_order", "metadata": {"drug_name": "Stavudine"}}],
        "Review medications",
        "2026-01-01",
        True,
        exposure_complete=True,
    )

    assert isinstance(ctx, ddi_client.DdiPatientContext)
    assert "Medication safety context (ddi-service):" in ctx.medication_safety_facts
    assert len(mappings) == 2
    assert mappings[1]["group"] == "reference"
    assert mappings[1]["resourceType"] == "drug_reference"
    assert "[2] Drug reference (stavudine + zidovudine):" in chart

    # Repeated SSE calls with identical answer_text should hit per-turn cache (1 /v1/check call).
    first = team._compute_safety_check(ctx, "Patient is on stavudine [1].", "Review medications", True)
    second = team._compute_safety_check(ctx, "Patient is on stavudine [1].", "Review medications", True)
    assert first.status == drug_safety.STATUS_CHECKED
    assert len(first.warnings) == 1
    assert first == second
    assert calls == ["/v1/prepare", "/v1/check"]


def test_ddi_service_connection_error_fails_open(monkeypatch):
    monkeypatch.setenv("DDI_SERVICE_URL", "http://127.0.0.1:9")
    chart, mappings, ctx = team._prepare_drug_safety(
        "[1] (2026-01-01) Drug order: Metformin\n",
        [{"index": 1, "text": "Drug order: Metformin"}],
        [],
        "Medications?",
        "2026-01-01",
        True,
    )
    assert "[1]" in chart
    assert len(mappings) == 1
    assert isinstance(ctx, ddi_client.DdiPatientContext)
    assert ctx.available is False

    res = team._compute_safety_check(ctx, "Metformin [1].", "Medications?", True)
    assert res.status == drug_safety.STATUS_UNAVAILABLE
    assert "ddi_service_unavailable" in res.issues
