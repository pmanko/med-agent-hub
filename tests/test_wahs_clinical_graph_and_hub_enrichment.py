"""Unit tests for Patient Clinical Entity Graph and Med Agent Hub Pre-Generation Enrichment."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

HUB_ROOT = Path(__file__).resolve().parents[1]
if str(HUB_ROOT) not in sys.path:
    sys.path.insert(0, str(HUB_ROOT))


def _ensure_http_and_fastapi_shims_if_missing() -> None:
    """Provide lightweight shims when running inside the stdlib/harness venv without fastapi/httpx."""
    try:
        import httpx  # noqa: F401
    except ModuleNotFoundError:
        httpx_mod = types.ModuleType("httpx")

        class _AsyncClient:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            async def __aenter__(self) -> "_AsyncClient":
                return self

            async def __aexit__(self, *args: Any) -> None:
                return None

            async def post(self, *args: Any, **kwargs: Any) -> Any:
                raise NotImplementedError

            def stream(self, *args: Any, **kwargs: Any) -> Any:
                raise NotImplementedError

        class _HTTPError(Exception):
            pass

        httpx_mod.AsyncClient = _AsyncClient  # type: ignore[attr-defined]
        httpx_mod.HTTPError = _HTTPError  # type: ignore[attr-defined]
        httpx_mod.TimeoutException = _HTTPError  # type: ignore[attr-defined]
        sys.modules["httpx"] = httpx_mod

    try:
        import fastapi  # noqa: F401
    except ModuleNotFoundError:
        fastapi_mod = types.ModuleType("fastapi")
        responses_mod = types.ModuleType("fastapi.responses")

        class _APIRouter:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            def get(self, *args: Any, **kwargs: Any) -> Any:
                return lambda fn: fn

            def post(self, *args: Any, **kwargs: Any) -> Any:
                return lambda fn: fn

        class _HTTPException(Exception):
            def __init__(self, status_code: int = 400, detail: Any = None) -> None:
                super().__init__(str(detail))
                self.status_code = status_code
                self.detail = detail

        class _Request:
            def __init__(self, headers: Dict[str, str] | None = None) -> None:
                self.headers = headers or {}

            async def is_disconnected(self) -> bool:
                return False

        class _Response:
            def __init__(self, content: Any = None, media_type: str | None = None, headers: Dict[str, str] | None = None) -> None:
                self.content = content
                self.media_type = media_type
                self.headers: Dict[str, str] = dict(headers or {})

        class _StreamingResponse(_Response):
            def __init__(self, content: Any = None, media_type: str | None = None, headers: Dict[str, str] | None = None) -> None:
                super().__init__(content=content, media_type=media_type, headers=headers)
                self.body_iterator = content

        fastapi_mod.APIRouter = _APIRouter  # type: ignore[attr-defined]
        fastapi_mod.Header = lambda default=None, **kwargs: default  # type: ignore[attr-defined]
        fastapi_mod.HTTPException = _HTTPException  # type: ignore[attr-defined]
        fastapi_mod.Request = _Request  # type: ignore[attr-defined]
        fastapi_mod.Response = _Response  # type: ignore[attr-defined]
        responses_mod.FileResponse = _Response  # type: ignore[attr-defined]
        responses_mod.HTMLResponse = _Response  # type: ignore[attr-defined]
        responses_mod.Response = _Response  # type: ignore[attr-defined]
        responses_mod.StreamingResponse = _StreamingResponse  # type: ignore[attr-defined]
        sys.modules["fastapi"] = fastapi_mod
        sys.modules["fastapi.responses"] = responses_mod


_ensure_http_and_fastapi_shims_if_missing()

from server.fhir import (  # noqa: E402
    ClinicalEdge,
    ClinicalEntityGraph,
    ClinicalNode,
    HubEnrichmentResult,
    build_wahs_hub_enrichment_block,
    condense_bundle_to_budget,
    estimate_tokens_local,
    extract_bundle,
    parse_omitted_clinical_items,
)
from server.openai_compat import (  # noqa: E402
    ChatCompletionRequest,
     chat_completions,
)
from server.wahs_demo import _builtin_sample_bundles  # noqa: E402


def _build_cardiorenal_test_bundle() -> Dict[str, Any]:
    """Return the built-in Elena Rostova cardiorenal sample bundle augmented with HFrEF, allergy & org note."""
    import base64

    samples = _builtin_sample_bundles()
    bundle = json.loads(json.dumps(samples["Elena_Rostova_Cardiorenal_sample"]))
    entries = bundle.setdefault("entry", [])
    entries.append(
        {
            "resource": {
                "resourceType": "Condition",
                "id": "cond-hfref-1",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "code": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "84114007",
                            "display": "Heart failure with reduced ejection fraction",
                        }
                    ],
                    "text": "Heart failure with reduced ejection fraction",
                },
                "onsetDateTime": "2024-03-15",
            }
        }
    )
    entries.append(
        {
            "resource": {
                "resourceType": "AllergyIntolerance",
                "id": "alg-penicillin-1",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "criticality": "high",
                "code": {
                    "coding": [
                        {
                            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                            "code": "7980",
                            "display": "Penicillin V",
                        }
                    ],
                    "text": "Penicillin",
                },
                "recordedDate": "2023-05-10",
                "reaction": [{"manifestation": [{"text": "Anaphylaxis"}]}],
            }
        }
    )
    note_text = (
        "Assessment and Plan: Walk-in visit at UrgentCare Plus Medical Center. "
        "Patient states she stopped taking spironolactone 5 days ago due to dizziness "
        "and hyperkalemia (potassium 5.7 mmol/L). Advised urgent nephrology and cardiology "
        "follow-up for Stage 3B/4 CKD and HFrEF."
    )
    entries.append(
        {
            "resource": {
                "resourceType": "DocumentReference",
                "id": "note-outside-urgentcare-99",
                "status": "current",
                "date": "2026-01-18T14:00:00Z",
                "type": {"text": "Outside Urgent Care Walk-In Note"},
                "author": [{"display": "UrgentCare Plus Medical Center"}],
                "content": [
                    {
                        "attachment": {
                            "contentType": "text/plain",
                            "data": base64.b64encode(note_text.encode("utf-8")).decode("ascii"),
                        }
                    }
                ],
            }
        }
    )
    return bundle


def test_clinical_entity_graph_from_extracted_chart_and_bundle() -> None:
    bundle = _build_cardiorenal_test_bundle()
    extracted = extract_bundle(bundle)
    graph = ClinicalEntityGraph.from_extracted_chart(extracted)

    assert "Elena" in graph.patient_name
    assert len(graph.nodes) >= 6
    assert len(graph.edges) >= 4

    edge_types = {e.edge_type for e in graph.edges}
    assert "TREATS" in edge_types
    assert "CONTRAINDICATED_OR_DOSE_ADJUST_IN" in edge_types
    assert "MONITORED_BY" in edge_types
    assert "CONTRADICTED_OR_MODIFIED_BY_NOTE" in edge_types
    assert "REPORTED_BY_ORG" in edge_types

    subgraph_lines = graph.format_subgraph_lines(
        question="What medication conflicts and renal dosing concerns exist for her Stage 4 CKD and potassium?"
    )
    assert len(subgraph_lines) >= 3
    joined = "\n".join(subgraph_lines)
    assert "--[" in joined
    assert "[" in joined  # Preserves [N] citation tags


def test_clinical_entity_graph_from_condensed_ledger_lines() -> None:
    condensed_ledger = "\n".join(
        [
            "[1] (2026-01-19) Patient: Elena Rostova, DOB: 1954-04-12, Gender: female, Age: 71",
            "[2] (2026-01-19) Completeness Manifest: Tier 1 | Raw JSON ~42,000 tok -> Condensed ~1,400 tok | bundle_entries=64, rendered_lines=10; omitted_clinical_items=0",
            "[3] (2025-11-10) Condition: Chronic kidney disease, stage 4 (severe) [active] (SNOMED 431857002) [UUID: cond-ckd4]",
            "[4] (2025-11-10) Condition: Heart failure with reduced ejection fraction [active] (SNOMED 84114007) [UUID: cond-hfref]",
            "[5] (2025-11-10) Condition: Atrial fibrillation [active] (SNOMED 49436004) [UUID: cond-afib]",
            "[6] (2025-12-01) Medication [HIGH-ALERT: Anticoagulant]: Apixaban 5 MG Oral Tablet [active] — 5 mg twice daily (RxNorm 1364430) [UUID: med-apix-5]",
            "[7] (2025-12-01) Medication: Spironolactone 25 MG Oral Tablet [active] — 25 mg daily (RxNorm 313096) [UUID: med-spiro-25]",
            "[8] (2025-12-01) Medication: Metformin hydrochloride 1000 MG Oral Tablet [active] — 1000 mg twice daily (RxNorm 861004) [UUID: med-met-1000]",
            "[9] (2026-01-15) Lab Trajectory: Creatinine [Mass/volume] in Serum or Plasma (LOINC 2160-0): latest 2.4 mg/dL on 2026-01-15 (prev: 1.9, 1.6) [UUID: obs-cr-latest]",
            "[10] (2026-01-15) Lab Trajectory: Potassium [Moles/volume] in Serum or Plasma (LOINC 2823-3): latest 5.6 mEq/L on 2026-01-15 (prev: 5.1, 4.7) [UUID: obs-k-latest]",
            "[11] (2026-01-18) Clinical Note: Cardiology Walk-In Note from UrgentCare Plus Clinic — Patient states she stopped taking spironolactone and reduced apixaban to 2.5 mg daily due to bruising [UUID: note-uc-1]",
        ]
    )

    graph = ClinicalEntityGraph.from_condensed_ledger(condensed_ledger)
    assert graph.patient_name == "Elena Rostova"
    assert graph.max_citation_idx == 11
    assert graph.omitted_clinical_items == 0

    edge_breakdown = graph.edge_breakdown()
    assert edge_breakdown.get("TREATS", 0) >= 2
    assert edge_breakdown.get("CONTRAINDICATED_OR_DOSE_ADJUST_IN", 0) >= 2
    assert edge_breakdown.get("MONITORED_BY", 0) >= 2
    assert edge_breakdown.get("CONTRADICTED_OR_MODIFIED_BY_NOTE", 0) >= 1
    assert edge_breakdown.get("REPORTED_BY_ORG", 0) >= 1


def test_hub_enrichment_skips_overflow_when_zero_omitted() -> None:
    bundle = _build_cardiorenal_test_bundle()
    extracted = extract_bundle(bundle)
    condensed_16k = condense_bundle_to_budget(extracted, target_tokens=16000)
    assert int(condensed_16k.completeness_manifest.get("omitted_clinical_items") or 0) == 0

    messages = [
        {
            "role": "user",
            "content": (
                f"{condensed_16k.chart_text}\n\n"
                "USER_QUESTION: Summarize her cardiorenal status, anticoagulation safety, and medication discrepancies."
            ),
        }
    ]
    res = build_wahs_hub_enrichment_block(messages, extracted=extracted, max_tokens=800)

    assert isinstance(res, HubEnrichmentResult)
    assert res.omitted_clinical_items == 0
    assert res.overflow_triggered is False
    assert res.overflow_recovered_count == 0
    assert res.subgraph_nodes >= 4
    assert res.subgraph_edges >= 2
    assert res.kb_Passages_matched >= 1
    assert res.enrichment_tokens <= 800
    assert estimate_tokens_local(res.enrichment_text) <= 800
    assert "[MED-AGENT-HUB ORCHESTRATION: CLINICAL ENTITY SUBGRAPH, QUESTION KB & TIER-0 OVERFLOW RECOVERY]" in res.enrichment_text
    assert "SKIPPED (omitted_clinical_items=0" in res.enrichment_text


def test_hub_enrichment_triggers_tier0_overflow_recovery_when_items_omitted() -> None:
    bundle = _build_cardiorenal_test_bundle()
    extracted = extract_bundle(bundle)

    # Simulate a tight 1k budget condensed prompt where clinical notes and labs were omitted
    tight_prompt = "\n".join(
        [
            "Patient: Elena Rostova (DOB 1954-04-12)",
            "Omitted by Active Budget: 6 items (clinical_note: 2, lab_observation: 4)",
            "[1] (2026-01-19) Patient: Elena Rostova, DOB: 1954-04-12, Gender: female",
            "[2] (2026-01-19) Completeness Manifest: Lossless Structural Condensation | Raw JSON ~45,000 tok -> Condensed ~920 tok | budget_overflow_omitted: clinical_note=2, lab_observation=4; omitted_clinical_items=6",
            "[3] (2025-11-10) Condition: Chronic kidney disease, stage 4 (severe) (SNOMED 431857002) [UUID: cond-ckd4]",
            "[4] (2025-12-01) Medication: Spironolactone 25 MG Oral Tablet [active] (RxNorm 313096) [UUID: med-spiro-25]",
            "[5] (2025-12-01) Medication: Apixaban 5 MG Oral Tablet [active] (RxNorm 1364430) [UUID: med-apix-5]",
            "",
            "USER_QUESTION: What did the outside urgent care note say about spironolactone and apixaban, and what are her latest potassium and creatinine trends?",
        ]
    )

    omitted_total, omitted_cats = parse_omitted_clinical_items(tight_prompt)
    assert omitted_total == 6
    assert omitted_cats.get("clinical_note") == 2
    assert omitted_cats.get("lab_observation") == 4

    res = build_wahs_hub_enrichment_block(
        [{"role": "user", "content": tight_prompt}],
        extracted=extracted,
        max_tokens=800,
    )

    assert res.omitted_clinical_items == 6
    assert res.overflow_triggered is True
    assert res.overflow_recovered_count >= 1
    assert res.max_citation_idx_before == 5
    assert res.max_citation_idx_after == 5 + res.overflow_recovered_count
    assert res.enrichment_tokens <= 800
    assert estimate_tokens_local(res.enrichment_text) <= 800
    assert "1. Tier-0 Overflow Recovery (ACTIVE: omitted_clinical_items=6" in res.enrichment_text
    assert "[6]" in res.enrichment_text
    assert "[UUID:" in res.enrichment_text


def test_openai_compat_wahs_kb_vs_raw_profile_enrichment() -> None:
    captured_payloads: List[Dict[str, Any]] = []

    class _FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {
                "id": "chatcmpl-test-123",
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "SUMMARY:\nPatient has Stage 4 CKD [3] and hyperkalemia [10] on spironolactone [7].\n\nQUESTIONS_FOR_DOCTOR:\n- Should spironolactone be held [7, 10]?",
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 350, "completion_tokens": 45, "total_tokens": 395},
            }

    async def _fake_post(self: Any, url: str, headers: Dict[str, str] | None = None, json: Dict[str, Any] | None = None) -> _FakeResponse:
        if json is not None:
            captured_payloads.append(json)
        return _FakeResponse()

    class _DummyRequest:
        headers: Dict[str, str] = {}

    class _DummyResponse:
        def __init__(self) -> None:
            self.headers: Dict[str, str] = {}

    prompt_content = "\n".join(
        [
            "[1] (2026-01-19) Patient: Elena Rostova, DOB: 1954-04-12, Gender: female",
            "[2] (2026-01-19) Completeness Manifest: Tier 1 | omitted_clinical_items=0",
            "[3] (2025-11-10) Condition: Chronic kidney disease, stage 4 (severe) (SNOMED 431857002) [UUID: cond-ckd4]",
            "[7] (2025-12-01) Medication: Spironolactone 25 MG Oral Tablet [active] (RxNorm 313096) [UUID: med-spiro-25]",
            "[10] (2026-01-15) Lab Trajectory: Potassium [Moles/volume] in Serum or Plasma (LOINC 2823-3): latest 5.6 mEq/L on 2026-01-15 [UUID: obs-k-latest]",
            "",
            "USER_QUESTION: Review her spironolactone safety given her Stage 4 CKD and potassium.",
        ]
    )

    with (
        patch("server.model_provisioner.ensure_local_model_provisioned", new=AsyncMock(return_value=None)),
        patch.object(
            sys.modules["httpx"].AsyncClient,
            "post",
            new=_fake_post,
        ),
    ):
        # 1. Execute with KB-Guided profile (-kb): should inject orchestration block and return hub_enrichment
        captured_payloads.clear()
        resp_obj_kb = _DummyResponse()
        req_kb = ChatCompletionRequest(
            model="wahs-e4b-kb",
            messages=[{"role": "user", "content": prompt_content}],
            stream=False,
        )
        data_kb = asyncio.run(
            chat_completions(req_kb, _DummyRequest(), resp_obj_kb)  # type: ignore[arg-type]
        )
        assert "hub_enrichment" in data_kb
        assert data_kb["hub_enrichment"]["omitted_clinical_items"] == 0
        assert data_kb["hub_enrichment"]["overflow_triggered"] is False
        assert data_kb["hub_enrichment"]["subgraph_edges"] >= 1
        assert data_kb["hub_enrichment"]["enrichment_tokens"] <= 800
        assert "X-WAHS-Hub-Enrichment" in resp_obj_kb.headers

        assert len(captured_payloads) >= 1
        sent_user_msg_kb = captured_payloads[0]["messages"][-1]["content"]
        assert "[MED-AGENT-HUB ORCHESTRATION: CLINICAL ENTITY SUBGRAPH, QUESTION KB & TIER-0 OVERFLOW RECOVERY]" in sent_user_msg_kb
        assert "CONTRAINDICATED_OR_DOSE_ADJUST_IN" in sent_user_msg_kb

        # 2. Execute with Raw Unhinted profile (-raw): should NOT inject orchestration block
        captured_payloads.clear()
        resp_obj_raw = _DummyResponse()
        req_raw = ChatCompletionRequest(
            model="wahs-e4b-raw",
            messages=[{"role": "user", "content": prompt_content}],
            stream=False,
        )
        data_raw = asyncio.run(
            chat_completions(req_raw, _DummyRequest(), resp_obj_raw)  # type: ignore[arg-type]
        )
        assert "hub_enrichment" not in data_raw
        assert "X-WAHS-Hub-Enrichment" not in resp_obj_raw.headers

        assert len(captured_payloads) >= 1
        sent_user_msg_raw = captured_payloads[0]["messages"][-1]["content"]
        assert "[MED-AGENT-HUB ORCHESTRATION:" not in sent_user_msg_raw


def test_openai_compat_wahs_streaming_first_chunk_and_header_enrichment() -> None:
    class _FakeStreamContext:
        async def __aenter__(self) -> "_FakeStreamContext":
            return self

        async def __aexit__(self, *args: Any) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

        async def aiter_lines(self):
            yield 'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"SUMMARY: "}}]}'
            yield 'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Spironolactone [7]."}}]}'
            yield "data: [DONE]"

    class _DummyRequest:
        headers: Dict[str, str] = {}

    class _DummyResponse:
        def __init__(self) -> None:
            self.headers: Dict[str, str] = {}

    prompt_content = "\n".join(
        [
            "[1] (2026-01-19) Patient: Elena Rostova, DOB: 1954-04-12, Gender: female",
            "[2] (2026-01-19) Completeness Manifest: Tier 1 | omitted_clinical_items=0",
            "[3] (2025-11-10) Condition: Chronic kidney disease, stage 4 (severe) (SNOMED 431857002) [UUID: cond-ckd4]",
            "[7] (2025-12-01) Medication: Spironolactone 25 MG Oral Tablet [active] (RxNorm 313096) [UUID: med-spiro-25]",
            "[10] (2026-01-15) Lab Trajectory: Potassium [Moles/volume] in Serum or Plasma (LOINC 2823-3): latest 5.6 mEq/L on 2026-01-15 [UUID: obs-k-latest]",
            "",
            "USER_QUESTION: Review her spironolactone safety given her Stage 4 CKD and potassium.",
        ]
    )

    with (
        patch("server.model_provisioner.ensure_local_model_provisioned", new=AsyncMock(return_value=None)),
        patch.object(
            sys.modules["httpx"].AsyncClient,
            "stream",
            new=lambda self, *args, **kwargs: _FakeStreamContext(),
        ),
    ):
        req_stream = ChatCompletionRequest(
            model="wahs-e4b-kb",
            messages=[{"role": "user", "content": prompt_content}],
            stream=True,
        )
        stream_resp = asyncio.run(
            chat_completions(req_stream, _DummyRequest(), _DummyResponse())  # type: ignore[arg-type]
        )
        assert "X-WAHS-Hub-Enrichment" in stream_resp.headers
        header_meta = json.loads(stream_resp.headers["X-WAHS-Hub-Enrichment"])
        assert header_meta["omitted_clinical_items"] == 0
        assert header_meta["overflow_triggered"] is False

        async def _collect_chunks() -> List[str]:
            out: List[str] = []
            async for item in stream_resp.body_iterator:
                out.append(str(item))
            return out

        chunks = asyncio.run(_collect_chunks())
        first_data = json.loads(chunks[0].strip()[5:].strip())
        assert "hub_enrichment" in first_data
        assert first_data["hub_enrichment"]["subgraph_edges"] >= 1
        second_data = json.loads(chunks[1].strip()[5:].strip())
        assert "hub_enrichment" not in second_data


def test_allergy_matching_avoids_metadata_false_positives_and_catches_true_conflicts() -> None:
    # 1. Extracted chart: Penicillin allergy + non-beta-lactam meds (Metformin, Spironolactone, Apixaban)
    bundle = _build_cardiorenal_test_bundle()
    extracted = extract_bundle(bundle)
    graph = ClinicalEntityGraph.from_extracted_chart(extracted)
    allergy_edges = [
        e for e in graph.edges
        if e.edge_type == "CONTRAINDICATED_OR_DOSE_ADJUST_IN"
        and graph.nodes[e.target_id].category == "AllergyIntolerance"
    ]
    assert allergy_edges == [], f"Expected zero false-positive allergy edges on non-beta-lactams, got: {allergy_edges}"

    # 2. Condensed ledger: Penicillin allergy + Metformin (no edge) vs Amoxicillin (true beta-lactam edge)
    ledger = "\n".join(
        [
            "[1] (2026-01-19) Patient: Elena Rostova, DOB: 1954-04-12, Gender: female",
            "[2] (2023-05-10) Allergy: Penicillin (status: active, verification: confirmed, criticality: high, category: medication, reaction: Anaphylaxis (severe)) [UUID: alg-pen-1]",
            "[3] (2025-12-01) Medication: Metformin hydrochloride 1000 MG Oral Tablet (status: active, category: medication) [UUID: med-met-1]",
            "[4] (2025-12-01) Medication: Amoxicillin 500 MG Oral Capsule (status: active, category: medication) [UUID: med-amox-1]",
        ]
    )
    ledger_graph = ClinicalEntityGraph.from_condensed_ledger(ledger)
    ledger_allergy_edges = [
        e for e in ledger_graph.edges
        if e.edge_type == "CONTRAINDICATED_OR_DOSE_ADJUST_IN"
        and ledger_graph.nodes[e.target_id].category == "AllergyIntolerance"
    ]
    assert len(ledger_allergy_edges) == 1
    assert "Amoxicillin" in ledger_graph.nodes[ledger_allergy_edges[0].source_id].label


def test_ensure_patient_chart_cache_registers_direct_bundle_dicts_from_catalog() -> None:
    import server.fhir.clinical_graph as cg_mod

    sample_bundle = _build_cardiorenal_test_bundle()
    custom_bundle = json.loads(json.dumps(sample_bundle))
    for entry in custom_bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "Patient":
            res["id"] = "catalog-direct-bundle-patient-99"
            res["name"] = [{"family": "CatalogTest", "given": ["DirectBundle"]}]

    orig_cache = dict(cg_mod._EXTRACTED_CHART_CACHE)
    orig_init = cg_mod._CACHE_INITIALIZED
    try:
        cg_mod._EXTRACTED_CHART_CACHE.clear()
        cg_mod._CACHE_INITIALIZED = False
        with patch(
            "server.wahs_demo._load_patient_catalog",
            return_value={"catalog_direct_key": custom_bundle},
        ):
            cg_mod._ensure_patient_chart_cache()
            assert "catalog_direct_key" in cg_mod._EXTRACTED_CHART_CACHE
            assert cg_mod._EXTRACTED_CHART_CACHE["catalog_direct_key"].patient_id == "catalog-direct-bundle-patient-99"
    finally:
        cg_mod._EXTRACTED_CHART_CACHE.clear()
        cg_mod._EXTRACTED_CHART_CACHE.update(orig_cache)
        cg_mod._CACHE_INITIALIZED = orig_init


def test_w29_clause_scoped_note_contradiction_and_question_intent_gating() -> None:
    """Verify W29.1-W29.4:
    1. Stop simvastatin and reconcile Lipitor 40 MG vs Simvastatin 20 MG only flags Simvastatin as stopped.
    2. Narrow historical lookup / abstention queries do not surface unrelated conflict edges or wildcard recoveries.
    3. Tier-0 overflow recovery checks only numbered [N] ledger lines (not Stage 2 KB text) and preserves Assessment and Plan.
    """
    ledger_with_reconcile_note = "\n".join(
        [
            "[1] (2026-01-26) Patient: Julian Mercer, DOB: 1954-10-08, Gender: male",
            "[2] (2026-01-26) Completeness Manifest: Tier 1 | omitted_clinical_items=0",
            "[12] (2026-01-25) Medication: Lipitor 40 MG Oral Tablet [atorvastatin] (status: active, RxNorm: 617311) [UUID: med-lip]",
            "[13] (2025-10-12) Medication: Simvastatin 20 MG Oral Tablet (status: active, RxNorm: 312961) [UUID: med-sim]",
            "[19] (2026-01-26) Clinical Note — Discharge Summary: Assessment and Plan: Stop simvastatin immediately and reconcile duplicate Lipitor 40 MG [atorvastatin] vs Simvastatin 20 MG lipid orders. [UUID: note-disch]",
        ]
    )
    graph = ClinicalEntityGraph.from_condensed_ledger(ledger_with_reconcile_note)
    note_edges = [
        e for e in graph.edges if e.edge_type == "CONTRADICTED_OR_MODIFIED_BY_NOTE"
    ]
    assert len(note_edges) == 1
    assert "Simvastatin" in graph.nodes[note_edges[0].source_id].label
    assert "Lipitor" not in graph.nodes[note_edges[0].source_id].label

    dup_edges = [
        e for e in graph.edges if e.edge_type == "CONTRAINDICATED_OR_DOSE_ADJUST_IN"
    ]
    assert len(dup_edges) == 1

    # Broad reconciliation query surfaces both edges
    broad_lines = graph.format_subgraph_lines(
        question="Please audit my active medications for duplicate statins and discharge hold instructions."
    )
    assert len(broad_lines) == 2

    # Narrow unrelated abstention query surfaces zero conflict edges
    narrow_lines = graph.format_subgraph_lines(
        question="What were the exact results of my coronary artery calcium CT Agatston score and Factor V Leiden genetic panel?"
    )
    assert narrow_lines == []



