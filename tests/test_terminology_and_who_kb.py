"""Tests for deduplicated OpenMRS terminology context, KV-prefix safety, and WHO KB lookup."""

import asyncio
import json

from server import engine, kb, terminology
from server.context_sources import (
    ContextBudget,
    ContextRequest,
    EvidenceRecord,
    QueryStoreSource,
    SourceRegistry,
    StaticKnowledgeSource,
    _ranked_records,
)
from server.levels_loader import compile_execution_spec
from tests.test_context_sources import _FakeQueryStoreClient, _messages


def test_derive_ciel_and_extract_record_codes():
    assert terminology.derive_ciel_code("625AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") == "CIEL:625"
    assert terminology.derive_ciel_code("165085AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") == "CIEL:165085"
    assert terminology.derive_ciel_code("dd8e5c71-1691-11df-97a5-7038c432aabf") is None

    codes = terminology.extract_record_codes(
        {
            "mapping_codes": ["SNOMED-CT:79740000", "CIEL:5334"],
            "concept_uuid": "5334AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "value_coded_uuid": "1065AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        }
    )
    assert codes == ["SNOMED-CT:79740000", "CIEL:5334", "CIEL:1065"]


def test_terminology_context_deduplicates_repeated_concepts():
    records = [
        EvidenceRecord(
            stable_id="obs:cd4-1",
            source="querystore",
            source_priority=10,
            resource_type="obs",
            resource_uuid="o1",
            date="2025-10-01",
            text="CD4 count: 145",
            metadata={
                "concept_name": "CD4 count",
                "concept_uuid": "5497AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "concept_class": "Test",
                "units": "cells/mm3",
                "interpretation": "L",
            },
        ),
        EvidenceRecord(
            stable_id="obs:cd4-2",
            source="querystore",
            source_priority=10,
            resource_type="obs",
            resource_uuid="o2",
            date="2025-09-01",
            text="CD4 count: 180",
            metadata={
                "concept_name": "CD4 count",
                "concept_uuid": "5497AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "concept_class": "Test",
                "units": "cells/mm3",
            },
        ),
        EvidenceRecord(
            stable_id="cond:thrush",
            source="querystore",
            source_priority=10,
            resource_type="condition",
            resource_uuid="c1",
            date="2025-10-22",
            text="Condition: Oral candidiasis",
            metadata={
                "concept_name": "Oral candidiasis",
                "concept_uuid": "5334AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "synonyms": ["Oral thrush"],
                "clinical_status": "ACTIVE",
            },
        ),
        EvidenceRecord(
            stable_id="kb:1",
            source="knowledge-base",
            source_priority=1,
            resource_type="KnowledgeReference",
            resource_uuid="hiv-cd4-role",
            date=None,
            text="Role of CD4 count",
            metadata={"concept_name": "Ignored KB"},
        ),
    ]

    block = terminology.build_terminology_context(records)
    assert block.startswith("Terminology context:\n")
    payload = json.loads(block.split("\n", 1)[1])["terminology_context"]
    concepts = [entry["concept"] for entry in payload]
    assert concepts.count("CD4 count") == 1
    assert "Oral candidiasis" in concepts
    assert "Ignored KB" not in concepts
    cd4 = next(e for e in payload if e["concept"] == "CD4 count")
    assert cd4["codes"] == ["CIEL:5497"]
    assert cd4["units"] == "cells/mm3"


def test_who_kb_matches_ampath_and_standard_ciel_codes():
    d4t_hits = kb.search("patient medications", k=3, codes=["CIEL:625"])
    assert any(row["id"] == "hiv-d4t-phaseout" for row in d4t_hits)

    thrush_hits = kb.search("oral candidiasis", k=3, codes=["CIEL:5334"])
    assert thrush_hits[0]["id"] == "hiv-oral-candidiasis-management"
    assert thrush_hits[0].get("section")

    cotrim_hits = kb.search("", k=3, codes=["CIEL:916"])
    assert any(row["id"] == "hiv-cotrimoxazole" for row in cotrim_hits)


def test_source_registry_surfaces_chart_triggered_who_guidelines_into_ranking():
    client = _FakeQueryStoreClient(
        [
            {
                "resourceType": "drug_order",
                "resourceUuid": "med-d4t",
                "date": "2026-01-05",
                "text": "Drug order: Stavudine 30mg",
                "metadata": {
                    "drug_name": "Stavudine",
                    "concept_name": "Stavudine",
                    "concept_uuid": "625AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "action": "NEW",
                },
            },
            {
                "resourceType": "condition",
                "resourceUuid": "cond-thrush",
                "date": "2026-01-04",
                "text": "Condition: Oral candidiasis. status: ACTIVE",
                "metadata": {
                    "concept_name": "Oral candidiasis",
                    "concept_uuid": "5334AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    "clinical_status": "ACTIVE",
                },
            },
        ]
    )
    registry = SourceRegistry([QueryStoreSource(client), StaticKnowledgeSource()])
    ledger = asyncio.run(
        registry.build_ledger(
            ContextRequest(
                patient="patient-hiv-1",
                messages=_messages(),
                supplemental_sources=("knowledge-base",),
                question="What is going on with this patient?",
            )
        )
    )

    kb_records = [r for r in ledger.records if r.resource_type == "KnowledgeReference"]
    assert kb_records
    ids = {r.resource_uuid for r in kb_records}
    assert ids & {"hiv-d4t-phaseout", "hiv-oral-candidiasis-management"}
    first_kb_mapping = next(m for m in ledger.mappings() if m["resourceType"] == "KnowledgeReference")
    assert first_kb_mapping["group"] == "reference"
    assert first_kb_mapping["provenance"].get("section")

    ranked = _ranked_records(ledger.records, "What is going on with this patient?", recent_core_limit=2)
    reasons_by_id = {record.stable_id: reason for record, reason in ranked}
    assert reasons_by_id["knowledge-base:hiv-d4t-phaseout"] == "chart_kb_guideline"
    assert reasons_by_id["knowledge-base:hiv-oral-candidiasis-management"] == "chart_kb_guideline"


def test_cross_turn_msg0_prefix_stays_byte_identical_with_terminology_and_turn_gathered():
    class ExactWordCounter:
        async def count(self, _model: str, text: str) -> int:
            return len(text.split())

        async def count_records(self, _model: str, texts) -> tuple[int, ...]:
            return tuple(len(text.split()) for text in texts)

    raw_records = [
        {
            "resourceType": "condition",
            "resourceUuid": "cond-1",
            "date": "2026-01-04",
            "text": "Condition: Oral candidiasis. status: ACTIVE",
            "metadata": {
                "concept_name": "Oral candidiasis",
                "concept_uuid": "5334AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "synonyms": ["Oral thrush"],
                "clinical_status": "ACTIVE",
            },
        },
        {
            "resourceType": "drug_order",
            "resourceUuid": "med-1",
            "date": "2026-01-05",
            "text": "Drug order: Stavudine 30mg",
            "metadata": {
                "drug_name": "Stavudine",
                "concept_name": "Stavudine",
                "concept_uuid": "625AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "action": "NEW",
            },
        },
    ]
    client = _FakeQueryStoreClient(raw_records)
    registry = SourceRegistry([QueryStoreSource(client), StaticKnowledgeSource()])
    profile = compile_execution_spec("gemma-4-12b", {"mode": "verified"})

    async def prepare(question: str):
        state = engine._State(messages=[{"role": "user", "content": question}])
        req = engine.ExecutionRequest(
            profile=profile,
            messages=state.messages,
            patient="patient-hiv-1",
            source_registry=registry,
            token_counter=ExactWordCounter(),
        )
        await engine._prepare_context(req, state)
        return state

    state_a = asyncio.run(prepare("Tell me about thrush and candidiasis"))
    state_b = asyncio.run(prepare("What ART changes are needed for d4T?"))

    assert state_a.gathered == state_b.gathered
    assert "Terminology context:" in state_a.gathered
    assert state_a.messages[0]["content"] == state_b.messages[0]["content"]
