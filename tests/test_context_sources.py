from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx
import pytest

from server.context_sources import (
    ContextBudget,
    ContextRequest,
    ContextSourceError,
    EvidenceLedger,
    EvidenceRecord,
    InlineChartSource,
    InsufficientContextError,
    QueryStoreSource,
    RouterTokenCounter,
    SourceRegistry,
    StaticKnowledgeSource,
    fit_message_history,
    _ranked_records,
    select_context,
)


class WordTokenCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def count(self, model: str, text: str) -> int:
        self.calls.append((model, text))
        return len(text.split())


def test_router_chat_counter_falls_back_to_template_then_tokenize(monkeypatch):
    calls = []

    class Response:
        def __init__(self, status, body):
            self.status_code = status
            self._body = body
            self.request = httpx.Request("POST", "http://router/test")

        def json(self):
            return self._body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError(
                    "failed",
                    request=self.request,
                    response=httpx.Response(self.status_code, request=self.request),
                )

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, *, json, headers):
            calls.append((url, json, headers))
            if url.endswith("/v1/chat/completions/input_tokens"):
                return Response(404, {})
            if url.endswith("/apply-template"):
                return Response(200, {"prompt": "<turn>user hello<turn>model"})
            return Response(200, {"tokens": [1, 2, 3, 4]})

    monkeypatch.setattr("server.context_sources.httpx.AsyncClient", Client)
    counter = RouterTokenCounter("http://router")

    count = asyncio.run(
        counter.count_chat(
            "gemma-e4b",
            {
                "messages": [{"role": "user", "content": "hello"}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {"name": "chart_answer"},
                },
            },
        )
    )

    assert count == 4
    assert [url.rsplit("/", 1)[-1] for url, _json, _headers in calls] == [
        "input_tokens",
        "apply-template",
        "tokenize",
    ]
    assert calls[0][1]["response_format"]["json_schema"]["name"] == "chart_answer"
    assert set(calls[1][1]) == {"model", "messages"}
    assert calls[-1][1]["parse_special"] is True


def _messages(chart: str = "") -> list[dict[str, str]]:
    messages = [{"role": "system", "content": "system"}]
    if chart:
        messages.append(
            {
                "role": "user",
                "content": "Patient records (most recent first):\n" + chart,
            }
        )
    messages.append({"role": "user", "content": "What is the latest weight?"})
    return messages


def test_inline_context_works_without_querystore_configuration():
    chart = "[1] (2026-01-01) Weight: 70 kg\n[2] (2025-01-01) Weight: 68 kg\n"
    registry = SourceRegistry([InlineChartSource()])

    ledger = asyncio.run(
        registry.build_ledger(ContextRequest(messages=_messages(chart)))
    )

    assert ledger.source_names == ("inline",)
    assert ledger.render() == chart
    assert [record.stable_id for record in ledger.records] == ["inline:1", "inline:2"]


def test_patient_without_a_patient_source_or_inline_chart_fails_explicitly():
    registry = SourceRegistry([InlineChartSource()])

    with pytest.raises(ContextSourceError) as caught:
        asyncio.run(
            registry.build_ledger(
                ContextRequest(patient="patient-1", messages=_messages())
            )
        )

    assert caught.value.code == "context_source_unavailable"
    assert caught.value.source == "auto"


@dataclass
class AlternateSource:
    name: str = "alternate"
    priority: int = 20
    supports_patient: bool = True

    async def fetch(self, request: ContextRequest) -> EvidenceLedger:
        return EvidenceLedger(
            records=(
                EvidenceRecord(
                    stable_id=f"alternate:{request.patient}",
                    source="alternate",
                    source_priority=self.priority,
                    resource_type="Observation",
                    resource_uuid="obs-1",
                    date="2026-01-02",
                    text="(2026-01-02) Weight: 71 kg",
                ),
            )
        )


def test_mock_alternate_patient_source_uses_the_same_contract():
    registry = SourceRegistry([InlineChartSource(), AlternateSource()])

    ledger = asyncio.run(
        registry.build_ledger(
            ContextRequest(
                patient="patient-1", messages=_messages(), source="alternate"
            )
        )
    )

    assert ledger.source_names == ("alternate",)
    assert ledger.records[0].resource_uuid == "obs-1"


def test_requested_source_list_composes_patient_and_knowledge_evidence(monkeypatch):
    monkeypatch.setattr(
        "server.context_sources.kb.search",
        lambda _query, k=3: [
            {
                "id": "who-1",
                "title": "WHO guidance",
                "text": "Use chart-specific clinical judgment.",
                "source": "WHO",
                "url": "https://example.test/who",
                "version": "2026",
                "license": "CC BY",
            }
        ],
    )
    registry = SourceRegistry([InlineChartSource(), StaticKnowledgeSource()])
    chart = "[1] (2026-01-01) Weight: 70 kg\n"

    ledger = asyncio.run(
        registry.build_ledger(
            ContextRequest(
                messages=_messages(chart),
                sources=("inline", "knowledge-base"),
                question="How should weight be interpreted?",
            )
        )
    )

    assert ledger.source_names == ("inline", "knowledge-base")
    assert [record.stable_id for record in ledger.records] == [
        "inline:1",
        "knowledge-base:who-1",
    ]


def test_supplemental_source_uses_the_same_normalized_ledger(monkeypatch):
    monkeypatch.setattr(
        "server.context_sources.kb.search",
        lambda _query, k=3: [
            {
                "id": "kb-1",
                "title": "Reference",
                "text": "Normalized guidance.",
                "source": "Fixture",
            }
        ],
    )
    registry = SourceRegistry([InlineChartSource(), StaticKnowledgeSource()])

    ledger = asyncio.run(
        registry.build_ledger(
            ContextRequest(
                messages=_messages("[1] (2026-01-01) Weight: 70 kg\n"),
                supplemental_sources=("knowledge-base",),
                question="How should weight be interpreted?",
            )
        )
    )

    assert ledger.source_names == ("inline", "knowledge-base")
    assert ledger.records[1].stable_id == "knowledge-base:kb-1"
    assert ledger.mappings()[1]["resourceType"] == "KnowledgeReference"


def test_querystore_mapping_keeps_the_matching_raw_record_when_invalid_rows_are_skipped():
    class FakeClient:
        async def get_patient_chart(self, _patient):
            return [
                {"resourceType": "Observation", "text": "invalid without uuid"},
                {
                    "resourceType": "Encounter",
                    "resourceUuid": "enc-1",
                    "date": "2026-01-01",
                    "text": "Visit",
                    "metadata": {"mandatory_context": True},
                },
            ]

    ledger = asyncio.run(
        QueryStoreSource(FakeClient()).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )

    assert len(ledger.records) == 1
    assert ledger.records[0].resource_uuid == "enc-1"
    assert ledger.records[0].mandatory is True
    assert ledger.records[0].raw["text"] == "Visit"


def test_querystore_failure_is_explicit_not_an_empty_chart():
    class BrokenClient:
        async def get_patient_chart(self, _patient):
            raise RuntimeError("backend unavailable")

    with pytest.raises(ContextSourceError) as caught:
        asyncio.run(
            QueryStoreSource(BrokenClient()).fetch(
                ContextRequest(patient="patient-1", messages=_messages())
            )
        )

    assert caught.value.code == "context_source_failed"
    assert caught.value.source == "querystore"


def _ledger(*records: EvidenceRecord) -> EvidenceLedger:
    return EvidenceLedger(records=records)


def test_small_chart_is_preserved_byte_for_byte_with_exact_counting():
    ledger = _ledger(
        EvidenceRecord(
            "r1", "inline", 10, "Obs", "1", "2026-01-02", "(2026-01-02) Weight: 71 kg"
        ),
        EvidenceRecord(
            "r2", "inline", 10, "Obs", "2", "2025-01-02", "(2025-01-02) Weight: 69 kg"
        ),
    )
    counter = WordTokenCounter()

    view = asyncio.run(
        select_context(
            ledger,
            question="latest weight",
            model="gemma-e4b",
            budget=ContextBudget(context_window=100, reserved_output_tokens=10),
            counter=counter,
            fixed_text="system question",
        )
    )

    assert view.mode == "full"
    assert view.render() == ledger.render()
    assert view.excluded == ()
    assert counter.calls


def test_oversized_selection_is_stable_and_records_reasons():
    ledger = _ledger(
        EvidenceRecord(
            "safety",
            "drug",
            100,
            "Safety",
            "s",
            "2020-01-01",
            "Allergy: ibuprofen",
            mandatory=True,
        ),
        EvidenceRecord(
            "exact", "inline", 10, "Obs", "e", "2024-01-01", "Weight: 71 kg code WT-71"
        ),
        EvidenceRecord(
            "recent", "inline", 10, "Encounter", "r", "2026-01-01", "Routine visit"
        ),
        EvidenceRecord(
            "old", "inline", 10, "Encounter", "o", "2019-01-01", "Old routine visit"
        ),
    )
    counter = WordTokenCounter()
    budget = ContextBudget(context_window=19, reserved_output_tokens=4)

    first = asyncio.run(
        select_context(
            ledger,
            question="What is weight WT-71?",
            model="gemma-e4b",
            budget=budget,
            counter=counter,
            fixed_text="fixed prompt",
        )
    )
    second = asyncio.run(
        select_context(
            ledger,
            question="What is weight WT-71?",
            model="gemma-e4b",
            budget=budget,
            counter=counter,
            fixed_text="fixed prompt",
        )
    )

    assert first == second
    assert first.mode == "selected"
    assert first.included_ids[:2] == ("safety", "exact")
    assert [(item.stable_id, item.reason) for item in first.included[:2]] == [
        ("safety", "mandatory"),
        ("exact", "exact_match"),
    ]
    assert first.excluded
    assert all(item.reason for item in first.excluded)


def test_exact_identifier_matching_does_not_use_numeric_substrings():
    records = (
        EvidenceRecord("wrong", "inline", 1, "Obs", "w", "2026-01-02", "Code 312 unrelated"),
        EvidenceRecord("right", "inline", 1, "Obs", "r", "2026-01-01", "Code 12 relevant"),
    )

    ranked = _ranked_records(records, "What happened for code 12?")

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("right", "exact_match"),
        ("wrong", "ranked"),
    ]


def test_exact_quoted_phrase_matching_uses_token_boundaries():
    records = (
        EvidenceRecord("wrong", "inline", 1, "Obs", "w", "2026-01-02", "Code 123 unrelated"),
        EvidenceRecord("right", "inline", 1, "Obs", "r", "2026-01-01", "Code 12 relevant"),
    )

    ranked = _ranked_records(records, 'What happened for "code 12"?')

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("right", "exact_match"),
        ("wrong", "ranked"),
    ]


def test_mandatory_context_overflow_abstains_instead_of_truncating():
    ledger = _ledger(
        EvidenceRecord(
            "safety",
            "drug",
            100,
            "Safety",
            "s",
            "2020-01-01",
            "mandatory safety evidence that cannot be dropped",
            mandatory=True,
        )
    )

    with pytest.raises(InsufficientContextError) as caught:
        asyncio.run(
            select_context(
                ledger,
                question="question",
                model="gemma-e4b",
                budget=ContextBudget(context_window=5, reserved_output_tokens=2),
                counter=WordTokenCounter(),
                fixed_text="fixed prompt",
            )
        )

    assert caught.value.code == "insufficient_context"
    assert caught.value.mandatory_ids == ("safety",)


def test_multiturn_budget_strips_old_refs_and_drops_oldest_complete_turn():
    messages = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "old question"},
        {"role": "assistant", "content": "old answer [1]"},
        {"role": "user", "content": "recent question"},
        {"role": "assistant", "content": "recent answer [2]"},
        {"role": "user", "content": "current question"},
    ]

    view = asyncio.run(
        fit_message_history(
            messages,
            model="gemma-e4b",
            budget=ContextBudget(context_window=10, reserved_output_tokens=1),
            counter=WordTokenCounter(),
            fixed_renderer=lambda items: " ".join(
                str(item["content"]) for item in items
            ),
        )
    )

    contents = [str(message["content"]) for message in view.messages]
    assert view.dropped_turns == ("turn:1",)
    assert view.stripped_citation_tokens == 2
    assert "old question" not in contents
    assert "recent question" in contents
    assert "recent answer" in contents
    assert all("[2]" not in content for content in contents)
    assert contents[-1] == "current question"


def test_multiturn_minimum_overflow_abstains_without_dropping_latest_turn():
    messages = [
        {"role": "user", "content": "latest question"},
        {"role": "assistant", "content": "latest answer"},
        {"role": "user", "content": "current question"},
    ]

    with pytest.raises(InsufficientContextError, match="latest completed turn"):
        asyncio.run(
            fit_message_history(
                messages,
                model="gemma-e4b",
                budget=ContextBudget(context_window=4, reserved_output_tokens=1),
                counter=WordTokenCounter(),
                fixed_renderer=lambda items: " ".join(
                    str(item["content"]) for item in items
                ),
                mandatory_text="mandatory evidence",
                mandatory_ids=("safety",),
            )
        )


def test_drug_injection_preserves_inline_record_provenance():
    from server.engine import _ledger_after_drug_injection

    ledger = EvidenceLedger(
        (
            EvidenceRecord(
                stable_id="inline:1",
                source="inline",
                source_priority=10,
                resource_type="ChartRecord",
                resource_uuid=None,
                date="2026-01-01",
                text="(2026-01-01) Allergy: Penicillin",
            ),
        ),
        original_text="[1] (2026-01-01) Allergy: Penicillin\n",
    )

    rebuilt = _ledger_after_drug_injection(ledger, ledger.render(), ledger.mappings())

    assert rebuilt.records[0].stable_id == "inline:1"
    assert rebuilt.records[0].source == "inline"
    assert rebuilt.records[0].resource_type == "ChartRecord"
