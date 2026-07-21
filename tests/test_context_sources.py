from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

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
from server.patient_ledger_cache import PatientLedgerCache
from server.querystore_client import PatientLedgerFetch


class WordTokenCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def count(self, model: str, text: str) -> int:
        self.calls.append((model, text))
        return len(text.split())


class _RouterResponse:
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


def _patch_router_client(monkeypatch, post, lifecycle=None):
    class Client:
        def __init__(self, **_kwargs):
            if lifecycle is not None:
                lifecycle["created"] = lifecycle.get("created", 0) + 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def aclose(self):
            if lifecycle is not None:
                lifecycle["closed"] = lifecycle.get("closed", 0) + 1

        async def post(self, url, *, json, headers):
            return await post(url, json=json, headers=headers)

    monkeypatch.setattr("server.context_sources.httpx.AsyncClient", Client)


@pytest.fixture(autouse=True)
def _isolate_router_capability_cache():
    RouterTokenCounter.reset_capability_cache()
    yield
    RouterTokenCounter.reset_capability_cache()


def test_router_chat_counter_reuses_one_exact_count_and_client(monkeypatch):
    calls = []
    lifecycle = {}

    async def post(url, *, json, headers):
        calls.append((url, json, headers))
        if url.endswith("/v1/chat/completions/input_tokens"):
            return _RouterResponse(404, {})
        if url.endswith("/apply-template"):
            return _RouterResponse(200, {"prompt": "<turn>user hello<turn>model"})
        return _RouterResponse(200, {"tokens": [1, 2, 3, 4]})

    _patch_router_client(monkeypatch, post, lifecycle)
    counter = RouterTokenCounter("http://router-cache-test")

    async def exercise():
        first = await counter.count_chat(
            "gemma-e4b",
            {
                "messages": [{"role": "user", "content": "hello"}],
                "response_format": {"type": "json_schema"},
            },
        )
        second = await counter.count_chat(
            "gemma-e4b",
            {
                "messages": [{"role": "user", "content": "hello"}],
                "response_format": {"type": "different_generation_grammar"},
                "temperature": 0.0,
                "max_tokens": 512,
                "repeat_penalty": 1.1,
            },
        )
        await counter.aclose()
        return first, second

    assert asyncio.run(exercise()) == (4, 4)
    assert [url.rsplit("/", 1)[-1] for url, _json, _headers in calls] == [
        "input_tokens",
        "apply-template",
        "tokenize",
    ]
    assert lifecycle == {"created": 1, "closed": 1}
    assert counter.stats() == {
        "http_requests": 3,
        "chat_counts_performed": 1,
        "chat_cache_hits": 1,
        "chat_count_mode": "template",
        "cached_prompt_counts": 1,
    }


def test_router_chat_counter_caches_detected_fallback_across_instances(monkeypatch):
    calls = []

    async def post(url, *, json, headers):
        calls.append(url.rsplit("/", 1)[-1])
        if url.endswith("/v1/chat/completions/input_tokens"):
            return _RouterResponse(404, {})
        if url.endswith("/apply-template"):
            return _RouterResponse(200, {"prompt": json["messages"][0]["content"]})
        return _RouterResponse(200, {"tokens": [1, 2]})

    _patch_router_client(monkeypatch, post)

    async def exercise():
        first = RouterTokenCounter("http://router-capability-test")
        second = RouterTokenCounter("http://router-capability-test")
        try:
            await first.count_chat(
                "gemma-e4b", {"messages": [{"role": "user", "content": "one"}]}
            )
            await second.count_chat(
                "gemma-e4b", {"messages": [{"role": "user", "content": "two"}]}
            )
        finally:
            await first.aclose()
            await second.aclose()

    asyncio.run(exercise())

    assert calls == [
        "input_tokens",
        "apply-template",
        "tokenize",
        "apply-template",
        "tokenize",
    ]


def test_router_chat_counter_falls_back_to_template_then_tokenize(monkeypatch):
    calls = []

    async def post(url, *, json, headers):
        calls.append((url, json, headers))
        if url.endswith("/v1/chat/completions/input_tokens"):
            return _RouterResponse(404, {})
        if url.endswith("/apply-template"):
            return _RouterResponse(200, {"prompt": "<turn>user hello<turn>model"})
        return _RouterResponse(200, {"tokens": [1, 2, 3, 4]})

    _patch_router_client(monkeypatch, post)
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


@pytest.mark.parametrize(
    "failing_endpoint",
    ("input_tokens", "apply-template", "tokenize"),
)
def test_router_chat_counter_retries_one_transient_transport_failure(
    monkeypatch, failing_endpoint
):
    attempts = {}

    async def post(url, *, json, headers):
        endpoint = url.rsplit("/", 1)[-1]
        attempts[endpoint] = attempts.get(endpoint, 0) + 1
        if endpoint == failing_endpoint and attempts[endpoint] == 1:
            raise httpx.RemoteProtocolError(
                "Server disconnected without sending a response.",
                request=httpx.Request("POST", url),
            )
        if endpoint == "input_tokens":
            return _RouterResponse(404, {})
        if endpoint == "apply-template":
            return _RouterResponse(200, {"prompt": "<turn>user hello<turn>model"})
        return _RouterResponse(200, {"tokens": [1, 2, 3, 4]})

    _patch_router_client(monkeypatch, post)
    counter = RouterTokenCounter("http://router")

    count = asyncio.run(
        counter.count_chat(
            "gemma-e4b", {"messages": [{"role": "user", "content": "hello"}]}
        )
    )

    assert count == 4
    assert attempts[failing_endpoint] == 2


def test_router_token_counter_stops_after_one_transport_retry(monkeypatch):
    attempts = 0

    async def post(url, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise httpx.RemoteProtocolError(
            "Server disconnected without sending a response.",
            request=httpx.Request("POST", url),
        )

    _patch_router_client(monkeypatch, post)
    counter = RouterTokenCounter("http://router")

    with pytest.raises(ContextSourceError, match="Exact tokenizer unavailable"):
        asyncio.run(counter.count("gemma-e4b", "hello"))

    assert attempts == 2


def test_router_token_counter_does_not_retry_http_errors(monkeypatch):
    attempts = 0

    async def post(_url, **_kwargs):
        nonlocal attempts
        attempts += 1
        return _RouterResponse(400, {})

    _patch_router_client(monkeypatch, post)
    counter = RouterTokenCounter("http://router")

    with pytest.raises(ContextSourceError, match="Exact tokenizer unavailable"):
        asyncio.run(counter.count("gemma-e4b", "hello"))

    assert attempts == 1


def test_router_token_counter_propagates_cancellation_without_retry(monkeypatch):
    attempts = 0

    async def post(_url, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise asyncio.CancelledError

    _patch_router_client(monkeypatch, post)
    counter = RouterTokenCounter("http://router")

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(counter.count("gemma-e4b", "hello"))

    assert attempts == 1


def test_router_record_counter_tokenizes_all_records_in_one_request(monkeypatch):
    calls = []

    async def post(url, *, json, headers):
        calls.append((url, json, headers))
        return _RouterResponse(
            200,
            {
                "tokens": [
                    {"id": index, "piece": character}
                    for index, character in enumerate(json["content"])
                ]
            },
        )

    _patch_router_client(monkeypatch, post)
    counter = RouterTokenCounter("http://router")

    costs = asyncio.run(counter.count_records("gemma-e4b", ("ab", "cde")))

    assert costs == (2, 3)
    assert len(calls) == 1
    assert calls[0][0].endswith("/tokenize")
    assert calls[0][1]["with_pieces"] is True
    assert calls[0][1]["parse_special"] is False


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


def test_inline_allergy_record_is_mandatory_safety_evidence():
    chart = (
        "[1] (2026-01-01) Weight: 70 kg\n"
        "[2] (2025-10-22) Allergy: Penicillins (drug allergen)\n"
    )

    ledger = asyncio.run(
        SourceRegistry([InlineChartSource()]).build_ledger(
            ContextRequest(messages=_messages(chart))
        )
    )

    assert [record.mandatory for record in ledger.records] == [False, True]


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
    assert ledger.mappings()[1]["provenance"] == {
        "authority": "WHO",
        "url": "https://example.test/who",
        "version": "2026",
        "license": "CC BY",
    }


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
    assert "[2] KnowledgeReference (source: knowledge-base):" in ledger.render()


def test_knowledge_source_uses_latest_completed_answer_for_followup_recall(monkeypatch):
    captured = []

    def fake_search(query, k=3):
        captured.append(query)
        if "Prior answer context" in query:
            return [
                {
                    "id": "d4t-guidance",
                    "title": "ART guidance",
                    "text": "Stavudine is not preferred.",
                    "source": "WHO",
                }
            ]
        return []

    monkeypatch.setattr("server.context_sources.kb.search", fake_search)
    source = StaticKnowledgeSource()
    request = ContextRequest(
        messages=(
            {"role": "user", "content": "What regimen is documented?"},
            {"role": "assistant", "content": "The regimen includes stavudine [12]."},
            {"role": "user", "content": "Is that still recommended?"},
        ),
        question="Is that still recommended?",
    )

    ledger = asyncio.run(source.fetch(request))

    assert len(captured) == 2
    assert captured[0] == "Is that still recommended?"
    assert "stavudine" in captured[1]
    assert "[12]" not in captured[1]
    assert [record.stable_id for record in ledger.records] == [
        "knowledge-base:d4t-guidance"
    ]


def test_knowledge_source_protects_current_topic_from_prior_answer_pollution(
    monkeypatch,
):
    def fake_search(query, k=3):
        if "Prior answer context" in query:
            return [
                {"id": "hiv-1", "title": "HIV 1", "text": "ART", "source": "WHO"},
                {"id": "hiv-2", "title": "HIV 2", "text": "ART", "source": "WHO"},
                {"id": "hiv-3", "title": "HIV 3", "text": "ART", "source": "WHO"},
            ]
        return [
            {
                "id": "metformin",
                "title": "Metformin monitoring",
                "text": "Monitor renal function.",
                "source": "Reference",
            },
            {
                "id": "diabetes",
                "title": "Diabetes care",
                "text": "Review glycemic control.",
                "source": "Reference",
            },
        ]

    monkeypatch.setattr("server.context_sources.kb.search", fake_search)
    ledger = asyncio.run(
        StaticKnowledgeSource().fetch(
            ContextRequest(
                messages=(
                    {
                        "role": "assistant",
                        "content": "The prior regimen used stavudine [7].",
                    },
                    {
                        "role": "user",
                        "content": "What monitoring does metformin require?",
                    },
                ),
                question="What monitoring does metformin require?",
            )
        )
    )

    assert [record.stable_id for record in ledger.records] == [
        "knowledge-base:metformin",
        "knowledge-base:diabetes",
        "knowledge-base:hiv-1",
    ]


class _FakeQueryStoreClient:
    """A minimal stand-in for QueryStoreClient: identity attributes + one canned ledger fetch."""

    base_url = "http://openmrs"
    username = "service"

    def __init__(self, records, *, snapshot_id="snap-1", etag='"etag-1"', ranked_hits=None):
        self._records = records
        self._snapshot_id = snapshot_id
        self._etag = etag
        self._ranked_hits = ranked_hits if ranked_hits is not None else []
        self.calls: list[Optional[str]] = []
        self.search_calls: list[str] = []

    async def fetch_patient_ledger(self, _patient, *, if_none_match=None):
        self.calls.append(if_none_match)
        return PatientLedgerFetch(
            not_modified=False,
            records=self._records,
            snapshot_id=self._snapshot_id,
            etag=self._etag,
        )

    async def search_patient_records(self, _patient, query, *, limit=20):
        self.search_calls.append(query)
        return self._ranked_hits


def test_querystore_mapping_keeps_the_matching_raw_record_when_invalid_rows_are_skipped():
    client = _FakeQueryStoreClient(
        [
            {"resourceType": "Observation", "text": "invalid without uuid"},
            {
                "resourceType": "Encounter",
                "resourceUuid": "enc-1",
                "date": "2026-01-01",
                "text": "Visit",
                "metadata": {"mandatory_context": True},
            },
        ]
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )

    assert len(ledger.records) == 1
    assert ledger.records[0].resource_uuid == "enc-1"
    assert ledger.records[0].mandatory is True
    assert ledger.records[0].raw["text"] == "Visit"


def test_querystore_allergy_record_is_mandatory_without_source_metadata():
    client = _FakeQueryStoreClient(
        [
            {
                "resourceType": "AllergyIntolerance",
                "resourceUuid": "allergy-1",
                "date": "2025-10-22",
                "text": "Allergy: Penicillins (drug allergen)",
            }
        ]
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )

    assert ledger.records[0].mandatory is True


def test_querystore_failure_is_explicit_not_an_empty_chart():
    class BrokenClient:
        base_url = "http://openmrs"
        username = "service"

        async def fetch_patient_ledger(self, _patient, *, if_none_match=None):
            raise RuntimeError("backend unavailable")

    with pytest.raises(ContextSourceError) as caught:
        asyncio.run(
            QueryStoreSource(BrokenClient(), cache=PatientLedgerCache()).fetch(
                ContextRequest(patient="patient-1", messages=_messages())
            )
        )

    assert caught.value.code == "context_source_failed"
    assert caught.value.source == "querystore"


def test_querystore_source_reuses_the_shared_cache_across_fetches():
    client = _FakeQueryStoreClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-1", "date": "2026-01-01", "text": "Visit"}]
    )
    cache = PatientLedgerCache()
    source = QueryStoreSource(client, cache=cache)
    request = ContextRequest(patient="patient-1", messages=_messages())

    asyncio.run(source.fetch(request))
    asyncio.run(source.fetch(request))

    # Both turns revalidate (never skip the network call outright), but the second reuses the
    # first's etag rather than acquiring cold.
    assert client.calls == [None, '"etag-1"']


def test_querystore_source_keys_the_cache_by_deployment_identity_and_patient():
    cache = PatientLedgerCache()
    client_a = _FakeQueryStoreClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-a", "date": "2026-01-01", "text": "A"}]
    )
    client_b = _FakeQueryStoreClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-b", "date": "2026-01-01", "text": "B"}]
    )
    client_b.base_url = "http://other-openmrs"

    asyncio.run(
        QueryStoreSource(client_a, cache=cache).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )
    ledger_b = asyncio.run(
        QueryStoreSource(client_b, cache=cache).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )

    # A different deployment for the same patient uuid must not reuse client_a's cached ledger.
    assert ledger_b.records[0].resource_uuid == "enc-b"
    assert len(cache) == 2


def test_querystore_source_skips_ranked_search_when_there_is_no_question():
    client = _FakeQueryStoreClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-1", "date": "2026-01-01", "text": "Visit"}]
    )

    asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages(), question="")
        )
    )

    assert client.search_calls == []


def test_querystore_source_marks_resolved_ranked_hits_with_their_rank():
    last_modified = "2026-01-16T12:00:00Z"
    client = _FakeQueryStoreClient(
        [
            {
                "resourceType": "Encounter",
                "resourceUuid": "enc-1",
                "date": "2026-01-01",
                "text": "Visit",
                "lastModified": last_modified,
            },
            {
                "resourceType": "Observation",
                "resourceUuid": "obs-1",
                "date": "2026-01-02",
                "text": "Weight: 58 kg",
                "lastModified": last_modified,
            },
        ],
        ranked_hits=[
            {"resourceType": "Observation", "resourceUuid": "obs-1", "lastModified": last_modified},
            {"resourceType": "Encounter", "resourceUuid": "enc-1", "lastModified": last_modified},
        ],
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages(), question="what was the weight?")
        )
    )

    assert client.search_calls == ["what was the weight?"]
    by_uuid = {record.resource_uuid: record for record in ledger.records}
    assert by_uuid["obs-1"].querystore_rank == 1
    assert by_uuid["enc-1"].querystore_rank == 2


def test_querystore_source_leaves_unranked_records_with_no_rank():
    client = _FakeQueryStoreClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-1", "date": "2026-01-01", "text": "Visit"}],
        ranked_hits=[],
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages(), question="anything?")
        )
    )

    assert ledger.records[0].querystore_rank is None


def test_querystore_source_degrades_gracefully_when_ranked_search_itself_fails():
    class FlakyRankedSearchClient(_FakeQueryStoreClient):
        async def search_patient_records(self, _patient, query, *, limit=20):
            raise RuntimeError("ranked search backend unavailable")

    client = FlakyRankedSearchClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-1", "date": "2026-01-01", "text": "Visit"}]
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages(), question="anything?")
        )
    )

    # The turn still gets its full ledger; it just proceeds without the ranked bonus.
    assert ledger.records[0].resource_uuid == "enc-1"
    assert ledger.records[0].querystore_rank is None


def test_querystore_source_drops_a_ranked_hit_that_never_resolves_against_the_ledger():
    client = _FakeQueryStoreClient(
        [{"resourceType": "Encounter", "resourceUuid": "enc-1", "date": "2026-01-01", "text": "Visit"}],
        ranked_hits=[{"resourceType": "Observation", "resourceUuid": "ghost", "lastModified": "x"}],
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages(), question="anything?")
        )
    )

    # The ledger still only has the one real record; the unresolvable ghost hit is dropped
    # silently rather than fabricating evidence or failing the turn.
    assert [record.resource_uuid for record in ledger.records] == ["enc-1"]
    assert ledger.records[0].querystore_rank is None


def test_querystore_records_carry_clinical_date_and_date_kind():
    client = _FakeQueryStoreClient(
        [
            {
                "resourceType": "Condition",
                "resourceUuid": "cond-1",
                "date": "2026-01-20",
                "clinicalDate": "2026-01-15",
                "dateKind": "administrative",
                "text": "Condition: Hypertension",
            }
        ]
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )

    record = ledger.records[0]
    assert record.clinical_date == "2026-01-15"
    assert record.date_kind == "administrative"
    mapping = record.mapping(1)
    assert mapping["clinicalDate"] == "2026-01-15"
    assert mapping["dateKind"] == "administrative"


def test_querystore_records_default_clinical_date_and_date_kind_to_none():
    client = _FakeQueryStoreClient(
        [
            {
                "resourceType": "Encounter",
                "resourceUuid": "enc-1",
                "date": "2026-01-01",
                "text": "Visit",
            }
        ]
    )

    ledger = asyncio.run(
        QueryStoreSource(client, cache=PatientLedgerCache()).fetch(
            ContextRequest(patient="patient-1", messages=_messages())
        )
    )

    record = ledger.records[0]
    assert record.clinical_date is None
    assert record.date_kind is None


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
        EvidenceRecord(
            "wrong", "inline", 1, "Obs", "w", "2026-01-02", "Code 312 unrelated"
        ),
        EvidenceRecord(
            "right", "inline", 1, "Obs", "r", "2026-01-01", "Code 12 relevant"
        ),
    )

    ranked = _ranked_records(records, "What happened for code 12?", recent_core_limit=0)

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("right", "exact_match"),
        ("wrong", "zero_relevance"),
    ]


def test_explicit_identifier_does_not_match_an_unlabeled_numeric_value():
    records = (
        EvidenceRecord(
            "wrong",
            "inline",
            1,
            "Observation",
            "wrong",
            "2026-01-02",
            "Potassium: 12 mmol/L",
        ),
        EvidenceRecord(
            "right",
            "inline",
            1,
            "Observation",
            "right",
            "2026-01-01",
            "Code 12: relevant finding",
        ),
    )

    ranked = _ranked_records(records, "What happened for code 12?", recent_core_limit=0)

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("right", "exact_match"),
        ("wrong", "zero_relevance"),
    ]


def test_exact_quoted_phrase_matching_uses_token_boundaries():
    records = (
        EvidenceRecord(
            "wrong", "inline", 1, "Obs", "w", "2026-01-02", "Code 123 unrelated"
        ),
        EvidenceRecord(
            "right", "inline", 1, "Obs", "r", "2026-01-01", "Code 12 relevant"
        ),
    )

    ranked = _ranked_records(
        records, 'What happened for "code 12"?', recent_core_limit=0
    )

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("right", "exact_match"),
        ("wrong", "zero_relevance"),
    ]


def test_common_question_words_do_not_make_a_record_relevant():
    records = (
        EvidenceRecord(
            "irrelevant",
            "inline",
            1,
            "Observation",
            "i",
            "2026-01-02",
            "The patient is stable",
        ),
        EvidenceRecord(
            "medication",
            "inline",
            1,
            "DrugOrder",
            "m",
            "2026-01-01",
            "Drug order: Metformin 500 mg",
        ),
    )

    ranked = _ranked_records(
        records,
        "What medications is the patient taking?",
        recent_core_limit=0,
    )

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("medication", "meaningful_overlap"),
        ("irrelevant", "zero_relevance"),
    ]


def test_age_question_matches_a_birth_record_without_literal_word_overlap():
    record = EvidenceRecord(
        "demographics",
        "inline",
        1,
        "Patient",
        "patient-1",
        None,
        "Patient: Test Person. Born 1984-04-09.",
    )

    ranked = _ranked_records((record,), "How old is this patient?", recent_core_limit=0)

    assert [(item.stable_id, reason) for item, reason in ranked] == [
        ("demographics", "meaningful_overlap")
    ]


def test_direct_relevance_precedes_the_bounded_recent_core():
    routine = tuple(
        EvidenceRecord(
            f"routine-{index:02d}",
            "inline",
            1,
            "Observation",
            f"routine-{index:02d}",
            f"2026-{index // 28 + 1:02d}-{index % 28 + 1:02d}",
            "Routine visit observation",
        )
        for index in range(32)
    )
    relevant = EvidenceRecord(
        "metformin-monitoring",
        "inline",
        1,
        "Observation",
        "metformin-monitoring",
        "2020-01-01",
        "Metformin monitoring requires renal function assessment",
    )

    ranked = _ranked_records(
        (*routine, relevant),
        "What monitoring does metformin require?",
        recent_core_limit=32,
    )

    assert ranked[0] == (relevant, "meaningful_overlap")


def test_temporal_window_number_is_not_an_exact_identifier():
    stage = EvidenceRecord(
        "stage-3",
        "inline",
        1,
        "Observation",
        "stage-3",
        "2026-01-02",
        "Current WHO HIV stage: 3",
    )
    weight = EvidenceRecord(
        "weight",
        "inline",
        1,
        "Observation",
        "weight",
        "2026-01-01",
        "Weight: 70 kg",
    )

    ranked = _ranked_records(
        (stage, weight),
        "Show weight over the last 3 months",
        recent_core_limit=0,
    )

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("weight", "meaningful_overlap"),
        ("stage-3", "zero_relevance"),
    ]


def test_decimal_temporal_window_is_not_numeric_evidence():
    potassium = EvidenceRecord(
        "potassium",
        "inline",
        1,
        "Observation",
        "potassium",
        "2026-01-02",
        "Potassium: 3.5 mmol/L",
    )
    weight = EvidenceRecord(
        "weight",
        "inline",
        1,
        "Observation",
        "weight",
        "2026-01-01",
        "Weight: 70 kg",
    )

    ranked = _ranked_records(
        (potassium, weight),
        "Show weight over the last 3.5 months",
        recent_core_limit=0,
    )

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("weight", "meaningful_overlap"),
        ("potassium", "zero_relevance"),
    ]


def test_bare_order_does_not_turn_a_time_window_into_an_identifier():
    stage = EvidenceRecord(
        "stage-3",
        "inline",
        1,
        "Observation",
        "stage-3",
        "2026-01-02",
        "Current WHO HIV stage: 3",
    )
    medication = EvidenceRecord(
        "metformin",
        "inline",
        1,
        "DrugOrder",
        "metformin",
        "2026-01-01",
        "Metformin medication order",
    )

    ranked = _ranked_records(
        (stage, medication),
        "Order 3 months of metformin",
        recent_core_limit=0,
    )

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("metformin", "meaningful_overlap"),
        ("stage-3", "zero_relevance"),
    ]


@pytest.mark.parametrize("window", ("6-month", "6mo", "6MO", "3.5-month"))
def test_compact_time_window_is_not_relevance_evidence(window):
    interval = EvidenceRecord(
        "interval",
        "inline",
        1,
        "Observation",
        "interval",
        "2026-01-02",
        f"Follow-up interval: {window}",
    )
    weight = EvidenceRecord(
        "weight",
        "inline",
        1,
        "Observation",
        "weight",
        "2026-01-01",
        "Weight: 70 kg",
    )

    ranked = _ranked_records(
        (interval, weight),
        f"Show weight over the last {window} window",
        recent_core_limit=0,
    )

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("weight", "meaningful_overlap"),
        ("interval", "zero_relevance"),
    ]


def test_authority_name_can_select_a_knowledge_reference():
    record = EvidenceRecord(
        "who-guidance",
        "knowledge-base",
        1,
        "KnowledgeReference",
        "who-guidance",
        None,
        "WHO preferred first-line antiretroviral therapy",
    )

    ranked = _ranked_records((record,), "What does WHO recommend?", recent_core_limit=0)

    assert [(item.stable_id, reason) for item, reason in ranked] == [
        ("who-guidance", "meaningful_overlap")
    ]


def test_appointment_question_matches_return_visit_evidence():
    record = EvidenceRecord(
        "return-visit",
        "inline",
        1,
        "Observation",
        "return-visit",
        "2026-01-01",
        "Return visit date: 2026-02-01",
    )

    ranked = _ranked_records(
        (record,), "Is there an upcoming appointment?", recent_core_limit=0
    )

    assert [(item.stable_id, reason) for item, reason in ranked] == [
        ("return-visit", "meaningful_overlap")
    ]


def test_recent_clinical_core_is_bounded_and_deterministic():
    records = (
        EvidenceRecord(
            "old", "inline", 1, "Encounter", "o", "2020-01-01", "Routine visit"
        ),
        EvidenceRecord(
            "new-b", "inline", 1, "Encounter", "b", "2026-01-03", "Routine visit"
        ),
        EvidenceRecord(
            "new-a", "inline", 1, "Encounter", "a", "2026-01-03", "Routine visit"
        ),
        EvidenceRecord(
            "kb",
            "knowledge-base",
            1,
            "KnowledgeReference",
            "k",
            "2027-01-01",
            "Unrelated guidance",
        ),
    )

    ranked = _ranked_records(records, "", recent_core_limit=2)

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("new-a", "recent_core"),
        ("new-b", "recent_core"),
        ("old", "zero_relevance"),
        ("kb", "zero_relevance"),
    ]


def test_active_conditions_have_priority_within_the_bounded_clinical_core():
    records = (
        EvidenceRecord(
            "newest",
            "inline",
            1,
            "ChartRecord",
            "newest",
            "2026-02-01",
            "Routine observation",
        ),
        EvidenceRecord(
            "second-newest",
            "inline",
            1,
            "ChartRecord",
            "second-newest",
            "2026-01-31",
            "Routine observation",
        ),
        EvidenceRecord(
            "active-condition",
            "inline",
            1,
            "ChartRecord",
            "active-condition",
            "2025-01-01",
            "Condition: Chronic disease. Status: ACTIVE.",
        ),
    )

    ranked = _ranked_records(records, "", recent_core_limit=2)

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("active-condition", "active_condition"),
        ("newest", "recent_core"),
        ("second-newest", "zero_relevance"),
    ]


def test_recency_ranking_prefers_clinical_date_over_the_administrative_sort_date():
    records = (
        EvidenceRecord(
            "admin-newer-sort-date",
            "inline",
            1,
            "Condition",
            "cond-1",
            "2026-03-01",  # record_date: an audit fallback, looks newest by sort date alone
            "Condition: Hypertension",
            clinical_date="2025-01-01",  # onset: the real clinical event, actually oldest
            date_kind="administrative",
        ),
        EvidenceRecord(
            "clinical-actual-newest",
            "inline",
            1,
            "Encounter",
            "enc-1",
            "2026-01-15",
            "Visit",
            clinical_date="2026-01-15",
            date_kind="clinical_event",
        ),
    )

    ranked = _ranked_records(records, "", recent_core_limit=2)

    assert [record.stable_id for record, _ in ranked] == [
        "clinical-actual-newest",
        "admin-newer-sort-date",
    ]


def test_querystore_ranked_candidates_are_unioned_in_above_recent_core_below_relevant():
    records = (
        EvidenceRecord(
            "keyword-match",
            "querystore",
            50,
            "Encounter",
            "kw-1",
            "2020-01-01",
            "Weight overlap: this text shares the query token 'weight'",
        ),
        EvidenceRecord(
            "ranked-2nd",
            "querystore",
            50,
            "Observation",
            "obs-2",
            "2026-01-14",
            "Some paraphrase the local keyword ranker would miss",
            querystore_rank=2,
        ),
        EvidenceRecord(
            "ranked-1st",
            "querystore",
            50,
            "Observation",
            "obs-1",
            "2026-01-15",
            "Another paraphrase, ranked closer to the question",
            querystore_rank=1,
        ),
        EvidenceRecord(
            "unranked-recent",
            "querystore",
            50,
            "Encounter",
            "enc-3",
            "2026-01-20",
            "Newest record, but neither keyword-matched nor querystore-ranked",
        ),
    )

    ranked = _ranked_records(records, "weight", recent_core_limit=2)

    assert [(record.stable_id, reason) for record, reason in ranked] == [
        ("keyword-match", "meaningful_overlap"),
        ("ranked-1st", "querystore_ranked"),
        ("ranked-2nd", "querystore_ranked"),
        ("unranked-recent", "recent_core"),
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


def test_history_measurement_defers_when_no_old_turn_can_be_dropped():
    messages = [
        {"role": "user", "content": "latest question"},
        {"role": "assistant", "content": "latest answer [1]"},
        {"role": "user", "content": "current question"},
    ]
    counter = WordTokenCounter()

    view = asyncio.run(
        fit_message_history(
            messages,
            model="gemma-e4b",
            budget=ContextBudget(context_window=4, reserved_output_tokens=1),
            counter=counter,
            fixed_renderer=lambda items: " ".join(
                str(item["content"]) for item in items
            ),
            mandatory_text="mandatory evidence",
            mandatory_ids=("safety",),
            defer_when_no_droppable=True,
        )
    )

    assert counter.calls == []
    assert view.fixed_input_tokens is None
    assert view.dropped_turns == ()
    assert view.stripped_citation_tokens == 1


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
