from __future__ import annotations

import asyncio

import httpx
import pytest

from server.querystore_client import (
    InconsistentSnapshotError,
    PatientLedgerFetch,
    QueryStoreClient,
)


class _FakeResponses:
    """A fake httpx.AsyncClient whose .get() returns one canned response per call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    def __call__(self, **_kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, _url, *, params, headers=None):
        self.requests.append({"params": dict(params), "headers": dict(headers or {})})
        return self._responses.pop(0)


def _page(records, total, snapshot_id="snap-1", etag='"etag-1"', status=200):
    request = httpx.Request("GET", "http://openmrs/querystore")
    body = {"results": records, "totalCount": total}
    if snapshot_id is not None:
        body["snapshotId"] = snapshot_id
    headers = {"ETag": etag} if etag is not None else {}
    return httpx.Response(status, json=body, headers=headers, request=request)


def _run(client_coro):
    return asyncio.run(client_coro)


def test_non_success_response_is_propagated_to_the_source_adapter(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")
    fake = _FakeResponses([httpx.Response(401, request=request)])
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(httpx.HTTPStatusError):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1"
            )
        )


@pytest.mark.parametrize(
    "body",
    [
        {"totalCount": 0},
        {"results": [], "totalCount": True},
        {"results": [], "totalCount": 1},
        {"results": {}, "totalCount": 0},
    ],
)
def test_full_chart_rejects_malformed_or_truncated_envelopes(monkeypatch, body):
    request = httpx.Request("GET", "http://openmrs/querystore")
    body = {**body, "snapshotId": "snap-1"}
    fake = _FakeResponses(
        [httpx.Response(200, json=body, headers={"ETag": '"etag-1"'}, request=request)]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="patient chart|patient chart page"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1"
            )
        )


def test_full_chart_accepts_thin_endpoint_envelope(monkeypatch):
    fake = _FakeResponses(
        [_page([{"resourceType": "obs", "resourceUuid": "one"}], total=1)]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    result = _run(
        QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
            "patient-1"
        )
    )

    assert result == PatientLedgerFetch(
        not_modified=False,
        records=[{"resourceType": "obs", "resourceUuid": "one"}],
        snapshot_id="snap-1",
        etag='"etag-1"',
    )


@pytest.mark.parametrize(
    "record",
    [
        {"resourceType": "", "resourceUuid": "one"},
        {"resourceType": "   ", "resourceUuid": "one"},
        {"resourceType": "obs", "resourceUuid": ""},
        {"resourceType": "obs", "resourceUuid": "\t"},
    ],
)
def test_full_chart_rejects_blank_record_identity(monkeypatch, record):
    fake = _FakeResponses([_page([record], total=1)])
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="stable identity"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1"
            )
        )


def test_full_chart_accepts_multiple_pages(monkeypatch):
    fake = _FakeResponses(
        [
            _page([{"resourceType": "obs", "resourceUuid": "one"}], total=2),
            _page([{"resourceType": "obs", "resourceUuid": "two"}], total=2),
        ]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    result = _run(
        QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
            "patient-1", page_size=1
        )
    )

    assert [record["resourceUuid"] for record in result.records] == ["one", "two"]
    assert result.snapshot_id == "snap-1"


def test_full_chart_rejects_duplicate_record_across_pages(monkeypatch):
    fake = _FakeResponses(
        [
            _page([{"resourceType": "obs", "resourceUuid": "one"}], total=2),
            _page([{"resourceType": "obs", "resourceUuid": "one"}], total=2),
        ]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="duplicate"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1", page_size=1
            )
        )


def test_full_chart_rejects_a_missing_snapshot_id(monkeypatch):
    fake = _FakeResponses(
        [_page([{"resourceType": "obs", "resourceUuid": "one"}], total=1, snapshot_id=None)]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="snapshot"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1"
            )
        )


def test_full_chart_rejects_a_missing_etag(monkeypatch):
    fake = _FakeResponses(
        [_page([{"resourceType": "obs", "resourceUuid": "one"}], total=1, etag=None)]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="etag|ETag"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1"
            )
        )


def test_full_chart_rejects_a_snapshot_id_that_changes_mid_page(monkeypatch):
    fake = _FakeResponses(
        [
            _page(
                [{"resourceType": "obs", "resourceUuid": "one"}],
                total=2,
                snapshot_id="snap-1",
            ),
            _page(
                [{"resourceType": "obs", "resourceUuid": "two"}],
                total=2,
                snapshot_id="snap-2",
            ),
        ]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    # A distinct exception type: this specific failure (the chart mutated mid-pagination) is the
    # one condition callers should retry once before failing explicitly, per the ledger cache's
    # contract; other malformed-response failures below are not given that treatment.
    with pytest.raises(InconsistentSnapshotError, match="snapshot"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1", page_size=1
            )
        )


def test_if_none_match_is_sent_only_on_the_first_page_request(monkeypatch):
    fake = _FakeResponses(
        [
            _page([{"resourceType": "obs", "resourceUuid": "one"}], total=2),
            _page([{"resourceType": "obs", "resourceUuid": "two"}], total=2),
        ]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    _run(
        QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
            "patient-1", page_size=1, if_none_match='"stale-etag"'
        )
    )

    assert fake.requests[0]["headers"] == {"If-None-Match": '"stale-etag"'}
    assert fake.requests[1]["headers"] == {}


def test_a_304_on_the_first_page_short_circuits_without_fetching_further_pages(
    monkeypatch,
):
    request = httpx.Request("GET", "http://openmrs/querystore")
    fake = _FakeResponses([httpx.Response(304, request=request)])
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    result = _run(
        QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
            "patient-1", if_none_match='"current-etag"'
        )
    )

    assert result == PatientLedgerFetch(not_modified=True)
    assert len(fake.requests) == 1


def test_a_304_is_only_honored_on_the_first_page(monkeypatch):
    # A 304 body carries no payload; if a later page somehow returned 304 that would be a
    # server contract violation, not a valid "nothing changed" signal for the whole chart.
    request = httpx.Request("GET", "http://openmrs/querystore")
    fake = _FakeResponses(
        [
            _page([{"resourceType": "obs", "resourceUuid": "one"}], total=2),
            httpx.Response(304, request=request),
        ]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(httpx.HTTPStatusError):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").fetch_patient_ledger(
                "patient-1", page_size=1
            )
        )


def _ranked_page(records, status=200):
    request = httpx.Request("GET", "http://openmrs/querystore")
    # Ranked windows are intentionally uncached: no snapshotId, no ETag, null totalCount.
    return httpx.Response(status, json={"results": records, "totalCount": None}, request=request)


def test_search_patient_records_sends_the_patient_and_question_as_query_params(monkeypatch):
    fake = _FakeResponses([_ranked_page([])])
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    _run(
        QueryStoreClient("http://openmrs", "service", "secret").search_patient_records(
            "patient-1", "what is the latest weight?", limit=5
        )
    )

    assert fake.requests[0]["params"] == {
        "patient": "patient-1",
        "q": "what is the latest weight?",
        "limit": 5,
    }


def test_search_patient_records_returns_hits_in_rank_order(monkeypatch):
    fake = _FakeResponses(
        [
            _ranked_page(
                [
                    {"resourceType": "obs", "resourceUuid": "one", "rank": 1},
                    {"resourceType": "obs", "resourceUuid": "two", "rank": 2},
                ]
            )
        ]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    hits = _run(
        QueryStoreClient("http://openmrs", "service", "secret").search_patient_records(
            "patient-1", "weight"
        )
    )

    assert [hit["resourceUuid"] for hit in hits] == ["one", "two"]


def test_search_patient_records_rejects_a_malformed_page(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")
    fake = _FakeResponses(
        [httpx.Response(200, json={"results": "not-a-list"}, request=request)]
    )
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="ranked search"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").search_patient_records(
                "patient-1", "weight"
            )
        )


def test_search_patient_records_rejects_a_hit_without_a_stable_identity(monkeypatch):
    fake = _FakeResponses([_ranked_page([{"resourceType": "obs", "resourceUuid": ""}])])
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(ValueError, match="stable identity"):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").search_patient_records(
                "patient-1", "weight"
            )
        )


def test_search_patient_records_propagates_non_success_responses(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")
    fake = _FakeResponses([httpx.Response(404, request=request)])
    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", fake)

    with pytest.raises(httpx.HTTPStatusError):
        _run(
            QueryStoreClient("http://openmrs", "service", "secret").search_patient_records(
                "patient-1", "weight"
            )
        )
