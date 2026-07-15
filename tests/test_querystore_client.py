from __future__ import annotations

import asyncio

import httpx
import pytest

from server.querystore_client import QueryStoreClient


def test_non_success_response_is_propagated_to_the_source_adapter(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, *, params):
            return httpx.Response(401, request=request)

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(
            QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
                "patient-1"
            )
        )


@pytest.mark.parametrize(
    "body",
    [
        {"results": [], "totalCount": 0},
        {"results": [], "totalCount": 0, "complete": False},
        {"results": [], "totalCount": True, "complete": True},
        {"results": [{"resourceUuid": "one"}], "totalCount": 2, "complete": True},
        {"results": {}, "totalCount": 0, "complete": True, "snapshotId": "snapshot-1"},
    ],
)
def test_full_chart_rejects_missing_incomplete_or_truncated_envelopes(monkeypatch, body):
    request = httpx.Request("GET", "http://openmrs/querystore")

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, *, params):
            return httpx.Response(200, json=body, request=request)

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    with pytest.raises(ValueError, match="complete patient chart"):
        asyncio.run(
            QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
                "patient-1"
            )
        )


def test_full_chart_accepts_exact_complete_envelope(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, *, params):
            return httpx.Response(
                200,
                json={
                    "results": [{"resourceType": "obs", "resourceUuid": "one"}],
                    "totalCount": 1,
                    "complete": True,
                    "snapshotId": "snapshot-1",
                },
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    records = asyncio.run(
        QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
            "patient-1"
        )
    )
    assert records == [{"resourceType": "obs", "resourceUuid": "one"}]


def test_full_chart_accepts_multiple_pages_from_one_snapshot(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, *, params):
            start = params["startIndex"]
            return httpx.Response(
                200,
                json={
                    "results": [
                        {"resourceType": "obs", "resourceUuid": "one" if start == 0 else "two"}
                    ],
                    "totalCount": 2,
                    "complete": True,
                    "snapshotId": "snapshot-1",
                },
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    records = asyncio.run(
        QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
            "patient-1", page_size=1
        )
    )

    assert [record["resourceUuid"] for record in records] == ["one", "two"]


@pytest.mark.parametrize("second_snapshot", ["snapshot-2", "snapshot-1"])
def test_full_chart_rejects_changed_snapshot_or_duplicate_record(monkeypatch, second_snapshot):
    request = httpx.Request("GET", "http://openmrs/querystore")

    class Client:
        calls = 0

        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, *, params):
            self.calls += 1
            uuid = "one" if self.calls == 1 or second_snapshot == "snapshot-1" else "two"
            return httpx.Response(
                200,
                json={
                    "results": [{"resourceType": "obs", "resourceUuid": uuid}],
                    "totalCount": 2,
                    "complete": True,
                    "snapshotId": "snapshot-1" if self.calls == 1 else second_snapshot,
                },
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    match = "snapshot" if second_snapshot == "snapshot-2" else "duplicate"
    with pytest.raises(ValueError, match=match):
        asyncio.run(
            QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
                "patient-1", page_size=1
            )
        )
