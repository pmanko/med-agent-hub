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
        {"totalCount": 0},
        {"results": [], "totalCount": True},
        {"results": [], "totalCount": 1},
        {"results": {}, "totalCount": 0},
    ],
)
def test_full_chart_rejects_malformed_or_truncated_envelopes(monkeypatch, body):
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

    with pytest.raises(ValueError, match="patient chart|patient chart page"):
        asyncio.run(
            QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
                "patient-1"
            )
        )


def test_full_chart_accepts_thin_endpoint_envelope(monkeypatch):
    request = httpx.Request("GET", "http://openmrs/querystore")

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, *, params):
            body = {
                "results": [{"resourceType": "obs", "resourceUuid": "one"}],
                "totalCount": 1,
            }
            return httpx.Response(
                200,
                json=body,
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    records = asyncio.run(
        QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
            "patient-1"
        )
    )
    assert records == [{"resourceType": "obs", "resourceUuid": "one"}]


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
                    "results": [record],
                    "totalCount": 1,
                },
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    with pytest.raises(ValueError, match="stable identity"):
        asyncio.run(
            QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
                "patient-1"
            )
        )


def test_full_chart_accepts_multiple_pages(monkeypatch):
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
            body = {
                "results": [
                    {"resourceType": "obs", "resourceUuid": "one" if start == 0 else "two"}
                ],
                "totalCount": 2,
            }
            return httpx.Response(
                200,
                json=body,
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    records = asyncio.run(
        QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
            "patient-1", page_size=1
        )
    )

    assert [record["resourceUuid"] for record in records] == ["one", "two"]


def test_full_chart_rejects_duplicate_record_across_pages(monkeypatch):
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
            return httpx.Response(
                200,
                json={
                    "results": [{"resourceType": "obs", "resourceUuid": "one"}],
                    "totalCount": 2,
                },
                request=request,
            )

    monkeypatch.setattr("server.querystore_client.httpx.AsyncClient", Client)

    with pytest.raises(ValueError, match="duplicate"):
        asyncio.run(
            QueryStoreClient("http://openmrs", "service", "secret").get_patient_chart(
                "patient-1", page_size=1
            )
        )
