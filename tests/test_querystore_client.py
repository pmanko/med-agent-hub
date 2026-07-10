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
