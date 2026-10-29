"""The QueryStore export: the Hub's own client and serializer produce a complete ledger for
fixture capture and corpus checks, without the answer pipeline."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import httpx

import server.querystore_client as querystore_client
import server.querystore_export as querystore_export
from server.config import QueryStoreConfig


def _capped_querystore(records, page_cap):
    """A QueryStore that returns at most ``page_cap`` records a page, as OpenMRS does."""
    starts = []

    def handler(request: httpx.Request) -> httpx.Response:
        start = int(request.url.params["startIndex"])
        starts.append(start)
        body = {
            "results": records[start : start + page_cap],
            "totalCount": len(records),
            "snapshotId": "snap-1",
            "chartTruncated": False,
            "projectionComplete": True,
        }
        return httpx.Response(200, json=body, headers={"ETag": '"e1"'})

    return handler, starts


def test_export_prints_the_complete_ledger_as_the_hub_renders_it(monkeypatch, capsys):
    records = [
        {"resourceType": "obs", "resourceUuid": f"o{i}", "date": f"2026-01-0{i + 1}",
         "text": f"Weight (kg): {70 + i} kg", "dateKind": "clinical_event", "metadata": {}}
        for i in range(5)
    ]
    handler, starts = _capped_querystore(records, page_cap=2)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        querystore_client.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    monkeypatch.setattr(
        querystore_export,
        "querystore_config",
        QueryStoreConfig(base_url="http://querystore/openmrs", username="reader", password="secret"),
    )

    assert querystore_export.main(["--patient", "p1"]) == 0

    exported = json.loads(capsys.readouterr().out)
    assert starts == [0, 2, 4], "pages past the server's cap until totalCount"
    assert exported["patient"] == "p1" and exported["total"] == 5
    assert exported["snapshot_id"] == "snap-1"
    assert exported["chart_snapshot"].splitlines()[0].startswith("[1] (2026-01-01) Weight (kg): 70 kg")
    assert [m["resourceUuid"] for m in exported["mappings"]] == [f"o{i}" for i in range(5)]


def test_export_refuses_without_querystore_configuration(monkeypatch, capsys):
    monkeypatch.setattr(querystore_export, "querystore_config", QueryStoreConfig(base_url=""))
    assert querystore_export.main(["--patient", "p1"]) == 1
    assert "not configured" in capsys.readouterr().err


def test_the_export_depends_only_on_the_querystore_adapter():
    """Import boundary: the export belongs to the QueryStore adapter and imports nothing of the
    answer pipeline (engine, team, profiles, prompts, temporal checks)."""
    tree = ast.parse(Path(querystore_export.__file__).read_text())
    local = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module
    }
    assert local == {"chart_serializer", "config", "querystore_client"}
