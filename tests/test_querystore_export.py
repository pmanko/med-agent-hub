"""The QueryStore export: the Hub's own client and serializer produce a complete ledger for
fixture capture and corpus checks, without the answer pipeline."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import httpx
import pytest

import server.querystore_client as querystore_client
import server.querystore_export as querystore_export
from server.config import QueryStoreConfig


def _capped_querystore(records, page_cap, *, projection_complete=True):
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
            "projectionComplete": projection_complete,
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
    monkeypatch.setattr(querystore_export, "resolve_hub_build_revision", lambda: "rev-controlled-1")

    assert querystore_export.main(["--patient", "p1"]) == 0

    exported = json.loads(capsys.readouterr().out)
    assert starts == [0, 2, 4], "pages past the server's cap until totalCount"
    assert exported["patient"] == "p1" and exported["total"] == 5
    assert exported["hub_revision"] == "rev-controlled-1"
    assert exported["snapshot_id"] == "snap-1"
    assert exported["projection_complete"] is True
    assert exported["records"] == records
    assert exported["chart_snapshot"].splitlines()[0].startswith("[1] (2026-01-01) Weight (kg): 70 kg")
    assert [m["resourceUuid"] for m in exported["mappings"]] == [f"o{i}" for i in range(5)]


def test_export_preserves_false_projection_completeness(monkeypatch, capsys):
    records = [
        {"resourceType": "obs", "resourceUuid": "o0", "date": "2026-01-01",
         "text": "Weight (kg): 70 kg", "dateKind": "clinical_event", "metadata": {}}
    ]
    handler, _starts = _capped_querystore(records, page_cap=2, projection_complete=False)
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
    assert exported["projection_complete"] is False


@pytest.mark.parametrize(
    "bad_argv",
    [
        ["--patient", ""],
        ["--patient", "   "],
        ["--patient", "p1", "--timeout", "0"],
        ["--patient", "p1", "--timeout", "-1"],
        ["--patient", "p1", "--timeout", "nan"],
        ["--patient", "p1", "--timeout", "inf"],
    ],
)
def test_export_rejects_blank_patient_or_nonpositive_nonfinite_timeout(bad_argv):
    with pytest.raises(SystemExit) as exc_info:
        querystore_export.main(bad_argv)
    assert exc_info.value.code == 2


def test_export_refuses_without_querystore_configuration(monkeypatch, capsys):
    monkeypatch.setattr(querystore_export, "querystore_config", QueryStoreConfig(base_url=""))
    assert querystore_export.main(["--patient", "p1"]) == 1
    err = capsys.readouterr().err
    assert "not configured" in err
    assert "QUERYSTORE_USERNAME" in err and "QUERYSTORE_PASSWORD" in err


def _local_server_modules(source_text: str) -> set[str]:
    tree = ast.parse(source_text)
    local: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level == 1 and node.module:
                local.add(node.module.split(".")[0])
            elif node.level == 0 and node.module:
                if node.module == "server":
                    local.update(alias.name.split(".")[0] for alias in node.names)
                elif node.module.startswith("server."):
                    local.add(node.module.split(".", 1)[1].split(".")[0])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("server."):
                    local.add(alias.name.split(".", 1)[1].split(".")[0])
    return local


def test_the_export_depends_only_on_the_querystore_adapter():
    """Import boundary: the export belongs to the QueryStore adapter and imports nothing of the
    answer pipeline (engine, team, profiles, prompts, temporal checks)."""
    source_text = Path(querystore_export.__file__).read_text()
    assert _local_server_modules(source_text) == {"chart_serializer", "config", "querystore_client"}
    assert "engine" in _local_server_modules("import server.engine\n" + source_text)
    assert "engine" in _local_server_modules("from server import engine\n" + source_text)
