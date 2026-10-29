"""Export patients' QueryStore ledgers exactly as the Hub reads and renders them.

Part of the QueryStore adapter, beside :mod:`querystore_client` and :mod:`chart_serializer`: it
reads through the adapter's client and configuration and renders with the chart serializer, so a
validation fixture or corpus check sees the evidence ledger the Hub builds answers from. It imports
nothing of the answer pipeline, and a Hub deployed without QueryStore never runs it.

Usage, inside the Hub's runtime (which holds the QueryStore configuration)::

    python -m server.querystore_export --patient <uuid> [--patient <uuid> ...]

Prints one JSON object per patient and line: ``patient``, ``hub_revision``, ``snapshot_id``,
``total``, ``records`` (the raw QueryStore records), ``chart_snapshot`` and ``mappings``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List, Optional

from .chart_serializer import render_chart
from .config import querystore_config, resolve_hub_build_revision
from .querystore_client import QueryStoreClient


async def export_ledger(client: QueryStoreClient, patient: str) -> Dict[str, Any]:
    fetch = await client.fetch_patient_ledger(patient)
    records = list(fetch.records or [])
    chart_snapshot, mappings = render_chart(records)
    return {
        "patient": patient,
        "hub_revision": resolve_hub_build_revision(),
        "snapshot_id": fetch.snapshot_id,
        "total": len(records),
        "records": records,
        "chart_snapshot": chart_snapshot,
        "mappings": mappings,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--patient", action="append", required=True, help="patient uuid (repeatable)")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args(argv)
    if not querystore_config.enabled:
        print("ERROR: QueryStore is not configured in this Hub (QUERYSTORE_BASE_URL, _USERNAME, _PASSWORD)", file=sys.stderr)
        return 1
    client = QueryStoreClient(
        querystore_config.base_url,
        querystore_config.username,
        querystore_config.password,
        timeout=args.timeout,
    )
    for patient in args.patient:
        print(json.dumps(asyncio.run(export_ledger(client, patient))), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
