"""HTTP client for querystore's read API (``GET /ws/rest/v1/querystore/patientrecord``).

The hub is a non-JVM consumer, so it reaches querystore over REST under a service account —
querystore ADR Decision 16's additive HTTP surface for exactly this external-consumer case. This
pages the full patient chart and returns the raw records; :mod:`chart_serializer` renders them to the
``[N] (date) text`` chart text the LLM expects.
"""
from __future__ import annotations

from typing import Any

import httpx


class QueryStoreClient:
    """Reads a patient's chart from querystore over REST. Auth is OpenMRS Basic (a service account)."""

    def __init__(self, base_url: str, username: str, password: str, *, timeout: float = 30.0) -> None:
        # base_url is the OpenMRS app root, e.g. http://harness-openmrs-backend:8080/openmrs
        self._url = base_url.rstrip("/") + "/ws/rest/v1/querystore/patientrecord"
        self._auth = httpx.BasicAuth(username, password)
        self._timeout = timeout

    async def get_patient_chart(self, patient_uuid: str, *, page_size: int = 500) -> list[dict[str, Any]]:
        """The full chart (querystore ``getPatientChart``), reverse-chronological, no ranking.

        Pages until the server's ``totalCount`` is reached, requiring one stable ``snapshotId`` and
        unique record identities across the assembled pages. Raises
        ``httpx.HTTPStatusError`` on a non-2xx (e.g. 404 for an unknown patient, 401/403 on auth) — the
        source adapter translates the failure into an explicit ``context_source_failed`` response.
        """
        records: list[dict[str, Any]] = []
        start = 0
        expected_total: int | None = None
        expected_snapshot: str | None = None
        seen_ids: set[tuple[str, str]] = set()
        async with httpx.AsyncClient(timeout=self._timeout, auth=self._auth) as client:
            while True:
                resp = await client.get(
                    self._url,
                    params={"patient": patient_uuid, "limit": page_size, "startIndex": start},
                )
                resp.raise_for_status()
                body = resp.json()
                if body.get("complete") is not True:
                    raise ValueError(
                        "Querystore did not return an explicitly complete patient chart"
                    )
                snapshot = body.get("snapshotId")
                if not isinstance(snapshot, str) or not snapshot:
                    raise ValueError(
                        "Querystore did not return a complete patient chart snapshot identity"
                    )
                if expected_snapshot is None:
                    expected_snapshot = snapshot
                elif snapshot != expected_snapshot:
                    raise ValueError(
                        "Querystore changed the complete patient chart snapshot while paging"
                    )
                page = body.get("results")
                if not isinstance(page, list):
                    raise ValueError(
                        "Querystore did not return a valid complete patient chart page"
                    )
                for record in page:
                    if not isinstance(record, dict):
                        raise ValueError(
                            "Querystore returned a malformed patient chart record"
                        )
                    resource_type = record.get("resourceType")
                    resource_uuid = record.get("resourceUuid")
                    if (
                        not isinstance(resource_type, str)
                        or not resource_type.strip()
                        or not isinstance(resource_uuid, str)
                        or not resource_uuid.strip()
                    ):
                        raise ValueError(
                            "Querystore returned a patient chart record without a stable identity"
                        )
                    identity = (resource_type, resource_uuid)
                    if identity in seen_ids:
                        raise ValueError(
                            "Querystore returned a duplicate patient chart record while paging"
                        )
                    seen_ids.add(identity)
                records.extend(page)
                total = body.get("totalCount")
                if type(total) is not int or total < 0:
                    raise ValueError(
                        "Querystore did not return a valid complete patient chart total"
                    )
                if expected_total is None:
                    expected_total = total
                elif total != expected_total:
                    raise ValueError(
                        "Querystore changed the complete patient chart total while paging"
                    )
                start += len(page)
                if not page or len(page) < page_size or start >= expected_total:
                    break
        if expected_total is None or len(records) != expected_total:
            raise ValueError(
                "Querystore did not return the complete patient chart: "
                f"expected {expected_total} records, received {len(records)}"
            )
        return records
