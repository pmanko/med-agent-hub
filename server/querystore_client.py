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

        Pages until the server's ``totalCount`` is reached (or a short page signals the end). Raises
        ``httpx.HTTPStatusError`` on a non-2xx (e.g. 404 for an unknown patient, 401/403 on auth) — the
        source adapter translates the failure into an explicit ``context_source_failed`` response.
        """
        records: list[dict[str, Any]] = []
        start = 0
        async with httpx.AsyncClient(timeout=self._timeout, auth=self._auth) as client:
            while True:
                resp = await client.get(
                    self._url,
                    params={"patient": patient_uuid, "limit": page_size, "startIndex": start},
                )
                resp.raise_for_status()
                body = resp.json()
                page = body.get("results") or []
                records.extend(page)
                total = body.get("totalCount")
                start += len(page)
                if not page or len(page) < page_size or (total is not None and start >= total):
                    break
        return records
