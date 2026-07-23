"""HTTP client for querystore's read API (``GET /ws/rest/v1/querystore/patientrecord``).

The hub is a non-JVM consumer, so it reaches querystore over REST under a service account —
querystore ADR Decision 16's additive HTTP surface for exactly this external-consumer case. This
pages the full patient chart, honoring the endpoint's conditional-GET freshness contract, and
returns the raw records; :mod:`chart_serializer` renders them to the ``[N] (date) text`` chart text
the LLM expects. :mod:`patient_ledger_cache` is the caller that supplies a remembered
``if_none_match`` and decides whether to keep serving its cached copy on ``304``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx


class InconsistentSnapshotError(ValueError):
    """A chart mutated mid-pagination: pages within one acquisition disagreed on snapshot id.

    Distinct from other malformed-response failures because it is the one condition
    :mod:`patient_ledger_cache` retries once (a live-write race) before failing explicitly.
    """


@dataclass(frozen=True)
class PatientLedgerFetch:
    """Result of one full-chart acquisition attempt.

    ``not_modified`` is True only when the first page revalidated against a supplied
    ``if_none_match`` etag via HTTP 304 — the caller's existing cached ledger is still current
    and ``records``/``snapshot_id``/``etag`` are unset. Otherwise the complete chart was (re)fetched:
    every page shared one ``snapshot_id`` (a materialization identity, not a cache-invalidation
    guess), and ``etag`` is the first page's strong revalidation token for the *next* acquisition.
    """

    not_modified: bool
    records: Optional[list[dict[str, Any]]] = None
    snapshot_id: Optional[str] = None
    etag: Optional[str] = None


class QueryStoreClient:
    """Reads a patient's chart from querystore over REST. Auth is OpenMRS Basic (a service account)."""

    def __init__(self, base_url: str, username: str, password: str, *, timeout: float = 30.0) -> None:
        # base_url/username are kept (not just folded into the encoded auth header) because
        # patient_ledger_cache keys its bounded cache on source identity + authorization scope.
        self.base_url = base_url.rstrip("/")
        self.username = username
        self._url = self.base_url + "/ws/rest/v1/querystore/patientrecord"
        self._auth = httpx.BasicAuth(username, password)
        self._timeout = timeout

    async def fetch_patient_ledger(
        self,
        patient_uuid: str,
        *,
        page_size: int = 500,
        if_none_match: Optional[str] = None,
    ) -> PatientLedgerFetch:
        """The materialized chart from querystore ``getPatientChart``, newest first.

        Sends ``If-None-Match: {if_none_match}`` on the first page only, when supplied. A ``304``
        there means the complete chart is unchanged, so the fetch stops after one request and
        the caller keeps its existing cached records. Otherwise every page is (re)fetched and
        required to carry one shared ``snapshotId`` and ``totalCount`` — mirroring the per-record
        identity/duplicate checks below — before the acquisition is trusted as complete.

        Raises
        ``httpx.HTTPStatusError`` on a non-2xx/304 (e.g. 404 for an unknown patient, 401/403 on
        auth) — the source adapter translates the failure into an explicit
        ``context_source_failed`` response.
        """
        records: list[dict[str, Any]] = []
        start = 0
        expected_total: Optional[int] = None
        snapshot_id: Optional[str] = None
        etag: Optional[str] = None
        seen_ids: set[tuple[str, str]] = set()
        async with httpx.AsyncClient(timeout=self._timeout, auth=self._auth) as client:
            first_request = True
            while True:
                headers = (
                    {"If-None-Match": if_none_match}
                    if first_request and if_none_match
                    else None
                )
                resp = await client.get(
                    self._url,
                    params={"patient": patient_uuid, "limit": page_size, "startIndex": start},
                    headers=headers,
                )
                if first_request and resp.status_code == 304:
                    return PatientLedgerFetch(not_modified=True)
                first_request = False
                resp.raise_for_status()
                body = resp.json()
                page = body.get("results")
                if not isinstance(page, list):
                    raise ValueError(
                        "Querystore did not return a valid patient chart page"
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
                        "Querystore did not return a valid patient chart total"
                    )
                if expected_total is None:
                    expected_total = total
                elif total != expected_total:
                    raise ValueError(
                        "Querystore changed the patient chart total while paging"
                    )
                page_snapshot = body.get("snapshotId")
                if snapshot_id is None:
                    if not page_snapshot:
                        raise ValueError(
                            "Querystore did not return a snapshot id for the full patient chart"
                        )
                    snapshot_id = page_snapshot
                    etag = resp.headers.get("ETag")
                    if not etag:
                        raise ValueError(
                            "Querystore did not return an ETag for the full patient chart"
                        )
                elif page_snapshot != snapshot_id:
                    raise InconsistentSnapshotError(
                        "Querystore changed the patient chart snapshot while paging"
                    )
                start += len(page)
                if not page or len(page) < page_size or start >= expected_total:
                    break
        if expected_total is None or len(records) != expected_total:
            raise ValueError(
                "Querystore returned a truncated patient chart: "
                f"expected {expected_total} records, received {len(records)}"
            )
        return PatientLedgerFetch(
            not_modified=False, records=records, snapshot_id=snapshot_id, etag=etag
        )

    async def fetch_context_slice(
        self,
        patient_uuid: str,
        question: str,
        *,
        types: set[str] | frozenset[str] | list[str] = frozenset(),
        temporal: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """The tier-tagged context slice (querystore ADR Decision 17) for one question.

        The caller's question interpretation rides as ``types`` (typed-complete resource
        types) and ``temporal`` (recency anchor applies). Each returned record carries a
        ``tier`` — ``mandatory`` records are never droppable downstream. Like ranked search
        this window is question-dependent and uncached: no ``snapshotId``/``ETag``.
        """
        params: dict[str, Any] = {
            "patient": patient_uuid,
            "mode": "context",
            "temporal": "true" if temporal else "false",
            "limit": limit,
        }
        if question:
            params["q"] = question
        if types:
            params["types"] = ",".join(sorted(types))
        async with httpx.AsyncClient(timeout=self._timeout, auth=self._auth) as client:
            resp = await client.get(self._url, params=params)
            resp.raise_for_status()
            body = resp.json()
        rows = body.get("results")
        if not isinstance(rows, list):
            raise ValueError("Querystore did not return a valid context slice page")
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("tier"), str):
                raise ValueError("Querystore returned a context-slice record without a tier")
        return rows

    async def search_patient_records(
        self, patient_uuid: str, query: str, *, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Top-K ranked candidates for one question, via querystore's ``patient + q`` hybrid search.

        Unlike :meth:`fetch_patient_ledger`, this window is intentionally uncached: no
        ``snapshotId`` or ``ETag``, and a hit can legitimately disagree with the full chart (e.g.
        a just-voided record the ranked index hasn't caught up to yet). Callers must resolve each
        hit against the trusted full ledger (:mod:`ranked_candidates`) before using its content —
        this method returns querystore's raw hits, not evidence.
        """
        async with httpx.AsyncClient(timeout=self._timeout, auth=self._auth) as client:
            resp = await client.get(
                self._url, params={"patient": patient_uuid, "q": query, "limit": limit}
            )
            resp.raise_for_status()
            body = resp.json()
        hits = body.get("results")
        if not isinstance(hits, list):
            raise ValueError("Querystore did not return a valid ranked search page")
        for hit in hits:
            if not isinstance(hit, dict):
                raise ValueError("Querystore returned a malformed ranked search record")
            resource_type = hit.get("resourceType")
            resource_uuid = hit.get("resourceUuid")
            if (
                not isinstance(resource_type, str)
                or not resource_type.strip()
                or not isinstance(resource_uuid, str)
                or not resource_uuid.strip()
            ):
                raise ValueError(
                    "Querystore returned a ranked search record without a stable identity"
                )
        return hits
