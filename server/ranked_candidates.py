"""Resolve querystore's per-question ranked search hits against the trusted full-chart ledger.

Querystore's ranked window (``patient + q``) is intentionally uncached and can legitimately
disagree with the full chart it is ranked over: a write race, or a hit whose record has since been
voided and dropped from the full chart (ADR Decision 10 — voided records are deleted from the read
store, not marked). A hit is only trusted once its ``(resourceType, resourceUuid)`` identity is
found in the ledger with a matching ``lastModified``; the *ledger's* copy of the record is what
gets used, never the hit's own fields, so ranked search can only ever reorder already-trusted
evidence, never introduce content the full ledger doesn't vouch for.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable

RefetchLedgerFn = Callable[[], Awaitable[list[dict[str, Any]]]]


def _identity(record: dict[str, Any]) -> tuple[Any, Any]:
    return (record.get("resourceType"), record.get("resourceUuid"))


def _index_by_identity(records: list[dict[str, Any]]) -> dict[tuple[Any, Any], dict[str, Any]]:
    return {_identity(record): record for record in records}


async def resolve_ranked_hits_with_retry(
    hits: list[dict[str, Any]],
    ledger_records: list[dict[str, Any]],
    refetch_ledger: RefetchLedgerFn,
) -> list[dict[str, Any]]:
    """The ledger's own records for ``hits`` that resolve, in the hits' original rank order.

    On any unresolved hit, refetches the ledger once via ``refetch_ledger`` and re-resolves only
    those — a hit still unresolved after that retry (or if the refetch itself fails) is dropped
    rather than raised: ranked candidates are supplementary evidence, never fatal to the turn.
    """
    if not hits:
        return []

    by_identity = _index_by_identity(ledger_records)
    resolved_by_identity: dict[tuple[Any, Any], dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    for hit in hits:
        record = by_identity.get(_identity(hit))
        if record is not None and record.get("lastModified") == hit.get("lastModified"):
            resolved_by_identity[_identity(hit)] = record
        else:
            unresolved.append(hit)

    if unresolved:
        try:
            fresh_ledger = await refetch_ledger()
        except Exception:
            fresh_ledger = []
        fresh_by_identity = _index_by_identity(fresh_ledger)
        for hit in unresolved:
            record = fresh_by_identity.get(_identity(hit))
            if record is not None and record.get("lastModified") == hit.get("lastModified"):
                resolved_by_identity[_identity(hit)] = record

    return [
        resolved_by_identity[_identity(hit)]
        for hit in hits
        if _identity(hit) in resolved_by_identity
    ]
