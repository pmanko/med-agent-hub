"""Bounded, memory-only, single-flight cache for a patient's full querystore chart.

Revalidates on every access using the first page's conditional ETag (RFC 9110/9111): an unchanged
chart costs one HTTP request; a changed chart is refetched and replaces the cached ledger. This
never skips revalidation to save the round trip — the roadmap requires every turn to revalidate,
not just charts believed stale by a TTL.

A concurrent request for the same key is serialized behind a per-key lock rather than triggering a
duplicate full-chart fetch, so a burst of turns for one patient cannot stampede querystore; once the
first caller completes, the rest each still revalidate (typically a cheap 304) before returning —
this cache never substitutes another caller's answer for a stopped-short fetch of its own.

Never serves a cached ledger after a failed fetch or an unresolved snapshot race (this module's
``get_records`` only ever returns records it just validated) — the roadmap's "never serve stale
context after source or validation failure." A chart mutated mid-pagination
(:class:`~server.querystore_client.InconsistentSnapshotError`) is retried once before that failure
propagates; every other failure propagates immediately without a retry.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Hashable, Optional

from .querystore_client import InconsistentSnapshotError, PatientLedgerFetch

FetchFn = Callable[[Optional[str]], Awaitable[PatientLedgerFetch]]


@dataclass(frozen=True)
class _CacheEntry:
    records: tuple[dict[str, Any], ...]
    snapshot_id: str
    etag: str


class PatientLedgerCache:
    def __init__(self, *, max_entries: int = 32) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._entries: "OrderedDict[Hashable, _CacheEntry]" = OrderedDict()
        self._locks: "OrderedDict[Hashable, asyncio.Lock]" = OrderedDict()

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: Hashable) -> bool:
        return key in self._entries

    def _lock_for(self, key: Hashable) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def get_records(self, key: Hashable, fetch: FetchFn) -> list[dict[str, Any]]:
        """Return the patient's current chart records for ``key``, single-flighted per key."""
        async with self._lock_for(key):
            cached = self._entries.get(key)
            if_none_match = cached.etag if cached else None
            try:
                result = await fetch(if_none_match)
            except InconsistentSnapshotError:
                result = await fetch(if_none_match)  # one retry, then let it raise

            if result.not_modified:
                if cached is None:
                    raise RuntimeError(
                        "Querystore reported 304 Not Modified for an uncached patient chart"
                    )
                self._entries.move_to_end(key)
                return list(cached.records)

            assert result.records is not None
            assert result.snapshot_id is not None
            assert result.etag is not None
            entry = _CacheEntry(
                records=tuple(result.records),
                snapshot_id=result.snapshot_id,
                etag=result.etag,
            )
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                evicted_key, _ = self._entries.popitem(last=False)
                self._forget_lock_if_idle(evicted_key)
            return list(entry.records)

    def _forget_lock_if_idle(self, key: Hashable) -> None:
        # Bounds `_locks` alongside `_entries`. Skips a lock some other task currently holds for
        # this key (a concurrent fetch racing the eviction) rather than pulling it out from under
        # that task; the dict entry is tiny, and the next release leaves nothing else referencing
        # it, so it is immediately collectible.
        lock = self._locks.get(key)
        if lock is not None and not lock.locked():
            del self._locks[key]


# SourceRegistry.default() (server.context_sources) rebuilds a fresh QueryStoreClient/
# QueryStoreSource on every turn, so the cache itself must outlive any one request; this
# process-level singleton is what makes revalidation (rather than a cold fetch) the common case.
_default_cache: Optional[PatientLedgerCache] = None


def default_patient_ledger_cache() -> PatientLedgerCache:
    global _default_cache
    if _default_cache is None:
        _default_cache = PatientLedgerCache()
    return _default_cache


def reset_default_patient_ledger_cache() -> None:
    """Clear the process-level singleton. Test isolation only; never called at runtime."""
    global _default_cache
    _default_cache = None
