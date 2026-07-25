from __future__ import annotations

import asyncio

import pytest

from server.patient_ledger_cache import PatientLedgerCache
from server.querystore_client import InconsistentSnapshotError, PatientLedgerFetch


def _modified(records, snapshot_id="snap-1", etag='"etag-1"'):
    return PatientLedgerFetch(
        not_modified=False, records=records, snapshot_id=snapshot_id, etag=etag
    )


def _not_modified():
    return PatientLedgerFetch(not_modified=True)


def test_a_cold_key_fetches_with_no_if_none_match():
    cache = PatientLedgerCache()
    calls = []

    async def fetch(if_none_match):
        calls.append(if_none_match)
        return _modified([{"resourceUuid": "one"}])

    records = asyncio.run(cache.get_records("patient-1", fetch))

    assert records == [{"resourceUuid": "one"}]
    assert calls == [None]


def test_every_call_revalidates_even_when_unchanged():
    cache = PatientLedgerCache()
    calls = []

    async def fetch(if_none_match):
        calls.append(if_none_match)
        return _not_modified() if calls[:-1] else _modified([{"resourceUuid": "one"}])

    first = asyncio.run(cache.get_records("patient-1", fetch))
    second = asyncio.run(cache.get_records("patient-1", fetch))

    assert first == second == [{"resourceUuid": "one"}]
    # The second call must have revalidated using the etag captured on the first, not skipped
    # the network call outright.
    assert calls == [None, '"etag-1"']


def test_a_changed_chart_replaces_the_cached_records():
    cache = PatientLedgerCache()
    responses = [
        _modified([{"resourceUuid": "one"}], snapshot_id="snap-1", etag='"etag-1"'),
        _modified([{"resourceUuid": "one"}, {"resourceUuid": "two"}], snapshot_id="snap-2", etag='"etag-2"'),
    ]

    async def fetch(_if_none_match):
        return responses.pop(0)

    first = asyncio.run(cache.get_records("patient-1", fetch))
    second = asyncio.run(cache.get_records("patient-1", fetch))

    assert first == [{"resourceUuid": "one"}]
    assert second == [{"resourceUuid": "one"}, {"resourceUuid": "two"}]


def test_distinct_keys_do_not_share_a_cache_slot():
    cache = PatientLedgerCache()

    async def fetch_a(_if_none_match):
        return _modified([{"resourceUuid": "a"}])

    async def fetch_b(_if_none_match):
        return _modified([{"resourceUuid": "b"}])

    records_a = asyncio.run(cache.get_records("patient-a", fetch_a))
    records_b = asyncio.run(cache.get_records("patient-b", fetch_b))

    assert records_a == [{"resourceUuid": "a"}]
    assert records_b == [{"resourceUuid": "b"}]


def test_not_modified_on_a_cold_key_is_a_contract_violation():
    cache = PatientLedgerCache()

    async def fetch(_if_none_match):
        return _not_modified()

    with pytest.raises(RuntimeError, match="304|uncached"):
        asyncio.run(cache.get_records("patient-1", fetch))


def test_an_inconsistent_snapshot_is_retried_once_then_succeeds():
    cache = PatientLedgerCache()
    attempts = []

    async def fetch(if_none_match):
        attempts.append(if_none_match)
        if len(attempts) == 1:
            raise InconsistentSnapshotError("chart mutated mid-pagination")
        return _modified([{"resourceUuid": "one"}])

    records = asyncio.run(cache.get_records("patient-1", fetch))

    assert records == [{"resourceUuid": "one"}]
    assert len(attempts) == 2


def test_an_inconsistent_snapshot_that_persists_fails_explicitly():
    cache = PatientLedgerCache()
    attempts = []

    async def fetch(if_none_match):
        attempts.append(if_none_match)
        raise InconsistentSnapshotError("chart mutated mid-pagination")

    with pytest.raises(InconsistentSnapshotError):
        asyncio.run(cache.get_records("patient-1", fetch))

    assert len(attempts) == 2


def test_a_fetch_failure_propagates_without_serving_a_stale_ledger():
    cache = PatientLedgerCache()

    async def good_fetch(_if_none_match):
        return _modified([{"resourceUuid": "one"}])

    asyncio.run(cache.get_records("patient-1", good_fetch))

    async def bad_fetch(_if_none_match):
        raise RuntimeError("backend unavailable")

    with pytest.raises(RuntimeError, match="backend unavailable"):
        asyncio.run(cache.get_records("patient-1", bad_fetch))

    # A subsequent healthy call still works; the failure did not corrupt the cache entry.
    records = asyncio.run(cache.get_records("patient-1", good_fetch))
    assert records == [{"resourceUuid": "one"}]


def test_bounded_size_evicts_the_least_recently_used_key():
    cache = PatientLedgerCache(max_entries=2)

    async def fetch_one(_if_none_match):
        return _modified([{"resourceUuid": "one"}])

    async def fetch_two(_if_none_match):
        return _modified([{"resourceUuid": "two"}])

    async def fetch_three(_if_none_match):
        return _modified([{"resourceUuid": "three"}])

    asyncio.run(cache.get_records("patient-1", fetch_one))
    asyncio.run(cache.get_records("patient-2", fetch_two))
    asyncio.run(cache.get_records("patient-3", fetch_three))

    assert len(cache) == 2
    assert "patient-1" not in cache
    assert "patient-2" in cache
    assert "patient-3" in cache


def test_max_entries_must_be_positive():
    with pytest.raises(ValueError):
        PatientLedgerCache(max_entries=0)


def test_concurrent_calls_for_the_same_key_are_single_flighted():
    cache = PatientLedgerCache()
    in_flight = 0
    max_concurrent = 0

    async def fetch(_if_none_match):
        nonlocal in_flight, max_concurrent
        in_flight += 1
        max_concurrent = max(max_concurrent, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return _modified([{"resourceUuid": "one"}])

    async def run_both():
        await asyncio.gather(
            cache.get_records("patient-1", fetch),
            cache.get_records("patient-1", fetch),
        )

    asyncio.run(run_both())

    assert max_concurrent == 1
