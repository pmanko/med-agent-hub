from __future__ import annotations

import asyncio

import pytest

from server.ranked_candidates import resolve_ranked_hits_with_retry


def _record(resource_uuid, last_modified="2026-01-01T00:00:00Z", **overrides):
    record = {
        "resourceType": "Observation",
        "resourceUuid": resource_uuid,
        "lastModified": last_modified,
        "text": f"record {resource_uuid}",
    }
    record.update(overrides)
    return record


def _hit(resource_uuid, last_modified="2026-01-01T00:00:00Z"):
    return {
        "resourceType": "Observation",
        "resourceUuid": resource_uuid,
        "lastModified": last_modified,
    }


async def _no_refetch():
    raise AssertionError("refetch_ledger should not be called when every hit resolves")


def test_hits_that_match_the_ledger_by_identity_and_last_modified_resolve():
    ledger = [_record("one"), _record("two")]
    hits = [_hit("two"), _hit("one")]

    resolved = asyncio.run(resolve_ranked_hits_with_retry(hits, ledger, _no_refetch))

    # Original rank order is preserved (hit order), and the LEDGER's own record is returned.
    assert [record["resourceUuid"] for record in resolved] == ["two", "one"]
    assert resolved[0] is ledger[1]
    assert resolved[1] is ledger[0]


def test_a_hit_missing_from_the_ledger_is_dropped_after_a_refetch_that_still_lacks_it():
    ledger = [_record("one")]
    hits = [_hit("one"), _hit("ghost")]
    refetch_calls = []

    async def refetch():
        refetch_calls.append(1)
        return ledger  # unchanged: "ghost" genuinely isn't in the chart (e.g. voided)

    resolved = asyncio.run(resolve_ranked_hits_with_retry(hits, ledger, refetch))

    assert [record["resourceUuid"] for record in resolved] == ["one"]
    assert len(refetch_calls) == 1


def test_a_stale_last_modified_resolves_after_one_refetch_catches_up():
    ledger = [_record("one", last_modified="2026-01-01T00:00:00Z")]
    hits = [_hit("one", last_modified="2026-01-02T00:00:00Z")]  # the ranked index saw a newer write

    async def refetch():
        return [_record("one", last_modified="2026-01-02T00:00:00Z")]

    resolved = asyncio.run(resolve_ranked_hits_with_retry(hits, ledger, refetch))

    assert [record["resourceUuid"] for record in resolved] == ["one"]
    assert resolved[0]["lastModified"] == "2026-01-02T00:00:00Z"


def test_refetch_is_never_called_when_every_hit_resolves_on_the_first_pass():
    ledger = [_record("one")]
    hits = [_hit("one")]

    # _no_refetch raises if called; a passing test proves it never was.
    resolved = asyncio.run(resolve_ranked_hits_with_retry(hits, ledger, _no_refetch))

    assert [record["resourceUuid"] for record in resolved] == ["one"]


def test_refetch_happens_at_most_once_even_with_multiple_unresolved_hits():
    ledger = []
    hits = [_hit("ghost-a"), _hit("ghost-b")]
    refetch_calls = []

    async def refetch():
        refetch_calls.append(1)
        return []

    resolved = asyncio.run(resolve_ranked_hits_with_retry(hits, ledger, refetch))

    assert resolved == []
    assert len(refetch_calls) == 1


def test_a_refetch_failure_degrades_to_dropping_the_unresolved_hits_rather_than_raising():
    ledger = [_record("one")]
    hits = [_hit("one"), _hit("ghost")]

    async def failing_refetch():
        raise RuntimeError("querystore unreachable")

    resolved = asyncio.run(resolve_ranked_hits_with_retry(hits, ledger, failing_refetch))

    # "one" already resolved on the first pass and is unaffected by the refetch failure; "ghost"
    # is dropped rather than the whole call raising — ranked candidates are supplementary, never
    # fatal to the turn.
    assert [record["resourceUuid"] for record in resolved] == ["one"]


def test_empty_hits_resolve_to_an_empty_list_without_calling_refetch():
    resolved = asyncio.run(resolve_ranked_hits_with_retry([], [_record("one")], _no_refetch))

    assert resolved == []
