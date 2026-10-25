"""Drug-reference injection keeps each record's QueryStore selection, so the selector still uses it."""

from server import engine
from server.context_sources import EvidenceLedger, EvidenceRecord


def test_records_keep_their_slice_tier_and_rank_after_drug_reference_injection():
    weight = EvidenceRecord(
        stable_id="querystore:obs:w1",
        source="querystore",
        source_priority=10,
        resource_type="obs",
        resource_uuid="w1",
        date="2026-01-26",
        text="(2026-01-26) Weight (kg): 71 kg",
        querystore_rank=3,
        slice_tier="similarity",
    )
    ledger = EvidenceLedger((weight,), original_text="[1] (2026-01-26) Weight (kg): 71 kg")
    mappings = [
        {**weight.mapping(1), "text": weight.text},
        {"index": 2, "sourceId": "drug-reference:nvp", "resourceType": "DrugReference", "text": "Nevirapine"},
    ]

    after = engine._ledger_after_drug_injection(ledger, "chart", mappings)

    kept = after.records[0]
    assert (kept.slice_tier, kept.querystore_rank) == ("similarity", 3)
    assert after.records[1].source == "drug-safety"
