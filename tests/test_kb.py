"""
Knowledge-base tests (server/kb.py): BM25/keyword search over the openly-licensed
clinical seed. Exercises the REAL index built from server/kb_data/corpus.jsonl —
no mocks; the corpus is the fixture.
"""

from server import kb


def test_search_retrieves_the_relevant_snippet():
    hits = kb.search("metformin first-line diabetes")
    assert hits, "expected a hit for a corpus topic"
    assert hits[0]["id"] == "metformin-first-line-t2dm"
    assert "metformin" in hits[0]["text"].lower()


def test_each_hit_carries_provenance():
    hits = kb.search("childhood pneumonia fast breathing")
    assert hits
    top = hits[0]
    # Provenance is what lets the synthesizer attribute KB facts inline.
    for field in ("source", "version", "url", "license"):
        assert top.get(field), f"missing provenance field {field!r}"


def test_search_abstains_on_no_match():
    # A query with no corpus overlap returns empty — the caller must not invent.
    assert kb.search("xylophone quasार obscurenonsenseword") == []


def test_search_abstains_on_empty_query():
    assert kb.search("") == []
    assert kb.search("   ") == []


def test_search_respects_k_limit():
    # Broad query that overlaps several snippets; k caps the result count.
    hits = kb.search("children pneumonia diarrhoea diabetes hypertension", k=2)
    assert len(hits) <= 2


def test_results_are_ranked_best_first():
    # "hypertension blood pressure" should rank the HTN snippet above incidental
    # overlaps from other snippets.
    hits = kb.search("hypertension blood pressure threshold")
    assert hits
    assert hits[0]["id"] == "htn-diagnosis-threshold"
