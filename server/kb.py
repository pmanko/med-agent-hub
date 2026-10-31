"""
Clinical knowledge base — a small openly-licensed seed searched via SQLite FTS5
(BM25) by the optional StaticKnowledgeSource context adapter. Demo-grade precursor
to F009: hand-curated snippets; no rerank / OpenMRS contextualization yet.

The corpus (server/kb_data/corpus.jsonl) is loaded into an in-memory index on
first use. Each result carries provenance (source/version/url/license); matching
rows become numbered KnowledgeReference records in the common evidence ledger.

FTS5 is the primary backend; if the runtime's sqlite lacks the FTS5 module we
fall back to a pure-Python keyword-overlap ranker so the KB still works.
"""

import json
import logging
import os
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

_CORPUS_PATH = os.path.join(os.path.dirname(__file__), "kb_data", "corpus.jsonl")
_WAHS_CORPUS_PATH = os.path.join(os.path.dirname(__file__), "kb_data", "wahs_corpus.jsonl")
_DEFAULT_K = 3
_TERM = re.compile(r"[A-Za-z0-9]+")

_lock = threading.Lock()
_index: Optional["_Index"] = None
_indices_by_corpus: Dict[str, "_Index"] = {}


def _load_corpus(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        logger.warning("KB corpus not found at %s; KB is empty", path)
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                logger.warning("Skipping malformed KB line: %s", e)
    return rows


def _fields(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": row.get("id", ""),
        "title": row.get("title", ""),
        "text": row.get("text", ""),
        "source": row.get("source", ""),
        "version": row.get("version", ""),
        "url": row.get("url", ""),
        "license": row.get("license", ""),
        "codes": [str(c) for c in (row.get("codes") or []) if c],
        "tags": [str(t) for t in (row.get("tags") or []) if t],
    }


class _Index:
    """FTS5-backed index with a keyword-overlap fallback and optional code-first boosting."""

    def __init__(self, path: str = _CORPUS_PATH):
        self.rows = [_fields(r) for r in _load_corpus(path)]
        self.rows_by_id: Dict[str, Dict[str, Any]] = {r["id"]: r for r in self.rows if r.get("id")}
        self.conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(":memory:", check_same_thread=False)
            conn.execute(
                "CREATE VIRTUAL TABLE kb USING fts5("
                "id UNINDEXED, title, text, source UNINDEXED, version UNINDEXED, "
                "url UNINDEXED, license UNINDEXED, tokenize='porter')"
            )
            for r in self.rows:
                conn.execute(
                    "INSERT INTO kb (id,title,text,source,version,url,license) VALUES (?,?,?,?,?,?,?)",
                    (r["id"], r["title"], r["text"], r["source"], r["version"], r["url"], r["license"]),
                )
            conn.commit()
            self.conn = conn
            self.backend = "fts5"
        except sqlite3.OperationalError as e:
            logger.warning("sqlite FTS5 unavailable (%s); using keyword fallback", e)
            self.backend = "keyword"
        logger.info("KB index built: %d snippets (%s backend)", len(self.rows), self.backend)

    def search(
        self,
        query: str,
        k: int,
        *,
        codes: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        terms = [t.lower() for t in _TERM.findall(query or "")]
        code_set = {str(c).strip() for c in (codes or []) if str(c).strip()}
        if not terms and not code_set:
            return []

        fts_results: List[Dict[str, Any]] = []
        if terms and self.conn is not None:
            match = " OR ".join(terms)
            try:
                cur = self.conn.execute(
                    "SELECT id,title,text,source,version,url,license, bm25(kb) AS score "
                    "FROM kb WHERE kb MATCH ? ORDER BY score LIMIT ?",
                    (match, max(k * 3, 10)),
                )
                cols = ["id", "title", "text", "source", "version", "url", "license", "score"]
                fts_results = [dict(zip(cols, row)) for row in cur.fetchall()]
            except sqlite3.OperationalError as e:
                logger.warning("KB FTS5 search failed for %r: %s", query, e)
                fts_results = []
        elif terms:
            # keyword-overlap fallback: count distinct query terms present per snippet
            scored = []
            for r in self.rows:
                hay = f"{r['title']} {r['text']}".lower()
                hits = sum(1 for t in set(terms) if t in hay)
                if hits:
                    scored.append((hits, r))
            scored.sort(key=lambda x: x[0], reverse=True)
            fts_results = [
                {
                    "id": r["id"],
                    "title": r["title"],
                    "text": r["text"],
                    "source": r["source"],
                    "version": r["version"],
                    "url": r["url"],
                    "license": r["license"],
                    "score": float(h),
                }
                for h, r in scored[: max(k * 3, 10)]
            ]

        if not code_set:
            return fts_results[:k]

        # Hybrid code-first + BM25 re-ranking when clinical codes are supplied
        fts_rank_by_id = {r["id"]: idx for idx, r in enumerate(fts_results)}
        fts_score_by_id = {r["id"]: float(r["score"]) for r in fts_results}
        candidates: List[tuple[int, int, int, Dict[str, Any]]] = []
        for idx, r in enumerate(self.rows):
            rid = r["id"]
            row_codes = set(r.get("codes") or [])
            code_hits = len(row_codes & code_set)
            in_fts = 1 if rid in fts_rank_by_id else 0
            if code_hits == 0 and not in_fts:
                continue
            # Sort by: (1) both code & FTS match, (2) FTS rank or code hit count, (3) corpus order
            fts_pos = fts_rank_by_id.get(rid, 999)
            candidates.append(
                (
                    1 if (code_hits > 0 and in_fts) else (2 if in_fts else 3),
                    fts_pos if in_fts else -code_hits,
                    idx,
                    {
                        "id": r["id"],
                        "title": r["title"],
                        "text": r["text"],
                        "source": r["source"],
                        "version": r["version"],
                        "url": r["url"],
                        "license": r["license"],
                        "score": fts_score_by_id.get(rid, -float(code_hits)),
                    },
                )
            )
        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        return [c[3] for c in candidates[:k]]


def _get_index(corpus: str = "default") -> "_Index":
    global _index
    norm = (corpus or "default").strip().lower()
    if norm == "default":
        if _index is None:
            with _lock:
                if _index is None:
                    _index = _Index(_CORPUS_PATH)
        return _index
    with _lock:
        if norm not in _indices_by_corpus:
            path = _WAHS_CORPUS_PATH if norm == "wahs" else _CORPUS_PATH
            _indices_by_corpus[norm] = _Index(path)
        return _indices_by_corpus[norm]


def search(
    query: str,
    k: int = _DEFAULT_K,
    *,
    corpus: str = "default",
    codes: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Up to k clinical snippets matching the query, best first. Empty when
    nothing matches — the caller abstains rather than inventing."""
    return _get_index(corpus).search(query, k, codes=codes)
