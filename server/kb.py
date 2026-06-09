"""
Clinical knowledge base — a small openly-licensed seed searched via SQLite FTS5
(BM25), exposed as a typed tool the orchestrator may call. Demo-grade precursor
to F009: hand-curated snippets; no rerank / OpenMRS contextualization yet.

The corpus (server/kb_data/corpus.jsonl) is loaded into an in-memory index on
first use. Each result carries provenance (source/version/url/license) so the
synthesizer can label KB-derived claims inline — KB content never enters the
integer citation array (which is chart-record-only).

FTS5 is the primary backend; if the runtime's sqlite lacks the FTS5 module we
fall back to a pure-Python keyword-overlap ranker so the KB still works.
"""

import json
import logging
import os
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_CORPUS_PATH = os.path.join(os.path.dirname(__file__), "kb_data", "corpus.jsonl")
_DEFAULT_K = 3
_TERM = re.compile(r"[A-Za-z0-9]+")

_lock = threading.Lock()
_index: Optional["_Index"] = None


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
    }


class _Index:
    """FTS5-backed index with a keyword-overlap fallback."""

    def __init__(self, path: str = _CORPUS_PATH):
        self.rows = [_fields(r) for r in _load_corpus(path)]
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

    def search(self, query: str, k: int) -> List[Dict[str, Any]]:
        terms = [t.lower() for t in _TERM.findall(query or "")]
        if not terms:
            return []
        if self.conn is not None:
            match = " OR ".join(terms)
            try:
                cur = self.conn.execute(
                    "SELECT id,title,text,source,version,url,license, bm25(kb) AS score "
                    "FROM kb WHERE kb MATCH ? ORDER BY score LIMIT ?",
                    (match, k),
                )
                cols = ["id", "title", "text", "source", "version", "url", "license", "score"]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            except sqlite3.OperationalError as e:
                logger.warning("KB FTS5 search failed for %r: %s", query, e)
                return []
        # keyword-overlap fallback: count distinct query terms present per snippet
        scored = []
        for r in self.rows:
            hay = f"{r['title']} {r['text']}".lower()
            hits = sum(1 for t in set(terms) if t in hay)
            if hits:
                scored.append((hits, r))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [{**r, "score": float(h)} for h, r in scored[:k]]


def _get_index() -> "_Index":
    global _index
    if _index is None:
        with _lock:
            if _index is None:
                _index = _Index()
    return _index


def search(query: str, k: int = _DEFAULT_K) -> List[Dict[str, Any]]:
    """Up to k clinical snippets matching the query, best first. Empty when
    nothing matches — the caller abstains rather than inventing."""
    return _get_index().search(query, k)
