"""
Clinical knowledge base — provenance-bearing clinical reference corpora searched via
SQLite FTS5 (BM25) with optional deterministic clinical-code boosting (`codes=...`)
by the `StaticKnowledgeSource` context adapter.

Supported corpora (`server/kb_data/`):
- `"default"` (`corpus.jsonl`): Stream 1 HIV / primary-care clinical guidance seed.
- `"wahs"` (`wahs_corpus.jsonl`): WAHS+ multi-source reconciliation, CDC ACIP schedule,
  RxNorm duplicate-therapy, KDIGO/ADA staging exclusivity, and SDOH privacy snippets.

Each corpus is loaded into an in-memory index on demand and automatically reloads if
a previously missing corpus file becomes available or changes on disk. Matching rows
become numbered `KnowledgeReference` records in the common evidence ledger.

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


def reset_cache() -> None:
    """Clear in-memory KB indices."""
    global _index
    with _lock:
        _index = None
        _indices_by_corpus.clear()


def _corpus_path_for(corpus: str) -> str:
    if not isinstance(corpus, str):
        raise ValueError(f"Unsupported KB corpus: {corpus!r}")
    norm = (corpus or "default").strip().lower()
    if norm == "default":
        return _CORPUS_PATH
    if norm == "wahs":
        return _WAHS_CORPUS_PATH
    raise ValueError(f"Unsupported KB corpus: {corpus!r}")


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
    }


class _Index:
    """FTS5-backed index with a keyword-overlap fallback and optional code-first boosting."""

    def __init__(self, path: str = _CORPUS_PATH):
        self.path = path
        self.mtime_ns: Optional[int] = None
        if os.path.exists(path):
            try:
                self.mtime_ns = os.stat(path).st_mtime_ns
            except OSError:
                self.mtime_ns = None
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

    def search(
        self,
        query: str,
        k: int,
        *,
        codes: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not isinstance(k, int) or isinstance(k, bool) or k < 0:
            raise ValueError(f"k must be a non-negative integer, got {k!r}")
        if k == 0:
            return []
        terms = [t.lower() for t in _TERM.findall(query or "")]
        ordered_codes = list(
            dict.fromkeys(str(c).strip() for c in (codes or []) if str(c).strip())
        )
        code_priority = {c: idx for idx, c in enumerate(ordered_codes)}
        code_set = set(ordered_codes)
        if not terms and not code_set:
            return []

        fts_limit = max(len(self.rows), k * 3, 10) if code_set else max(k * 3, 10)
        fts_results: List[Dict[str, Any]] = []
        if terms and self.conn is not None:
            match = " OR ".join(terms)
            try:
                cur = self.conn.execute(
                    "SELECT id,title,text,source,version,url,license, bm25(kb) AS score "
                    "FROM kb WHERE kb MATCH ? ORDER BY score LIMIT ?",
                    (match, fts_limit),
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
                for h, r in scored[:fts_limit]
            ]

        if not code_set:
            return fts_results[:k]

        # Hybrid code-first + BM25 re-ranking when clinical codes are supplied
        fts_rank_by_id = {r["id"]: idx for idx, r in enumerate(fts_results)}
        fts_score_by_id = {r["id"]: float(r["score"]) for r in fts_results}
        candidates: List[tuple[int, int, int, int, Dict[str, Any]]] = []
        for idx, r in enumerate(self.rows):
            rid = r["id"]
            row_codes = set(r.get("codes") or [])
            matched_codes = [c for c in ordered_codes if c in row_codes]
            code_hits = len(matched_codes)
            best_code_prio = min((code_priority[c] for c in matched_codes), default=999999)
            in_fts = 1 if rid in fts_rank_by_id else 0
            if code_hits == 0 and not in_fts:
                continue
            fts_pos = fts_rank_by_id.get(rid, 999999)
            if code_hits > 0 and in_fts:
                tier = 1
                primary_rank = fts_pos
                secondary_rank = best_code_prio
            elif in_fts:
                tier = 2
                primary_rank = fts_pos
                secondary_rank = 999999
            else:
                tier = 3
                primary_rank = best_code_prio
                secondary_rank = -code_hits
            candidates.append(
                (
                    tier,
                    primary_rank,
                    secondary_rank,
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
        candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        return [c[4] for c in candidates[:k]]


def _is_index_stale(idx: Optional["_Index"], path: str) -> bool:
    if idx is None or idx.path != path or not idx.rows:
        return True
    if not os.path.exists(path):
        return True
    try:
        current_mtime = os.stat(path).st_mtime_ns
    except OSError:
        return True
    return idx.mtime_ns != current_mtime


def _get_index(corpus: str = "default") -> "_Index":
    global _index
    path = _corpus_path_for(corpus)
    norm = (corpus or "default").strip().lower()
    with _lock:
        if norm == "default":
            if _is_index_stale(_index, path):
                built = _Index(path)
                if built.rows and built.mtime_ns is not None:
                    _index = built
                else:
                    _index = None
                    return built
            return _index  # type: ignore[return-value]
        cached = _indices_by_corpus.get(norm)
        if _is_index_stale(cached, path):
            built = _Index(path)
            if built.rows and built.mtime_ns is not None:
                _indices_by_corpus[norm] = built
            else:
                _indices_by_corpus.pop(norm, None)
                return built
            return built
        return cached  # type: ignore[return-value]


def search(
    query: str,
    k: int = _DEFAULT_K,
    *,
    corpus: str = "default",
    codes: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Return up to `k` clinical snippets from `corpus` (`'default'` or `'wahs'`).

    Matches via FTS5 BM25 over `query` and/or deterministic `codes` overlap (preserving
    `codes` priority order for code-only and tie-broken matches). Returns an empty list
    when neither `query` nor `codes` matches any snippet."""
    if not isinstance(k, int) or isinstance(k, bool) or k < 0:
        raise ValueError(f"k must be a non-negative integer, got {k!r}")
    return _get_index(corpus).search(query, k, codes=codes)
