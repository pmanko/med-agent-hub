# Clinical knowledge-base seed

`corpus.jsonl` is a small, hand-curated seed of openly-licensed clinical
reference snippets that the Med Agent Team may consult via the `kb_search` tool
(`server/kb.py`). It is **demo-grade**: a handful of well-established facts to
prove the retrieve-and-ground path end-to-end, not a comprehensive guideline
corpus.

## Provenance

Each snippet carries `source`, `version`, `url`, and `license`. Every `url` was
verified against the live WHO publication page (title + year confirmed) on
2026-05-30. This is **document-level** provenance — the cited document is the
right source for the snippet's content — not page-anchor provenance: we do not
yet pin each claim to a specific page/figure within the document. Two claims
(zinc dosing; amoxicillin replacing cotrimoxazole) were additionally confirmed
against the source text during verification.

All current sources are WHO publications under **CC BY-NC-SA 3.0 IGO**.

## Format

One JSON object per line:

| field     | meaning                                                        |
|-----------|----------------------------------------------------------------|
| `id`      | stable slug (used in tests and logs)                           |
| `title`   | short human label                                              |
| `text`    | the snippet (atomic, a few sentences)                          |
| `source`  | publication name                                               |
| `version` | edition / year                                                 |
| `url`     | canonical publication URL (verified)                           |
| `license` | content license                                                |
| `tags`    | coarse topic tags                                              |

The index is rebuilt in-memory on first search (SQLite FTS5 / BM25, with a
keyword-overlap fallback) — edit this file and restart to pick up changes.

## Scope / next steps

Rigorous corpus acquisition — page-anchor provenance, broader sources
(EML children's list, RxNorm, MSF/IMCI dosing tables), and OpenMRS-contextualized
(PHI-free aggregate) content — is feature **F009**. KB facts are attributed
inline in prose by the synthesizer and are deliberately kept **out of** the
integer `citations` array, which is reserved for the patient's chart records.
