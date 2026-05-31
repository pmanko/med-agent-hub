# Clinical knowledge-base seed

`corpus.jsonl` is a small, hand-curated seed of reference snippets the Med Agent
Team may consult via the `kb_search` tool (`server/kb.py`). It is **demo-grade**:
enough well-established facts to prove the retrieve-and-ground path, not a
comprehensive corpus.

## Domains (18 snippets)

- **HIV / ART** (8) — preferred first-line (dolutegravir / TLD), the stavudine
  (d4T) phase-out, treat-all, WHO clinical staging, CD4 interpretation, adherence,
  TB co-infection, co-trimoxazole prophylaxis. Matches the HIV demo cohort.
- **General clinical / global health** (6) — WHO IMCI danger signs + fast-breathing
  thresholds, ORS+zinc, metformin/EML, amoxicillin pneumonia, hypertension threshold.
- **Terminology — CIEL / OCL** (2) — what the CIEL concept dictionary is and how
  Open Concept Lab distributes it.
- **OpenMRS data model** (2) — obs / encounter / visit information model and the
  concept dictionary, so the agent can interpret the chart structure it reasons over.

## Provenance

Each snippet carries `source`, `version`, `url`, `license`.

- **WHO clinical snippets** (HIV + general): every `url` was verified against the
  live WHO publication page (title + year) — document-level provenance, **CC BY-NC-SA
  3.0 IGO**. Not page-anchor provenance (we don't yet pin each claim to a page).
- **OpenMRS / CIEL meta snippets**: attributed to source (OpenMRS documentation /
  Open Concept Lab) with **license `confirm terms`** — the exact content license was
  not verified at authoring; confirm before any redistribution. These are technical
  context, not clinical guidance.

## Format

One JSON object per line: `id`, `title`, `text`, `source`, `version`, `url`,
`license`, `tags`. The index is rebuilt in-memory on first search (SQLite FTS5 /
BM25, keyword-overlap fallback) — edit this file and restart to pick up changes.

## Scope / next steps

Rigorous corpus acquisition — page-anchor provenance, verified terminology
licensing, broader sources, and OpenMRS-contextualized (PHI-free aggregate)
content — is feature **F009**. KB facts are attributed inline in prose by the
synthesizer and kept **out of** the integer `citations` array (chart-records-only).
