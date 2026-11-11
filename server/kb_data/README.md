# Clinical knowledge-base seeds & WAHS+ structured reference packs

`server/kb_data/` contains the Stream 1 provenance-bearing JSONL corpus searched via `server.kb`
(`StaticKnowledgeSource`), while Stream 6 WAHS+ structured reference packs and `wahs_corpus.jsonl`
live canonically in `wahs_core.kb_data` (`pmanko/wahs-engine`) to guarantee zero drift between
production `wahs-engine` and `med-agent-hub` evaluation sweeps:

1. **`corpus.jsonl` (`corpus="default"`, local in `server/kb_data/`)** — Stream 1 OpenMRS / global-health HIV, TB, malaria,
   pediatric IMCI, CIEL/OCL, and OpenMRS data-model seed (24 snippets).
2. **`wahs_core/kb_data/wahs_corpus.jsonl` (`corpus="wahs"`, imported from `wahs_core`)** — Stream 6 Washington Health Summary (WAHS+)
   clinical reconciliation, ACIP immunization schedule rules, medication brand↔generic and
   therapeutic-class duplication guidance, KDIGO/ADA stage progression, LOINC lab trajectory
   interpretation, and SDOH privacy segmentation reference, indexed with
   clinical `codes` (SNOMED CT, RxNorm, CVX, LOINC) for hybrid code-first + FTS5 BM25 retrieval.
3. **`wahs_core/kb_data/{acip_rules,rxnorm_lookup,clinical_exclusivity_and_privacy}.json` (imported via `wahs_core.kb_lookup`)** —
   CDC ACIP CVX rules, NLM RxNorm ingredient/class mappings, SNOMED CT condition stage exclusivity,
   SDOH privacy segmentation, and LOINC lab/vital reference ranges.

Matching JSONL rows enter the common evidence ledger as numbered `KnowledgeReference` records.

## Stream 1 Default Corpus Domains (`corpus.jsonl`, 24 snippets)

The clinical coverage is curated to the **demo-data profile** (see
`specs/artifacts/canvases/demo-data-profile.canvas.tsx` + `specs/artifacts/planning/kb-scope.md`):
the demo cohort is a global-health HIV/TB/pediatric dataset, so the seed covers HIV/ART, TB,
malaria, the EPI series, and OIs alongside the general WHO staples.

- **HIV / ART** (10) — preferred first-line (dolutegravir / TLD), the stavudine (d4T) phase-out,
  treat-all, WHO clinical staging, CD4 interpretation, adherence, TB co-infection, co-trimoxazole,
  **cryptococcal disease**, **second-line ART**.
- **TB & malaria** (3) — drug-susceptible TB regimen (2HRZE/4HR), uncomplicated + severe malaria (ACT /
  parenteral artesunate). *(Malaria is the demo's 2nd-most-common condition.)*
- **Pediatric / global health** (7) — WHO IMCI danger signs + fast-breathing thresholds, ORS+zinc,
  amoxicillin pneumonia, the **EPI infant immunization schedule**, metformin/EML, hypertension threshold.
- **Terminology — CIEL / OCL** (2) — what the CIEL concept dictionary is and how Open Concept Lab distributes it.
- **OpenMRS data model** (2) — obs / encounter / visit information model and the concept dictionary,
  so the agent can interpret the chart structure it reasons over.

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
content — is feature **F009**. Displayed KB facts cite their numbered
`KnowledgeReference` records and retain the source metadata in the evidence layer.
