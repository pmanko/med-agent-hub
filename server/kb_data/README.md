# Clinical knowledge-base seeds & WAHS+ structured reference packs

`server/kb_data/` contains two provenance-bearing JSONL corpora searched via `server.kb`
(`StaticKnowledgeSource`) and three deterministic reference packs loaded by `server.fhir.kb_lookup`:

1. **`corpus.jsonl` (`corpus="default"`)** — Stream 1 OpenMRS / global-health HIV, TB, malaria,
   pediatric IMCI, CIEL/OCL, and OpenMRS data-model seed (24 snippets).
2. **`wahs_corpus.jsonl` (`corpus="wahs"`)** — Stream 6 Washington Health Summary (WAHS+)
   clinical reconciliation, ACIP immunization schedule rules, medication brand↔generic and
   therapeutic-class duplication guidance, KDIGO/ADA stage progression, LOINC lab trajectory
   interpretation, and SDOH privacy segmentation reference (14 snippets), indexed with
   clinical `codes` (SNOMED CT, RxNorm, CVX, LOINC) for hybrid code-first + FTS5 BM25 retrieval.
3. **`acip_rules.json` (WAHS Layer 1)** — CDC ACIP CVX vaccine group mappings, duplicate window
   thresholds, and series rules (including the HPV under-15 2-dose rule and 5-month minimum interval exception).
4. **`rxnorm_lookup.json` (WAHS Layer 2)** — NLM RxNorm active ingredient, brand↔generic alias,
   and therapeutic class lookup table.
5. **`clinical_exclusivity_and_privacy.json` (WAHS Layer 3)** — SNOMED CT condition stage
   exclusivity (`ckd_stage`, `glycemic_progression`), SDOH sensitivity classifications
   (`SDOH/PRIVACY-SENSITIVE`, `SDOH-SOCIAL`), and LOINC lab/vital reference ranges with specimen-preserving aliases.

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
