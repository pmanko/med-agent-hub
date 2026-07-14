# med-agent-hub

med-agent-hub is the client-facing clinical answer service used by ChartSearchAI, the validation harness, and direct OpenAI-compatible clients. A request selects a validated profile or an explicit low-level leg. Profiles compose one shared stage engine; clients do not orchestrate the stages themselves.

## Architecture

```text
client
  -> POST /v1/chat/completions (profile id, messages, optional patient)
med-agent-hub
  -> context source(s) and complete evidence ledger
  -> optional deterministic context selection for oversized inputs
  -> optional team gather
  -> answer -> deterministic substance/temporal gate
  -> immediate reference resolution and answer_done
  -> optional review -> re-gate -> final reference resolution
  -> final Answer citation grounding
  -> In-Depth -> deterministic temporal and citation grounding gates -> done
OpenAI-compatible model router at LLM_BASE_URL
```

Streaming and blocking requests execute the same asynchronous engine in `server/engine.py`. Blocking requests drain the engine's events into the response envelope. Product profiles stream `answer_done`, optional `answer_validation`, `indepth_pending`, `indepth_done` or `indepth_error`, and `done`.

Product terminal events carry the complete assistant envelope. In-Depth state is emitted only in the nested `inDepth` object so its `answer` cannot overwrite the direct clinical Answer. Java and ESM clients reject the retired flattened In-Depth shape.

## Profiles

Configured profiles live in `server/levels.yaml` and declare a human label, topology, ordered stages, role models, prompts, validation policies, and context budget. The preferred product profile is `single-e4b-checked`; discovery marks it as the effective default only when it is available, otherwise the hub marks the available product profile with the lowest explicit `selection_priority`. Product envelopes always enforce deterministic temporal validation, regardless of discovery visibility, require exact tokenizer-backed context counting, and apply the hub-owned `chart_answer` JSON schema. A product request cannot replace that contract; low-level legs retain their existing caller-controlled `response_format` behavior.

Low-level experiment legs use these ids:

- `answer:<model>@<prompt>~<gate>~temp<n>`
- `answer-review:<model>@<prompt>`
- `indepth-only:<model>@<prompt>`

Low-level legs are callable but are not advertised by `GET /v1/models`. Unknown ids return a structured `model_not_found` response; they are never forwarded to the model backend.

## Context Sources

`server/context_sources.py` defines provider-neutral source, evidence-ledger, selector, and token-counter contracts. Available adapters are:

- inline numbered chart context;
- optional Querystore patient records;
- optional static clinical knowledge-base results.

Querystore is not a startup dependency. Inline requests work without it, and alternate sources can implement the same `ContextSource` contract. A caller can request a source list with `context.sources`; otherwise the hub selects one patient source or the inline chart. Team gather profiles add static knowledge as a supplemental source in the same evidence ledger, so KB facts retain stable provenance and citation mappings instead of traveling through a parallel context path.

Small charts retain their original chart text. Oversized charts use stable mandatory/exact-match/overlap/recency ordering, preserve canonical citation indices, include whole records only, and disclose every included or excluded source id and reason in trace metadata. Answer, In-Depth synthesis, In-Depth review, and the bounded retry each fit a stage-local prompt view with the exact model tokenizer; all views derive from the same complete ledger. Temporal facts and deterministic checks always use that complete ledger. Mandatory-context overflow returns structured `insufficient_context` metadata rather than silently truncating evidence.

## Validation and Evidence

- Every product Answer receives deterministic substance, date, temporal, date-value, and trend checks before `answer_done`.
- Reviewer edits are checked again before they can ship.
- Every product In-Depth claim receives deterministic temporal, citation-resolution, and citation-grounding results before display.
- Citation grounding checks every claim group in stable, context-bounded sequential batches; a failed batch cannot erase verdicts from successful batches.
- References resolve against the complete current evidence ledger and carry source id, resource metadata, source text, usage locations, resolution state, and final grounding state.
- Citation count is metadata, not a confidence score.

Trace packages are appended to `$TEAM_TRACE_DIR/trace.jsonl` (default `/app/trace`) and include the final answer, original draft when applicable, context selection, temporal facts summary, Answer and In-Depth gate results, final references, model roles, sampling settings, and ordered stage steps.

### Drug-safety data

The deterministic drug-safety layer accepts either the bundled curated JSON source or an operator-provided WHO-ATC export through `DRUG_SAFETY_SOURCE_FORMAT` and `DRUG_SAFETY_DATASET_PATH`. Curated cross-reactivity groups load independently through `DRUG_SAFETY_CROSS_REACTIVITY_PATH`, so cross-branch rules work with either entry source. The bundled seed group covers the NSAID branches `M01AE` and `N02BA`; deployments remain responsible for reviewing and extending this clinical data.

Weight-aware dose checks read the newest fresh numeric Querystore `obs` matching `DRUG_SAFETY_WEIGHT_CONCEPT_UUID` (CIEL weight `5089...` by default). `DRUG_SAFETY_WEIGHT_MAX_AGE_DAYS` defaults to 90. Set the concept value to `none` to disable only the weight-aware arm. Missing, stale, malformed, or unavailable optional safety data degrades to no additional warning and never interrupts an answer.

## Endpoints

- `POST /v1/chat/completions`: blocking or staged streaming profile execution.
- `GET /v1/models`: configured profile metadata, availability, validation capability, exact context requirements, and at most one hub-selected available default marker.
- `GET /health`: service health, uptime, and process memory.
- `GET /`: concise service status.

## Local Development

The hub is the application entrypoint; `LLM_BASE_URL` identifies its OpenAI-compatible model-serving backend, normally the local llama.cpp router.

```bash
poetry install --with dev
cp env.recommended .env
poetry run uvicorn server.main:app --host 0.0.0.0 --port 8080 --reload --env-file .env
```

Inline smoke request:

```bash
curl -fsS http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "answer:gemma-e4b@synthesis-chartsearchai~enforce~temp0",
    "messages": [
      {"role": "system", "content": "You are a clinical assistant."},
      {"role": "user", "content": "[1] (2026-01-01) Medication: lisinopril 10 mg"},
      {"role": "user", "content": "What medication is documented?"}
    ]
  }'
```

For a product profile, supply `patient` with a configured patient source or provide an inline chart. See the parent harness `make chartsearchai-local` workflow for the integrated OpenMRS setup.

## Runtime Layout

```text
server/
  main.py              FastAPI application
  openai_compat.py     OpenAI-compatible API and profile discovery
  engine.py            single stream-and-drain stage engine
  levels.yaml          configured profiles
  levels_loader.py     profile compiler and dynamic leg parser
  context_sources.py   sources, ledger, exact budgets, deterministic selector
  temporal.py          temporal facts and deterministic gates
  team.py              reusable answer/review/gather/grounding stage helpers
  drug_safety.py       deterministic dosing, interaction, and contraindication checks
  kb.py                provenance-bearing static clinical knowledge search
  prompts/             file-backed stage prompts
```

The retired A2A/MCP agent servers and their runtime dependencies are not part of this service.
