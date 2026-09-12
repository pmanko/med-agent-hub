# med-agent-hub

med-agent-hub is the shared profile and model-execution service used by
ChartSearchAI, Catalyst, the validation harness, and direct OpenAI-compatible
clients. All configured workflows use the same `Profile` schema. Hosted
clinical profiles compose the Hub's clinical stage engine; caller-orchestrated
profiles configure named roles that an application such as Catalyst invokes
while retaining its own domain workflow.

The hosted clinical workflow remains the client-facing clinical answer service;
the caller-orchestrated workflow does not move Catalyst's SQL business logic
into the Hub.

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

Product terminal events carry the complete assistant envelope. In-Depth state is emitted only in the nested `inDepth` object so its `answer` cannot overwrite the direct clinical Answer. When review or deterministic gates remove model-generated claims, `inDepth.reviewDraft` renders the pre-check claim set for review and `inDepth.reviewReferences` resolves only that draft's citations. These review fields are explicitly not the shipped answer or final evidence set. Java and ESM clients reject the retired flattened In-Depth shape.

When Answer checks change a model draft, `answerValidation.originalAnswer` preserves the earliest pre-check Answer and `answerValidation.originalReferences` resolves only that draft's citations. The final `references` field remains authoritative for the checked Answer and accepted In-Depth claims.

## Profiles

Configured profiles live in `server/levels.yaml` and declare a workflow,
human label, topology, ordered stages, role models, prompts, knobs, and policies.
`workflow: clinical_answer` profiles use the hosted clinical stage engine. The
`workflow: catalyst_query` profile uses `topology: caller`: Hub owns its models,
prompts, and knobs while Catalyst owns catalog context, SQL policy/lint,
writer/reviewer orchestration, execution, and lineage. The preferred clinical
product profile is `single-e4b-checked`; clinical discovery marks it as the
effective default only when available. Product envelopes always enforce
deterministic temporal validation, regardless of discovery visibility, require
exact tokenizer-backed context counting, and apply the hub-owned `chart_answer`
JSON schema. A product request cannot replace that contract; low-level legs
retain their existing caller-controlled `response_format` behavior.

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

Small charts retain their original chart text. For larger charts, the model input limit is a safety ceiling, not a target to fill. The deterministic selector admits mandatory safety records, exact matches, a 32-record clinical core that prioritizes active conditions and then recency, and records with meaningful normalized query overlap. It stops when eligible evidence is exhausted, even when capacity remains; other records are traced as `zero_relevance`. When eligible evidence exceeds the ceiling, one batched model-tokenizer request computes the rendered record costs. The selector ranks once, can skip an individually oversized record, and performs only bounded exact checks of assembled candidates. The final admitted prompt count is reused by dispatch, so a normal stage does not tokenize the same prompt twice. Router capability detection and the older `/apply-template` plus `/tokenize` fallback are cached for the process, while exact prompt counts stay in a small request-scoped cache. The selected view preserves canonical citation indices, includes whole records only, and discloses every included or excluded source id and reason in trace metadata. Answer, In-Depth synthesis, In-Depth review, and the bounded retry each fit a stage-local prompt view with the exact model tokenizer; all views derive from the same complete ledger. Temporal facts and deterministic checks always use that complete ledger. Mandatory-context overflow returns structured `insufficient_context` metadata rather than silently truncating evidence.

## Validation and Evidence

- Every product Answer receives deterministic substance, date, temporal, date-value, and trend checks before `answer_done`.
- Reviewer edits are checked again before they can ship.
- Every product In-Depth claim receives deterministic temporal, citation-resolution, and citation-grounding results before display.
- A rejected or edited In-Depth draft remains available as a separately labeled review artifact; it never enters final answer scoring or final evidence.
- Citation grounding checks every claim group in stable, context-bounded sequential batches; a failed batch cannot erase verdicts from successful batches.
- References resolve against the complete current evidence ledger and carry source id, resource metadata, source text, usage locations, resolution state, and final grounding state.
- Citation count is metadata, not a confidence score.

Trace packages are appended to `$TEAM_TRACE_DIR/trace.jsonl` (default `/app/trace`) and include request correlation metadata, the final answer, original draft when applicable, context selection, temporal facts summary, Answer and In-Depth gate results, final references, model roles, sampling settings, and ordered stage steps.

### Drug-safety data

The deterministic drug-safety layer accepts either a package-shaped JSON source or an operator-provided ATC classification export through `DRUG_SAFETY_SOURCE_FORMAT` and `DRUG_SAFETY_DATASET_PATH`. A source package declares its identity, version, provenance, and review state. Only `clinically_approved` packages can emit deterministic product warnings; `proposed`, `evidence_curated`, or `retired` data is reported honestly and cannot be presented as reviewed clinical decision support. The bundled JSON and cross-reactivity data are unreviewed research seeds. ATC supplies classification, not interaction, contraindication, duplicate-therapy, or cross-reactivity rules.

Every product response carries a canonical `drug_safety.v1` result with `checked`, `limited`, or `unavailable` status; package metadata; medication-mapping and patient-exposure coverage; identity confidence; issues; and any approved findings. The legacy `safetyStatus` and `safetyWarnings` fields remain for client compatibility. A missing source, incomplete patient exposure, unresolved active medication, or failed execution can therefore never look like a clean check.

Weight-aware dose checks read the newest fresh numeric Querystore `obs` matching `DRUG_SAFETY_WEIGHT_CONCEPT_UUID` (CIEL weight `5089...` by default). `DRUG_SAFETY_WEIGHT_MAX_AGE_DAYS` defaults to 90. Set the concept value to `none` to disable only the weight-aware arm. Missing, stale, malformed, or unavailable optional safety data degrades to no additional warning and never interrupts an answer.

Interaction rules preserve their source-assigned `severity`. `DRUG_SAFETY_MIN_INTERACTION_SEVERITY` defaults to `minor`, matching the bundled provider: rated `Unknown` rules are omitted, while `Minor`, `Moderate`, `Major`, and unrated curated rules remain eligible. An unrecognized setting falls back to `minor` rather than silently disabling rated warnings.

## Endpoints

- `POST /v1/chat/completions`: blocking or staged streaming profile execution.
- `GET /v1/models`: configured profile metadata, availability, validation capability,
  exact context requirements, at most one hub-selected available default marker,
  and a versioned top-level `backend` inventory (`catalog_reachable` plus exact
  router-advertised model IDs) for generic clients that own their own profiles.
  Router model IDs are public identifiers and must not embed credentials;
  endpoint and model metadata are credential-sanitized separately.
- `GET /v1/hub/query-profiles`: caller-orchestrated Catalyst query profiles,
  exact role/model/prompt/knob evidence, and live router availability.
- `POST /v1/hub/query-profiles/{profile}/roles/{role}/generate`: execute one
  configured query role. Callers provide non-system messages and an optional
  response format; callers cannot override the role model, prompt, or knobs.
  Every request that reaches the model returns versioned request evidence with
  the exact system and caller messages, selected profile/role/model, response
  format and model settings, and an RFC 8785 canonical SHA-256 digest. When the
  router supports it, the same evidence includes the exact rendered prompt and
  token count against the model's advertised context window and the role's
  configured output allowance. Missing rendering, counting, or window
  information is reported with a stable reason and is never estimated. For an
  otherwise valid request, only a known overflow blocks the model call; it
  returns `422` with the same evidence. The existing `token_accounting` field
  keeps its compatible successful shape.
  Named Catalyst role generation has no automatic total deadline. It waits for
  completion unless the caller disconnects or explicitly stops the request;
  either interruption cancels the outstanding model HTTP request and releases
  any slot held by that request. An interrupted waiter never releases another
  caller's slot. Transport interruption cannot supply a completed model
  response.
- `POST /v1/hub/query-profiles/{profile}/roles/{role}/warm`: internal
  deployment-lifecycle warmup for Catalyst's reusable writer prefix. It accepts
  the same caller context and applies the same fixed profile. Disconnecting the
  lifecycle caller cancels the model work. It returns no model content or
  request evidence and must not be used for a person’s query.
- `POST /v1/hub/generate`: raw single-model compatibility endpoint for generic
  consumers that own their own profile configuration. Catalyst does not use it.
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
