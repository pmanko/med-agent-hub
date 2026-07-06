# med-agent-hub — staged clinical answer service with an OpenAI-compat bridge

Clinical answer service for one patient's chart, fronted by an **OpenAI-compatible `POST /v1/chat/completions`** endpoint. External consumers such as OpenMRS ChartSearchAI and the validation harness treat med-agent-hub as the LLM endpoint; the requested model id selects a stage-composed profile or a low-level leg.

## Architecture

```
client (e.g. ChartSearchAI)
  │  POST /v1/chat/completions   (OpenAI shape: messages[], response_format?, stream?)
  ▼
med-agent-hub   (one FastAPI / uvicorn process, port 8080)
  │  context       — retrieves chart/mappings when a patient is supplied
  │  gather        — optional team/orchestrator/expert stage
  │  answer        — writer produces answer/citations/blocks
  │  gate          — deterministic temporal/date/substance checks
  │  review        — optional answer rewrite/review stage
  │  grounding     — final citation verdicts for the final answer
  │  in-depth      — generated after the checked/edited answer
  ▼
OpenAI-compat LLM backend (LLM_BASE_URL) — one model call per role/step
```

The response is the `{answer, citations, blocks}` JSON envelope, optionally carrying `answerValidation`, `confidence`, `inDepth`, `references`, temporal-gate metadata, and safety warnings. Staged profiles stream phase events (`answer_done`, optional `answer_validation`, `indepth_pending`, `indepth_done`/`indepth_error`, `done`) so ChartSearchAI can show the fast answer before the slower tail finishes.

Every system prompt is a plain file under `server/prompts/`, read per request — edit a mounted `.txt` and the next request picks it up, no rebuild.

Each turn also appends one structured package (shipped answer, in-depth claims, per-section confidence, ordered call steps) to `$TEAM_TRACE_DIR/trace.jsonl` (default `/app/trace`) — the reasoning-trace artifact the validation harness's dashboard and report correlate against.

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compat, sync + stream. Runs the selected profile/leg when `model` is a configured or dynamic hub id; forwards unknown raw model ids to the backend.
- `GET /v1/models` — advertises configured profiles and dynamic answer/in-depth legs.
- `GET /` — root status (uptime + the active per-role models).
- `GET /health` — uptime + process memory.

## Profiles and low-level legs (declarative — `server/levels.yaml`)

One running instance serves every profile. The request's `model` id picks a configured profile from `server/levels.yaml` or a dynamic low-level id such as `answer:<writer>@<prompt>~enforce~temp0` / `indepth-only:<writer>`. Configured profiles declare stage inputs such as `answer_model` or role models, optional `validator`, `temporal_gate`, `staged`, and prompt names.

- Product profiles such as `single-12b-checked`, `single-e4b-checked`, and `single-a4b-checked` run a single writer with temporal enforcement, optional review, final grounding, and In-Depth.
- Team profiles keep the optional `gather` stage for orchestrator/expert experiments.
- Raw legs (`answer:...`, `answer-review:...`, `indepth-only:...`) stay minimal for harness/debug use and do not add product grounding verdicts.

Adding or retuning a level is a `levels.yaml` edit — no code change. (If the file is bind-mounted into a running container, recreate the service after editing; an in-place edit on macOS detaches the mount.)

## Quickstart (local dev)

```bash
poetry install --with dev
cp env.recommended .env        # set LLM_BASE_URL + the per-level model names
poetry run uvicorn server.main:app --host 0.0.0.0 --port 8080 --reload --env-file .env
```

Smoke test:

```bash
curl -fsS http://localhost:8080/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "med-agent-team-med",
  "messages": [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user",   "content": "[1] Diabetes [2] Lisinopril 10 mg"},
    {"role": "user",   "content": "What meds is this patient on?"}
  ]
}'
```

## Docker

```bash
docker build -t med-agent-hub:dev .
docker run --rm -p 8080:8080 \
  -e LLM_BASE_URL=http://host.docker.internal:1234 \
  --add-host host.docker.internal:host-gateway \
  med-agent-hub:dev
```

The image runs `uvicorn server.main:app` (a single process — the team is in-process, no subagent services). `LLM_BASE_URL` is the backend's **server root**; the code appends `/v1/chat/completions` (so do not include `/v1`).

## Project structure

```
server/
├── main.py            # FastAPI app (observability + bridge mount) — the entrypoint
├── openai_compat.py   # /v1/chat/completions, /v1/models
├── team.py            # stage engine: context/gather/answer/gate/review/grounding/in-depth; writes trace.jsonl
├── levels.yaml        # declarative profiles/legs: models, validator, temporal gate, knobs
├── levels_loader.py   # parses/validates levels.yaml, serves level ids to /v1/models
├── temporal.py        # deterministic temporal grounding (anchor + per-concept series; no LLM)
├── prompt_loader.py   # file-backed prompts (prompts/*.txt, read per request)
├── prompts/           # orchestrator, medical_expert, synthesis* (incl. -low/-answer/-indepth/-chartsearchai), validation* (.txt)
├── kb.py + kb_data/   # knowledge-base search tool (reference snippets)
├── config.py          # env config (LLM_BASE_URL, defaults the levels reference)
└── schemas.py         # request / response models
```

`server/sdk_agents/`, `server/agent_configs/`, `server/mcp/`, and `server/llm_clients.py` are **legacy A2A modules** retained on disk but **not wired into `server.main`** — the entrypoint mounts only the in-process bridge above.

## Requirements

- Python 3.10+
- Poetry
- An OpenAI-compat LLM endpoint (LM Studio, llama.cpp server, …) reachable at `LLM_BASE_URL`

## License

MIT
