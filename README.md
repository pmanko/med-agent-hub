# med-agent-hub — in-process Med Agent Team with an OpenAI-compat bridge

A small team of LLMs that answers a clinician's question about ONE patient's chart, fronted by an **OpenAI-compatible `POST /v1/chat/completions`** endpoint. External consumers (e.g. OpenMRS chartsearchai, the clinical-ai-validation-harness) treat med-agent-hub as a drop-in LLM endpoint; each request runs an **orchestrator → tools → synthesis → validation** loop entirely in this process.

## Architecture

```
client (e.g. chartsearchai)
  │  POST /v1/chat/completions   (OpenAI shape: messages[], response_format?, stream?)
  ▼
med-agent-hub   (one FastAPI / uvicorn process, port 8080)
  │  temporal grounding — deterministic chart parse: reference-date anchor +
  │     per-concept value/date series, so the synth reports dates/trends instead
  │     of deriving them (server/temporal.py, no LLM)
  │  orchestrator  — decides which teammates to consult, then stops
  │     ├─ kb_search       tool — external reference guidance (WHO / essential-meds)
  │     └─ medical_expert  tool — interprets THIS chart against the question
  │  synthesis     — writes the Answer + In-Depth claims
  │  validation    — on *-validated levels: one validator pass per section drops
  │     unsupported claims and grades each section {level: green|yellow|red, note}
  ▼
OpenAI-compat LLM backend (LLM_BASE_URL) — one model call per role/step
```

The response is the `{answer, citations, blocks}` JSON envelope; on validated levels it carries a `confidence` block (per-section `{level, note}`) that clients render as a tag — the OpenMRS chat, the harness dashboard, and the report all use it.

Every system prompt (orchestrator, medical_expert, the synthesis variants, the validation prompts) is a plain file under `server/prompts/`, read per request — edit a (mounted) `.txt` and the next request picks it up, no rebuild.

Each turn also appends one structured package (shipped answer, in-depth claims, per-section confidence, ordered call steps) to `$TEAM_TRACE_DIR/trace.jsonl` (default `/app/trace`) — the reasoning-trace artifact the validation harness's dashboard and report correlate against.

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compat, sync + stream. Runs the team when `model` is a level id from `server/levels.yaml`; forwards any other `model` straight to a single backend model (a raw team-vs-single baseline).
- `GET /v1/models` — advertises every level defined in `server/levels.yaml`.
- `GET /` — root status (uptime + the active per-role models).
- `GET /health` — uptime + process memory.

## Team levels (declarative — `server/levels.yaml`)

One running instance serves every level — the request's `model` id picks the level, and the level declares its per-role models (`orchestrator`, `expert`, `synthesizer`, optional `validator` + `validator_max_loops`). Levels today:

- `med-agent-team-low` / `-med` / `-high` — step the synthesizer and expert up in capability; LOW also swaps to a synthesis prompt tuned for its smaller synthesizer (`prompts/synthesis-low.txt`).
- `…-validated` variants — add the per-section validator pass (LOW/MED use a fixed competent-floor validator; HIGH scales the validator to the tier).
- `med-agent-team-12b` — single-tier baseline team.
- `med-agent-team-parity` — chartsearchai-parity lane: hub orchestration + temporal grounding, bare envelope (the controlled comparison arm).

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
├── team.py            # the in-process team: orchestrator → kb_search/medical_expert → synthesis → validation; writes trace.jsonl
├── levels.yaml        # declarative team levels: per-role models, validator, knobs
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
