# med-agent-hub — in-process Med Agent Team with an OpenAI-compat bridge

A small team of LLMs that answers a clinician's question about ONE patient's chart, fronted by an **OpenAI-compatible `POST /v1/chat/completions`** endpoint. External consumers (e.g. OpenMRS chartsearchai) treat med-agent-hub as a drop-in LLM endpoint; each request runs an **orchestrator → tools → synthesis** loop entirely in this process.

## Architecture

```
client (e.g. chartsearchai)
  │  POST /v1/chat/completions   (OpenAI shape: messages[], response_format?, stream?)
  ▼
med-agent-hub   (one FastAPI / uvicorn process, port 8080)
  │  orchestrator  — decides which teammates to consult, then stops
  │     ├─ kb_search       tool — external reference guidance (WHO / essential-meds)
  │     └─ medical_expert  tool — interprets THIS chart against the question
  │  synthesis     — writes the final {answer, citations, blocks} JSON envelope
  ▼
OpenAI-compat LLM backend (LLM_BASE_URL) — one model call per role/step
```

The orchestrator, medical_expert, and synthesis system prompts are plain files under `server/prompts/`, read per request — edit a (mounted) `.txt` and the next request picks it up, no rebuild.

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compat, sync + stream. Runs the team when `model` is a team preset; forwards any other `model` straight to a single backend model (a raw team-vs-single baseline).
- `GET /v1/models` — advertises the three team presets: `med-agent-team-low`, `med-agent-team-med`, `med-agent-team-high`.
- `GET /` — root status (uptime + the active per-role models).
- `GET /health` — uptime + process memory.

## Team levels

One running instance serves all three levels — the `model` id selects which per-role models run for that request (persistent config, never sent per request):

- `med-agent-team-low` / `-med` / `-high` step the **synthesizer** and **clinical-expert** models up in capability (e.g. the synthesizer goes qwen2.5-14b → qwen2.5-32b → qwen3.6); the orchestrator is a small model for low/med and a larger one for high.
- The `low` level also swaps to a synthesis prompt tuned for its smaller synthesizer (`prompts/synthesis-low.txt`).

The exact model ids are env-configurable in `server/config.py`.

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
├── team.py            # the in-process team: orchestrator → kb_search/medical_expert → synthesis
├── prompt_loader.py   # file-backed prompts (prompts/*.txt, read per request)
├── prompts/           # orchestrator, medical_expert, synthesis, synthesis-low (.txt)
├── kb.py + kb_data/   # knowledge-base search tool (reference snippets)
├── config.py          # env config (LLM_BASE_URL, per-level / per-role models)
└── schemas.py         # request / response models
```

`server/sdk_agents/`, `server/agent_configs/`, `server/mcp/`, and `server/llm_clients.py` are **legacy A2A modules** retained on disk but **not wired into `server.main`** — the entrypoint mounts only the in-process bridge above.

## Requirements

- Python 3.10+
- Poetry
- An OpenAI-compat LLM endpoint (LM Studio, llama.cpp server, …) reachable at `LLM_BASE_URL`

## License

MIT
