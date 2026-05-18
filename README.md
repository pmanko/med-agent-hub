# med-agent-hub — A2A medical chat with OpenAI-compat bridge

Three specialized agents (Router, Medical/MedGemma, Clinical) collaborating over the A2A protocol, fronted by an **OpenAI-compat `POST /v1/chat/completions`** endpoint. External consumers (e.g. OpenMRS chartsearchai) treat med-agent-hub as a drop-in LLM endpoint; the router classifies each query and dispatches to the right subagent.

## Architecture

```
client (e.g. chartsearchai)
  │  POST /v1/chat/completions  (OpenAI shape: messages[], response_format?, stream?)
  ▼
med-agent-hub
  │  ─ FastAPI web facade (port 8080)
  │  ─ A2A router (port 9100) classifies via cheap LLM
  │  ─ subagent (medical 9101 | clinical 9102) handles the synthesis
  ▼
LM Studio / OpenAI-compat LLM backend (LLM_BASE_URL)
```

## Endpoints

- `POST /v1/chat/completions` — OpenAI-compat sync + stream. Always engages router; the router picks {medical, clinical} and forwards the full `messages[]` array.
- `GET /v1/models` — virtual models advertised: `router` (the default contract for external consumers).
- `GET /v1/agents` — A2A skill discovery (aggregated agent-card skills).
- `GET /` — root status.
- `GET /manifest` — A2A agent URL pointers (diagnostic).
- `GET /health` — uptime + process memory.

There are no legacy `/generate/*` or `/chat` endpoints; they were removed when the OpenAI-compat bridge landed.

## Quickstart (local dev)

```bash
poetry install --with dev --extras "spark duckdb"
cp env.recommended .env  # configure LLM_BASE_URL, model names, A2A ports
poetry run honcho -f Procfile.dev start
```

This launches four processes: web (8080), router (9100), medical (9101), clinical (9102). The web process is the only externally-reachable surface; the agent ports stay inside the host/container.

Smoke test:

```bash
curl -fsS http://localhost:8080/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "router",
  "messages": [
    {"role": "system", "content": "You are a clinical assistant."},
    {"role": "user",   "content": "Patient records: [1] Diabetes [2] Lisinopril"},
    {"role": "user",   "content": "What meds is this patient on?"}
  ]
}'
```

## Docker

```bash
docker build -t med-agent-hub:dev .
docker run --rm -p 8080:8080 \
  -e LLM_BASE_URL=http://host.docker.internal:1234/v1 \
  --add-host host.docker.internal:host-gateway \
  med-agent-hub:dev
```

## Agent specialization (mixture of experts)

| Agent | Default model | Role |
|---|---|---|
| router | `llama-3.1-8b-instruct` | Cheap LLM classifier, JSON-only output |
| medical | `medgemma-1.5-4b-it` | Clinical synthesis with disclaimer |
| clinical | `gemma-3-4b-it` | Research / longitudinal record queries |

All three are routed through one LM Studio instance at `LLM_BASE_URL`.

## Documentation

- [Configuration](docs/getting-started/configuration.md)
- [LM Studio Setup](docs/getting-started/lm-studio.md)
- [System Overview](docs/architecture/overview.md)
- [Agent Reference](docs/architecture/agents.md)
- [Creating Agents](docs/development/creating-agents.md)

## Project structure

```
server/
├── main.py            # FastAPI app (observability + bridge mount)
├── openai_compat.py   # /v1/chat/completions, /v1/models, /v1/agents
├── sdk_agents/        # Router + Medical + Clinical executors
├── agent_configs/     # YAML agent cards (skills, prompts, model assignment)
├── mcp/               # MCP tool registry (feature-gated)
├── llm_clients.py     # LM Studio HTTP client
└── config.py          # Env config

Procfile.dev           # honcho process spec for local dev
Dockerfile             # single-image container build
env.recommended        # default settings template
```

## Requirements

- Python 3.10+
- Poetry
- LM Studio (or any OpenAI-compat LLM endpoint) reachable at `LLM_BASE_URL`

## License

MIT
