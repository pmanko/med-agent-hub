# Development Setup

This guide covers the Python-only development workflow using Poetry to manage all dependencies, with optional Node-based web UI.

## Prerequisites

- Python 3.10–3.13
- Poetry
- Optional services for full functionality:
  - LM Studio (local LLM) running at `http://localhost:1234`
  - OpenMRS Gateway providing FHIR R4 at `http://localhost:8090/openmrs/ws/fhir2/R4/`
  - Spark Thrift server at `localhost:10001` (Spark UI `http://localhost:4041`)
- Optional web UI: Node 18+ (for `web/` only)

## Environment Configuration

```bash
cd projects/med-agent-hub
cp env.recommended .env
# Edit .env as needed (LLM_BASE_URL, FHIR, Spark)
```

Minimal `.env` keys for local dev:

```env
LLM_BASE_URL=http://localhost:1234
AGENT_HOST_IP=localhost
A2A_ROUTER_PORT=9100
A2A_MEDGEMMA_PORT=9101
A2A_CLINICAL_PORT=9102
OPENMRS_FHIR_BASE_URL=http://localhost:8090/openmrs/ws/fhir2/R4/
OPENMRS_USERNAME=admin
OPENMRS_PASSWORD=Admin123
SPARK_THRIFT_HOST=localhost
SPARK_THRIFT_PORT=10001
SPARK_THRIFT_DATABASE=default
```

## Install Dependencies

```bash
poetry install --with dev --extras "spark duckdb"
```

## Validate Configuration

```bash
poetry run python tests/test_config.py
```

## Run API and Agents

Recommended (honcho via Poetry):

```bash
poetry run honcho -f Procfile.dev start
# API: http://localhost:8080
```

Fallback (without honcho):

```bash
# Terminal 1 (API)
poetry run uvicorn server.main:app --host 0.0.0.0 --port 8080 --reload

# Terminal 2 (agents)
poetry run python launch_a2a_agents.py
```

## Health Checks

```bash
curl http://localhost:8080/health
curl http://localhost:8080/manifest
curl http://localhost:9100/.well-known/agent-card.json
curl http://localhost:9101/.well-known/agent-card.json
curl http://localhost:9102/.well-known/agent-card.json
```

## Optional Web App (Svelte/Vite)

The UI under `web/` is a separate Node environment.

```bash
cd projects/med-agent-hub/web
npm ci
npm run dev  # http://localhost:5173 (proxies API to :8080)
```

## Port Map (defaults)

| Service | Port |
|--------|------|
| API (FastAPI) | 8080 |
| Router Agent | 9100 |
| Med Agent | 9101 |
| Clinical Agent | 9102 |
| LM Studio | 1234 |
| OpenMRS Gateway (FHIR) | 8090 |
| Spark Thrift | 10001 |
| Spark UI | 4041 |
| Web Dev (Vite) | 5173 |

## Troubleshooting

- Ensure LM Studio is running and `LLM_BASE_URL` matches.
- Check that OpenMRS Gateway and Spark Thrift ports are listening.
- Increase `CHAT_TIMEOUT_SECONDS` for long LLM calls.
- See also: docs/getting-started/configuration.md


