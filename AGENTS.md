# Working in `med-agent-hub`

`med-agent-hub` is the shared, profile-driven OpenAI `/v1` clinical AI runtime used by ChartSearchAI, Catalyst, the validation harness, and standalone OpenAI-compatible clients.

## Architecture & Scope Boundaries

- **Runtime Entrypoint (`server/main.py`):** Exposes `/v1/chat/completions` and profile configuration defined in [`server/levels.yaml`](server/levels.yaml) (`server/engine.py`, `server/openai_compat.py`, `server/temporal.py`, `server/kb.py`), plus the co-located `ddi_service/` package.
- **Keep Domain Business Logic Out of Hub Core:** Calling products own their domain SQL/EHR business logic and call named caller-orchestrated or hosted profiles rather than embedding product-specific rules inside core Hub stages.
- **Isolate Profiles by Consumer Namespace:** Keep workflow/experiment profiles cleanly separated by prefix (`openmrs-*`, `wahs-*`, `who-smart-*`, `catalyst-sql-*`, `lab-*`) in `server/levels.yaml`.
- **Docker Build Revision Check:** CI builds multi-arch images (`amd64`/`arm64`) with real `HUB_BUILD_REVISION` because `validate_config()` rejects the `"unknown"` default.

## Local Development & Verification

Requires Python `>=3.10,<3.14` and Poetry (`1.8.3` pinned in CI):

```bash
poetry install --with dev --no-interaction
poetry run pytest -q tests/
```

Style checks (configured in `pyproject.toml` dev group):

```bash
poetry run black --check .
poetry run isort --check .
poetry run flake8
```

Local server (`Procfile.dev`):

```bash
UVICORN_ENV_FILE=.env poetry run uvicorn server.main:app --host 0.0.0.0 --port ${WEB_PORT:-8080} --reload --env-file .env
```

## Git & PR Hygiene

- Branch cleanly from `main`; never commit secrets, `.env`, or local patient data.
- Push feature branches and open PRs on `pmanko/med-agent-hub` promptly so `hub-ci` (`unit-and-contract` + `docker-build-arch`) runs early.
