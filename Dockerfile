# Single-image build. The OpenAI-compat bridge runs the Med Agent Team
# in-process (no A2A subagent processes), so one uvicorn is the whole server.

# Stage 1: resolve + install dependencies with Poetry
FROM python:3.11-slim AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV POETRY_VERSION=1.8.3 \
    POETRY_VENV=/opt/poetry-venv \
    POETRY_NO_INTERACTION=1
RUN python -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install --upgrade pip setuptools \
    && $POETRY_VENV/bin/pip install "poetry==${POETRY_VERSION}"
ENV PATH="${PATH}:${POETRY_VENV}/bin"

COPY pyproject.toml poetry.lock ./
# Bridge runtime needs only the base deps (no spark/duckdb extras).
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes \
    && pip install --user --no-cache-dir -r requirements.txt

# Stage 2: runtime
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appuser && useradd -r -g appuser appuser

COPY --from=builder /root/.local /usr/local
COPY server/ ./server/

# Logs go to stdout (docker logs); reach the host's LM Studio by default.
ENV PYTHONUNBUFFERED=1 \
    LLM_BASE_URL=http://host.docker.internal:1234 \
    LLM_TEMPERATURE=0.0

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8080/health || exit 1

USER appuser
CMD ["python", "-m", "uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8080"]
