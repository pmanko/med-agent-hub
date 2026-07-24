# One uvicorn process hosts profile discovery and the stage engine.

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
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes \
    && pip install --user --no-cache-dir -r requirements.txt

# Stage 2: runtime
FROM python:3.11-slim
WORKDIR /app

ARG HUB_BUILD_REVISION=unknown
LABEL org.opencontainers.image.revision=${HUB_BUILD_REVISION}

COPY --from=builder /root/.local /usr/local
COPY server/ ./server/

# Logs go to stdout; reach the host-native llama.cpp router by default.
ENV PYTHONUNBUFFERED=1 \
    HUB_BUILD_REVISION=${HUB_BUILD_REVISION} \
    LLM_BASE_URL=http://host.docker.internal:8077 \
    LLM_TEMPERATURE=0.0

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=3).read()"]

# A numeric unprivileged identity needs no /etc/passwd mutation or runtime package install.
USER 65532:65532
CMD ["python", "-m", "uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8080"]
