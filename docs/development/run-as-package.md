# Run as Instant OpenHIE v2 Package

This project can be run as a package under Instant OpenHIE v2.

## Prerequisites

- Instant OpenHIE v2 tooling available in the repo root
- Environment variables configured (see below)

## Environment

Key variables used by the package (see `packages/med-agent-hub/package-metadata.json`):

- `LLM_BASE_URL` (default: `http://host.docker.internal:1234`)
- `OPENMRS_FHIR_BASE_URL` (e.g., `http://openmrs:8080/openmrs/ws/fhir2/R4/`)
- `A2A_ROUTER_URL`, `A2A_MEDGEMMA_URL`, `A2A_CLINICAL_URL`
- `SPARK_THRIFT_HOST`, `SPARK_THRIFT_PORT`, `SPARK_THRIFT_DATABASE`

## Commands

```bash
# From repo root
./instant package init -n med-agent-hub -d
./instant package up -n med-agent-hub -d

# Tear down
./instant package down -n med-agent-hub
./instant package destroy -n med-agent-hub
```

Compose files and metadata:

- `packages/med-agent-hub/docker-compose.yml`
- `packages/med-agent-hub/docker-compose.server.dev.yml`
- `packages/med-agent-hub/docker-compose.client.yml`
- `packages/med-agent-hub/docker-compose.client.dev.yml`
- `packages/med-agent-hub/package-metadata.json`

## Notes

- Ensure LM Studio is reachable from Docker (`host.docker.internal` may be required on macOS/Windows).
- Export environment variables or create a `.env` consumed by the package tooling.


