# Run with Docker Compose

Use the package compose files to run the server and client via Docker.

## Files

- `packages/med-agent-hub/docker-compose.yml` (server)
- `packages/med-agent-hub/docker-compose.server.dev.yml` (dev port mapping)
- `packages/med-agent-hub/docker-compose.client.yml` (client)
- `packages/med-agent-hub/docker-compose.client.dev.yml` (client dev port mapping)

## Environment

Set these for your environment (examples):

```bash
export LLM_BASE_URL=http://host.docker.internal:1234
export OPENMRS_FHIR_BASE_URL=http://openmrs:8080/openmrs/ws/fhir2/R4/
export A2A_ROUTER_URL=http://localhost:9100
export A2A_MEDGEMMA_URL=http://localhost:9101
export A2A_CLINICAL_URL=http://localhost:9102
export SPARK_THRIFT_HOST=spark-thrift
export SPARK_THRIFT_PORT=10000
export SPARK_THRIFT_DATABASE=default
```

## Commands (examples)

```bash
# Server
docker compose -f packages/med-agent-hub/docker-compose.yml \
               -f packages/med-agent-hub/docker-compose.server.dev.yml up -d

# Client
docker compose -f packages/med-agent-hub/docker-compose.client.yml \
               -f packages/med-agent-hub/docker-compose.client.dev.yml up -d

# View logs
docker compose -f packages/med-agent-hub/docker-compose.yml logs -f med-agent-hub-server
```

## Notes

- Ensure LM Studio is reachable from Docker (`host.docker.internal`).
- Confirm networks referenced by the package exist or are created by your orchestration.


