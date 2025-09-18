#!/bin/bash
#
# This script starts the multi-agent system, runs all test suites,
# and then shuts down the services.
#
# Usage: ./tests/run_tests.sh [test-selector]
#
# Test selectors:
#   all            - Run all test suites (default)
#   config         - Configuration tests
#   models         - Direct model tests
#   a2a            - A2A SDK tests
#   router         - Router agent tests
#   react          - ReAct orchestrator tests
#   mcp            - MCP tool tests (no agents needed)
#   mcp-integration - Full MCP integration tests
#

set -euo pipefail

echo "--- Stopping any lingering uvicorn processes ---"
pkill -f "uvicorn server" || true

# --- Cleanup Function ---
cleanup() {
    echo ""
    echo "--- Shutting down agent services ---"
    if [ -n "${HONCHO_PID:-}" ]; then
        # Kill the entire process group to ensure all child processes are terminated
        kill -TERM -"$HONCHO_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

# --- Service Startup ---
echo "--- Starting agent services with Honcho (using Procfile.dev) ---"
set -m
mkdir -p logs
# Redirect honcho output to a log file to avoid BrokenPipe when the parent exits
poetry run honcho -f Procfile.dev start >> logs/honcho.out 2>&1 &
HONCHO_PID=$!
set +m

# --- Readiness Check ---
echo "Waiting for services to become ready..."
READY=0
for i in $(seq 1 30); do
  OK=0
  curl -sf http://localhost:8080/health >/dev/null && OK=$((OK+1)) || true
  curl -sf http://localhost:9100/.well-known/agent-card.json >/dev/null && OK=$((OK+1)) || true
  curl -sf http://localhost:9101/.well-known/agent-card.json >/dev/null && OK=$((OK+1)) || true
  curl -sf http://localhost:9102/.well-known/agent-card.json >/dev/null && OK=$((OK+1)) || true
  # Note: Admin agent on 9103 is optional for now
  if [ "$OK" -eq 4 ]; then READY=1; break; fi
  sleep 2
done

if [ "$READY" -ne 1 ]; then
  echo "Services failed to become ready in time. Tailing recent logs:"
  tail -n 200 logs/*.log || true
  exit 1
fi

###############################################
# Test Execution
###############################################
ENV_FILE=".env"

run_case() {
  case "$1" in
    all)
      echo ""; echo "--- Running Configuration & Connectivity Tests ---"
      poetry run python tests/test_config.py --env-file "$ENV_FILE"
      poetry run python tests/test_models_direct.py --env-file "$ENV_FILE"
      echo ""; echo "--- Running A2A Integration and E2E Tests ---"
      poetry run python tests/test_a2a_sdk.py --env-file "$ENV_FILE"
      poetry run python tests/test_router_a2a.py --env-file "$ENV_FILE"
      poetry run python tests/test_react_orchestrator.py --env-file "$ENV_FILE"
      echo ""; echo "--- Running MCP Tool and Integration Tests ---"
      poetry run python tests/test_mcp_tools_direct.py
      poetry run python tests/test_mcp_integration.py --env-file "$ENV_FILE"
      ;;
    react)
      poetry run python tests/test_react_orchestrator.py --env-file "$ENV_FILE" ;;
    config)
      poetry run python tests/test_config.py --env-file "$ENV_FILE" ;;
    models)
      poetry run python tests/test_models_direct.py --env-file "$ENV_FILE" ;;
    a2a)
      poetry run python tests/test_a2a_sdk.py --env-file "$ENV_FILE" ;;
    router)
      poetry run python tests/test_router_a2a.py --env-file "$ENV_FILE" ;;
    mcp)
      echo ""; echo "--- Running MCP Tool Tests ---"
      poetry run python tests/test_mcp_tools_direct.py
      ;;
    mcp-integration)
      echo ""; echo "--- Running MCP Integration Tests ---"
      poetry run python tests/test_mcp_integration.py --env-file "$ENV_FILE"
      ;;
    *)
      if [ -f "$1" ]; then
        poetry run python "$1" --env-file "$ENV_FILE"
      else
        echo "Unknown test selector: $1" >&2; exit 1
      fi
      ;;
  esac
}

if [ "$#" -eq 0 ]; then
  run_case all
else
  for sel in "$@"; do
    run_case "$sel"
  done
fi

echo ""; echo "✅ Selected tests completed successfully!"

# Cleanup handled by trap
