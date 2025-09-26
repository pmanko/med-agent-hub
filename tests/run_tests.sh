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

echo "--- Ensuring a clean slate (stopping lingering processes) ---"

# Load env to allow port overrides
ENV_FILE=".env"
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

# Resolve ports from env with sensible defaults
WEB_PORT="${WEB_PORT:-8080}"
ROUTER_PORT="${A2A_ROUTER_PORT:-9100}"
MEDICAL_PORT="${A2A_MEDICAL_PORT:-${A2A_MEDGEMMA_PORT:-9101}}"
CLINICAL_PORT="${A2A_CLINICAL_PORT:-9102}"
ADMIN_PORT="${A2A_ADMIN_PORT:-9103}"

# Flag to signal we could not terminate some processes
UNKILLED_WARN=0

# Helper to kill by pattern with visibility
kill_by_pattern() {
  local label="$1"
  local pattern="$2"
  local pids
  pids=$(pgrep -f "$pattern" 2>/dev/null || true)
  if [ -n "${pids}" ]; then
    echo "Found $(echo "$pids" | wc -l | tr -d ' ') ${label} processes: $(echo "$pids" | tr '\n' ' ')"
    # Show ownership and command for visibility
    echo "Details for ${label} candidates:"
    for pid in $pids; do
      ps -o pid= -o user= -o cmd= -p "$pid" 2>/dev/null || true
    done
    # Try graceful termination first
    pkill -TERM -f "$pattern" >/dev/null 2>&1 || true
    sleep 0.3
    local remaining
    remaining=$(pgrep -f "$pattern" 2>/dev/null || true)
    # Force kill any leftovers owned by current user
    if [ -n "${remaining}" ]; then
      for pid in $remaining; do
        owner=$(ps -o user= -p "$pid" 2>/dev/null | tr -d ' ' || true)
        if [ "$owner" = "$USER" ] || [ -z "$owner" ]; then
          kill -KILL "$pid" >/dev/null 2>&1 || true
        fi
      done
      sleep 0.2
    fi
    # Final check
    remaining=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "${remaining}" ]; then
      echo "Warning: still running ${label} PIDs (may be other user/system): $(echo "$remaining" | tr '\n' ' ')"
      UNKILLED_WARN=1
    else
      echo "Successfully stopped ${label} processes."
    fi
  else
    echo "No ${label} processes found."
  fi
}

# Kill by pattern across all users using sudo if necessary (non-interactive)
kill_by_pattern_allusers() {
  local label="$1"
  local pattern="$2"
  local pids
  pids=$(pgrep -f "$pattern" 2>/dev/null || true)
  if [ -n "${pids}" ]; then
    echo "Found $(echo "$pids" | wc -l | tr -d ' ') ${label} processes: $(echo "$pids" | tr '\n' ' ')"
    echo "Details for ${label} candidates:"
    for pid in $pids; do
      ps -o pid= -o user= -o cmd= -p "$pid" 2>/dev/null || true
    done
    pkill -TERM -f "$pattern" >/dev/null 2>&1 || true
    sleep 0.3
    local remaining
    remaining=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "${remaining}" ]; then
      for pid in $remaining; do
        owner=$(ps -o user= -p "$pid" 2>/dev/null | tr -d ' ' || true)
        if [ "$owner" = "$USER" ] || [ -z "$owner" ]; then
          kill -KILL "$pid" >/dev/null 2>&1 || true
        else
          if command -v sudo >/dev/null 2>&1; then
            sudo -n kill -KILL "$pid" >/dev/null 2>&1 || true
          fi
        fi
      done
      sleep 0.2
    fi
    remaining=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "${remaining}" ]; then
      echo "Warning: still running ${label} PIDs after sudo attempt: $(echo "$remaining" | tr '\n' ' ')"
      UNKILLED_WARN=1
    else
      echo "Successfully stopped ${label} processes."
    fi
  else
    echo "No ${label} processes found."
  fi
}

# 1) Try to stop prior Honcho groups
kill_by_pattern "honcho" "honcho -f Procfile.dev"

# 2) Stop uvicorn servers bound to our known ports (use sudo as needed)
UVICORN_PIDS=""
for P in "$WEB_PORT" "$ROUTER_PORT" "$MEDICAL_PORT" "$CLINICAL_PORT" "$ADMIN_PORT"; do
  if command -v lsof >/dev/null 2>&1; then
    candidates=$(lsof -i :"$P" -sTCP:LISTEN -t 2>/dev/null || true)
    if [ -n "$candidates" ]; then
      echo "Found listeners on port $P: $(echo "$candidates" | tr '\n' ' ')"
      for pid in $candidates; do
        if ps -o cmd= -p "$pid" 2>/dev/null | grep -E "uvicorn|python" >/dev/null 2>&1; then
          UVICORN_PIDS+="$pid "
        fi
      done
    fi
  fi
done

if [ -n "$UVICORN_PIDS" ]; then
  echo "Targeting uvicorn PIDs on our ports: $UVICORN_PIDS"
  for pid in $UVICORN_PIDS; do kill -TERM "$pid" >/dev/null 2>&1 || true; done
  sleep 0.3
  REMAINING=""
  for pid in $UVICORN_PIDS; do
    if kill -0 "$pid" >/dev/null 2>&1; then REMAINING+="$pid "; fi
  done
  if [ -n "$REMAINING" ] && command -v sudo >/dev/null 2>&1; then
    echo "Escalating kill for PIDs: $REMAINING"
    for pid in $REMAINING; do sudo -n kill -KILL "$pid" >/dev/null 2>&1 || true; done
    sleep 0.2
  fi
  STILL=""
  for pid in $UVICORN_PIDS; do
    if kill -0 "$pid" >/dev/null 2>&1; then STILL+="$pid "; fi
  done
  if [ -n "$STILL" ]; then
    echo "Warning: still running PIDs on our ports: $STILL"
    UNKILLED_WARN=1
  else
    echo "Successfully stopped uvicorn on our ports."
  fi
else
  echo "No uvicorn processes bound to our ports."
fi

# 3) Ensure ports are free (best-effort)
PORTS=("$WEB_PORT" "$ROUTER_PORT" "$MEDICAL_PORT" "$CLINICAL_PORT" "$ADMIN_PORT")
for PORT in "${PORTS[@]}"; do
  if command -v lsof >/dev/null 2>&1; then
    before=$(lsof -i :"${PORT}" -sTCP:LISTEN -t 2>/dev/null || true)
    if [ -n "${before}" ]; then
      echo "Port ${PORT} in use by PIDs: $(echo "$before" | tr '\n' ' ')"
    else
      echo "Port ${PORT} is already free."
    fi
  fi
  if command -v fuser >/dev/null 2>&1; then
    # Graceful then force if needed
    fuser -k -TERM ${PORT}/tcp >/dev/null 2>&1 || true
    sleep 0.2
    fuser -k -KILL ${PORT}/tcp >/dev/null 2>&1 || true
  fi
  if command -v lsof >/dev/null 2>&1; then
    after=$(lsof -i :"${PORT}" -sTCP:LISTEN -t 2>/dev/null || true)
    if [ -n "${after}" ]; then
      echo "Warning: port ${PORT} still in use by PIDs: $(echo "$after" | tr '\n' ' ')"
    else
      echo "Port ${PORT} is free."
    fi
  fi
done

sleep 0.5

# If some processes could not be terminated, advise running with sudo
if [ "${UNKILLED_WARN:-0}" -eq 1 ]; then
  echo ""
  echo "WARNING: Some processes could not be terminated and may be owned by other users."
  echo "Consider rerunning with elevated privileges:"
  echo "  sudo $0 $*"
  echo ""
fi

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
# Stream honcho output to console and persist to logs
poetry run honcho -f Procfile.dev start 2>&1 | tee -a logs/honcho.out &
HONCHO_PID=$!
set +m

# --- Readiness Check ---
echo "Waiting for services to become ready..."
READY=0
for i in $(seq 1 30); do
  OK=0
  curl -sf http://localhost:"$WEB_PORT"/health >/dev/null && OK=$((OK+1)) || true
  curl -sf http://localhost:"$ROUTER_PORT"/.well-known/agent-card.json >/dev/null && OK=$((OK+1)) || true
  curl -sf http://localhost:"$MEDICAL_PORT"/.well-known/agent-card.json >/dev/null && OK=$((OK+1)) || true
  curl -sf http://localhost:"$CLINICAL_PORT"/.well-known/agent-card.json >/dev/null && OK=$((OK+1)) || true
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
