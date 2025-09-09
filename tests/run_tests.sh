#!/bin/bash
#
# This script starts the multi-agent system, runs all test suites,
# and then shuts down the services.
#
# Usage: ./tests/run_tests.sh
#

# --- Kill existing processes ---
echo "--- Stopping any lingering uvicorn processes ---"
pkill -f uvicorn || true

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Cleanup Function ---
cleanup() {
    echo ""
    echo "--- Shutting down agent services ---"
    if [ -n "$HONCHO_PID" ]; then
        # Kill the entire process group to ensure all child processes are terminated
        kill -TERM -$HONCHO_PID 2>/dev/null || true
    fi
}

# Trap EXIT signal to ensure cleanup runs
trap cleanup EXIT

# --- Service Startup ---
echo "--- Starting agent services with Honcho (using Procfile.dev) ---"
# Start honcho in a new process group
set -m
honcho -f Procfile.dev start &
HONCHO_PID=$!
set +m

echo "Waiting for services to initialize (15 seconds)..."
sleep 15

# --- Test Execution ---
# We explicitly pass the env file to the test scripts so they know which
# environment the services are running with.
ENV_FILE="env.recommended"

echo ""
echo "--- Running Configuration & Connectivity Tests ---"
poetry run python tests/test_config.py --env-file "$ENV_FILE"
poetry run python tests/test_models_direct.py --env-file "$ENV_FILE"

echo ""
echo "--- Running A2A Integration and E2E Tests ---"
poetry run python tests/test_a2a_sdk.py --env-file "$ENV_FILE"
poetry run python tests/test_router_a2a.py --env-file "$ENV_FILE"
poetry run python tests/test_react_orchestrator.py --env-file "$ENV_FILE"


echo ""
echo "✅ All tests passed successfully!"

# The 'trap' will handle cleanup on exit
