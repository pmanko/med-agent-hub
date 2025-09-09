# Test Suite Overview

This directory contains automated tests for the multi-agent system.

### Test Scripts

-   **`test_config.py`**: Validates the `.env` configuration file.
-   **`test_models_direct.py`**: Performs basic connectivity tests to ensure agents and the LLM are running.
-   **`test_a2a_sdk.py`**: A comprehensive end-to-end test suite that validates the entire A2A workflow, including agent discovery and configuration matching.
-   **`test_router_a2a.py`**: A focused integration test for the Router Agent's orchestration logic.

## Prerequisites

Ensure all development dependencies are installed:
```bash
poetry install --with dev
```

The integration tests require the agent services to be running:
```bash
poetry run python launch_a2a_agents.py
```

## Running Tests

To run all test suites in the recommended order, use the provided script:

```bash
./tests/run_tests.sh
```

Make sure the script is executable: `chmod +x tests/run_tests.sh`

You can also run tests individually with `pytest`.
