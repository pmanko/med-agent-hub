# Medical Multi-Agent Chat System

A pure A2A (Agent-to-Agent) protocol implementation for collaborative medical AI using the A2A SDK.

## Features

- **Three Specialized Agents**: Router (orchestration), MedGemma (medical Q&A), Clinical (research)
- **Local-First**: Runs entirely on your hardware with LM Studio
- **A2A Protocol**: Full compliance using A2A SDK v0.3.2+
- **Configurable**: Environment-based configuration

## Quick Start

```bash
# 1. Setup
cd projects/med-agent-hub
cp env.recommended .env
# Edit .env: Set AGENT_HOST_IP and LLM_BASE_URL

# 2. Install dependencies
poetry install

# 3. Start all agents
honcho -f Procfile.dev start

# 4. Test
poetry run python test_models_direct.py
```

## Documentation

See project docs:
- [Docs index](docs/docs.md)
- [Configuration](docs/getting-started/configuration.md)
- [LM Studio Setup](docs/getting-started/lm-studio.md)
- [System Overview](docs/architecture/overview.md)
- [Agent Reference](docs/architecture/agents.md)
- [Creating Agents](docs/development/creating-agents.md)

## Project Structure

```
server/
├── sdk_agents/        # Agent implementations
├── agent_cards/       # Agent capabilities
└── config.py         # Configuration

env.recommended       # Default settings
Procfile.dev         # Process management
test_models_direct.py # System tests
```

## Requirements

- Python 3.10+
- LM Studio with models loaded
- Poetry for dependency management

## License

MIT