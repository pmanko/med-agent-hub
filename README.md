# Medical Multi-Agent Chat System

A pure A2A (Agent-to-Agent) protocol implementation for collaborative medical AI using the A2A SDK.

## Features

- **Three Specialized Agents**: Router (orchestration), MedGemma (medical Q&A), Clinical (research)
- **Local-First**: Runs entirely on your hardware with LM Studio
- **A2A Protocol**: Full compliance using A2A SDK v0.3.2+
- **Configurable**: Environment-based configuration

## Run Options

- Development setup and workflow (Python-only, Poetry-managed): see the Development Guide
  - docs/development/dev-setup.md
- Run with Docker Compose: docs/development/run-with-docker.md
- Run as an Instant OpenHIE v2 package: docs/development/run-as-package.md
- LM Studio setup for local models: docs/getting-started/lm-studio.md

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
- Poetry for dependency management
- See the Development Guide for optional services used during development

## License

MIT