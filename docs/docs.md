# Medical Multi-Agent Chat Documentation

A pure A2A (Agent-to-Agent) protocol implementation using the A2A SDK v0.3.2+ for collaborative medical AI.

## Quick Start

```bash
# 1. Configure environment
cd projects/med-agent-hub
cp env.recommended .env  # Edit with your LM Studio IP

# 2. Start all agents
honcho -f Procfile.dev start

# 3. Test the system
poetry run python test_models_direct.py
```

Expected: Router (9100), MedGemma (9101), Clinical (9102) agents running.

## Documentation

### Getting Started
- **[Configuration](getting-started/configuration.md)** - Environment setup
- **[LM Studio Setup](getting-started/lm-studio.md)** - Local LLM configuration

### Architecture  
- **[System Overview](architecture/overview.md)** - Design and A2A implementation
- **[Agent Reference](architecture/agents.md)** - Available agents and skills

### Development
- **[Creating Agents](development/creating-agents.md)** - Extend the system

## Key Files

```
server/
├── sdk_agents/         # Agent implementations  
├── agent_cards/        # Agent capabilities (JSON)
└── config.py          # Central configuration

env.recommended        # Default configuration
Procfile.dev          # Process management
test_models_direct.py # System validation
```

## Support

- Check logs in `logs/` directory
- Ensure LM Studio is running with models loaded
- Verify `AGENT_HOST_IP` matches your network setup

