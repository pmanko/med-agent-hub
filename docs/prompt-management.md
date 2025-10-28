# Prompt Management with Agenta

This guide covers the Agenta integration for web-based prompt management and rapid iteration.

## What Is Agenta?

Agenta is an open-source LLMOps platform focused on prompt engineering, testing, and version control. It's deployed as an Instant OpenHIE package alongside your med-agent-hub agents.

**Key Features:**
- Interactive prompt playground with LM Studio integration
- Version control with full history and rollback
- Side-by-side prompt comparison and A/B testing
- Collaborative editing via web UI
- Environment management (dev/staging/prod)

## Architecture Overview

The med-agent-hub project uses a **dual-source prompt system**:

1. **Agenta API** (primary) - Web-based prompt management with version control
2. **YAML files** (fallback) - File-based prompts in `server/agent_configs/*.yaml`

Benefits:
- Edit prompts through web UI without code changes
- Test prompts in Agenta playground with your LM Studio models
- Version control with full history and rollback
- Graceful degradation if Agenta unavailable

## Prerequisites

Before deploying Agenta, ensure these packages are running:

```bash
# Check status
./instant package status -n database-postgres
./instant package status -n redis

# Deploy if needed
./instant package init -n database-postgres -d
./instant package init -n redis -d
```

## Deployment

### 1. Deploy Agenta Package

From the repository root:

```bash
./instant package init -n agenta -d
```

This will:
- Initialize the Agenta database in postgres-1
- Start agenta-backend API service (port 8001)
- Start agenta-frontend UI (port 8002)
- Start agenta-redis for caching

### 2. Verify Deployment

```bash
# Check API health
curl http://localhost:8001/api/v1/health

# Access web UI
open http://localhost:8002
```

You should see the Agenta welcome screen.

### 3. Configure LM Studio Models

This is a **one-time setup** to connect Agenta with your LM Studio instance:

1. Open Agenta UI: `http://localhost:8002`
2. Navigate to **Models** → **Add Custom Model**
3. Add each model used by your agents:

**Router/Orchestrator Model:**
- Name: `meta-llama-3.1-8b-instruct`
- Provider: `Custom`
- Base URL: `http://host.docker.internal:1234/v1`
- API Key: (leave empty for LM Studio)

**Medical Model:**
- Name: `medgemma-4b-it`
- Provider: `Custom`
- Base URL: `http://host.docker.internal:1234/v1`

**Clinical Model:**
- Name: `gemma-3-4b-it`
- Provider: `Custom`
- Base URL: `http://host.docker.internal:1234/v1`

4. Test each model in the playground to verify connectivity

### 4. Migrate Existing Prompts

Migrate your YAML prompts to Agenta:

```bash
cd projects/med-agent-hub

# Preview migration (dry run)
poetry run python -m server.prompt_management.migrate_to_agenta --dry-run

# Migrate all agents
poetry run python -m server.prompt_management.migrate_to_agenta

# Migrate specific agent only
poetry run python -m server.prompt_management.migrate_to_agenta --agent router

# Force overwrite if re-migrating
poetry run python -m server.prompt_management.migrate_to_agenta --force
```

This creates prompts in Agenta with names like:
- `router-system-prompt-template`
- `medical-system-prompt`
- `clinical-skill-population-analytics`
- etc.

### 5. Deploy/Restart Med Agent Hub

```bash
./instant package init -n med-agent-hub -d
```

Agents will now load prompts from Agenta (with YAML fallback).

## Rapid Iteration Workflow

### Edit Prompts

1. Open Agenta UI: `http://localhost:8002/prompts`
2. Select a prompt (e.g., `router-system-prompt-template`)
3. Click **Edit**
4. Modify the prompt text
5. Click **Save** to create a new version

### Test in Playground

1. In Agenta UI, navigate to **Playground**
2. Select your model (e.g., `meta-llama-3.1-8b-instruct`)
3. Select your prompt version
4. Enter test inputs for template variables
5. Click **Run** to test with your LM Studio model
6. Compare different prompt versions side-by-side

### Deploy to Agents

**Option 1: Wait for cache TTL (5 minutes)**
- Agents automatically pick up new prompts after cache expires
- No restart needed

**Option 2: Restart agents immediately**
```bash
cd projects/med-agent-hub
poetry run honcho -f Procfile.dev start
```

**Option 3: Clear cache programmatically**
```python
from server.prompt_management.loader import PromptLoader
loader = PromptLoader()
loader.reload_cache()
```

### Test Changes

Test your changes through the chat UI:

```bash
# Access chat UI
open http://localhost:8091

# Or test via API
curl -X POST http://localhost:3000/chat \
  -H "Content-Type: application/json" \
  -d '{"prompt": "What are symptoms of diabetes?"}'
```

## Environment Management

Agenta supports multiple environments for different stages:

- **development** (default) - Active experimentation
- **staging** - Testing before production
- **production** - Stable prompts for production use

### Change Environment

Update `env.recommended` or set environment variable:

```env
AGENTA_ENVIRONMENT=staging
```

Restart agents to pick up the new environment.

## Version Control and Rollback

### View Version History

1. Open Agenta UI: `http://localhost:8002/prompts`
2. Select a prompt
3. Click **Versions** tab
4. See full history of changes with timestamps

### Rollback to Previous Version

1. In the Versions tab, select a previous version
2. Click **Deploy to [environment]**
3. Agents will pick up the older version after cache TTL

## Configuration Options

### Backend Selection

Control prompt source via `PROMPT_BACKEND` environment variable:

```env
# Try Agenta first, fall back to YAML if unavailable (default)
PROMPT_BACKEND=auto

# Always use Agenta (fail if unavailable)
PROMPT_BACKEND=agenta

# Always use YAML files (ignore Agenta)
PROMPT_BACKEND=yaml
```

### Cache TTL

Control how often agents check for prompt updates:

```env
# Check every 5 minutes (default)
PROMPT_CACHE_TTL=300

# Check every minute (faster iteration)
PROMPT_CACHE_TTL=60

# Disable caching (always fetch from source)
PROMPT_CACHE_TTL=0
```

## Troubleshooting

### Agenta UI Not Accessible

```bash
# Check if Agenta services are running
docker ps | grep agenta

# Check logs
docker logs <container-id>

# Restart Agenta package
./instant package down -n agenta
./instant package up -n agenta -d
```

### Agents Not Loading New Prompts

1. **Check cache TTL**: Wait for cache to expire (default: 5 min)
2. **Restart agents**: Force reload by restarting
3. **Check logs**: Look for "Loaded prompt from agenta" messages
4. **Verify environment**: Ensure `AGENTA_ENVIRONMENT` matches your Agenta UI

### Database Connection Issues

```bash
# Check postgres is running
./instant package status -n database-postgres

# Check Agenta database exists
docker exec -it <postgres-container> psql -U postgres -l | grep agenta

# Re-run database initialization
./instant package destroy -n agenta
./instant package init -n agenta -d
```

### Fallback to YAML

If Agenta is having issues, temporarily disable it:

```bash
export PROMPT_BACKEND=yaml
cd projects/med-agent-hub
poetry run honcho -f Procfile.dev start
```

Agents will use YAML files exclusively until you change `PROMPT_BACKEND` back to `auto`.

## Testing Integration

Run integration tests to verify Agenta setup:

```bash
cd projects/med-agent-hub
poetry run python tests/test_agenta_integration.py
```

Expected output:
- ✅ Agenta Health Check
- ✅ Prompt Fetching
- ✅ Cache Behavior
- ✅ YAML Fallback
- ✅ Backend Modes

## Implementation Details

### Files Created

**Agenta Package (`packages/agenta/`):**
- Complete Instant OpenHIE package with postgres and redis dependencies
- Database initialization via `importer/create-agenta-db.js`
- Shares postgres-1 database (no separate postgres instance needed)
- Standard deployment via `./instant package init -n agenta -d`

**Prompt Management Module (`server/prompt_management/`):**
- `agenta_client.py` - API client with caching (300s TTL default)
- `loader.py` - Unified loader with automatic Agenta→YAML fallback
- `migrate_to_agenta.py` - CLI migration tool with dry-run support

**Agent Executor Updates:**
All 5 executors updated to use PromptLoader:
- `router_executor.py` - Router agent
- `react_router_executor.py` - ReAct router
- `medical_executor.py` - Medical agent
- `clinical_executor_v2.py` - Clinical agent
- `administrative_executor.py` - Administrative agent

### Prompt Naming Convention

YAML structure maps to Agenta names:
- `router.system_prompt_template` → `router-system-prompt-template`
- `medical.system_prompt` → `medical-system-prompt`
- `clinical.skill_prompts.population_analytics` → `clinical-skill-population-analytics`
- `administrative.prompts.route_action` → `administrative-route-action`

Total: 11 prompts migrated from YAML to Agenta

### Backend Selection Logic

1. Check `PROMPT_BACKEND` environment variable
2. If `agenta` or `auto`: Try Agenta API with caching
3. If successful: Return cached prompt
4. If failed or `yaml` mode: Load from YAML file
5. Log which backend provided each prompt

## Best Practices

1. **Always test in Agenta playground** before deploying to agents
2. **Use environments** - develop in `development`, promote to `production`
3. **Tag prompts** with metadata for organization
4. **Document template variables** in prompt descriptions
5. **Keep YAML files** - They serve as fallback and source of truth
6. **Version everything** - Never delete old versions, just create new ones
7. **Monitor logs** - Check which backend is serving prompts
8. **Test fallback** - Periodically verify agents work with `PROMPT_BACKEND=yaml`

## Package Information

**Agenta Package Details:**
- **Location**: `packages/agenta/`
- **Dependencies**: `database-postgres`, `redis`
- **Services**: agenta-backend, agenta-frontend, agenta-redis
- **Ports**: 8001 (API), 8002 (Web UI)
- **Database**: Shares postgres-1 (creates `agenta` database)
- **Deployment**: Standard Instant OpenHIE via `swarm.sh`

**Integration Points:**
- Med-agent-hub connects via `AGENTA_API_URL`
- All executors use `PromptLoader` singleton
- Automatic fallback to YAML if Agenta unavailable
- Zero downtime updates via cache TTL

## Next Steps

- Review [Agent Reference](architecture/agents.md) for agent-specific prompts
- See [Development Setup](development/dev-setup.md) for local development
- Check [Creating Agents](development/creating-agents.md) for adding new agents with prompts
- Explore [Agenta Package README](../../../packages/agenta/README.md) for package details

