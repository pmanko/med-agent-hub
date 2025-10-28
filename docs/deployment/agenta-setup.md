# Agenta Setup Guide

Quick reference for deploying and configuring Agenta prompt management.

## Prerequisites

Ensure these packages are deployed first:

```bash
./instant package init -n database-postgres -d
./instant package init -n redis -d
```

## Deployment Steps

### 1. Deploy Agenta Package

```bash
# From repository root
./instant package init -n agenta -d
```

Wait for services to start (30-60 seconds).

### 2. Verify Deployment

```bash
# Check API health
curl http://localhost:8001/api/v1/health
# Expected: {"status": "healthy"}

# Check web UI
curl http://localhost:8002
# Expected: HTML response

# List running containers
docker ps | grep agenta
# Expected: agenta-backend, agenta-frontend, agenta-redis
```

### 3. Configure LM Studio Models (One-Time Setup)

Open Agenta UI: `http://localhost:8002`

Navigate to **Models** → **Add Custom Model**:

**Model 1: Router/Orchestrator**
```
Name: meta-llama-3.1-8b-instruct
Provider: Custom
Base URL: http://host.docker.internal:1234/v1
API Key: (leave empty)
```

**Model 2: Medical**
```
Name: medgemma-4b-it
Provider: Custom
Base URL: http://host.docker.internal:1234/v1
API Key: (leave empty)
```

**Model 3: Clinical**
```
Name: gemma-3-4b-it
Provider: Custom
Base URL: http://host.docker.internal:1234/v1
API Key: (leave empty)
```

Click **Test** for each model to verify LM Studio connectivity.

### 4. Migrate Existing Prompts

```bash
cd projects/med-agent-hub

# Preview migration (recommended first time)
poetry run python -m server.prompt_management.migrate_to_agenta --dry-run

# Perform migration
poetry run python -m server.prompt_management.migrate_to_agenta
```

Expected output:
```
Migration Summary
==================
Total prompts processed: 11
Created/Updated: 11
Skipped: 0
Failed: 0

✅ Migration completed successfully!
```

### 5. Verify Prompts in Agenta

Open `http://localhost:8002/prompts` and verify you see:
- router-system-prompt-template
- router-react-system-prompt-template
- medical-system-prompt
- clinical-skill-routing-prompt-template
- clinical-skill-population-analytics
- clinical-skill-patient-longitudinal
- clinical-skill-fhir-search
- clinical-skill-medical-search
- administrative-route-action
- administrative-extract-review-params
- administrative-extract-schedule-params

### 6. Deploy Med Agent Hub

```bash
# Via Instant OpenHIE
./instant package init -n med-agent-hub -d

# OR via local development
cd projects/med-agent-hub
poetry run honcho -f Procfile.dev start
```

Check agent logs for:
```
Loaded prompt 'router.system_prompt_template' from agenta (XXX chars)
```

## First Prompt Edit

### Test the System

1. Open Agenta UI: `http://localhost:8002/prompts`
2. Click on `medical-system-prompt`
3. Click **Edit**
4. Add a line at the end: `Always mention the importance of regular check-ups.`
5. Click **Save**
6. Note the new version number

### Test in Playground

1. Go to **Playground** tab
2. Select model: `medgemma-4b-it`
3. Select prompt: `medical-system-prompt` (latest version)
4. Enter test query: `What are symptoms of diabetes?`
5. Click **Run**
6. Verify the response includes your new line

### Deploy to Agents

**Option A: Wait 5 minutes** (cache TTL expires automatically)

**Option B: Restart agents immediately**
```bash
cd projects/med-agent-hub
# Stop current agents (Ctrl+C)
poetry run honcho -f Procfile.dev start
```

### Verify Change

Test through chat UI:
```bash
open http://localhost:8091
# Ask: "What are symptoms of diabetes?"
# Response should include: "importance of regular check-ups"
```

## Rollback Test

If you need to revert:

1. Open `http://localhost:8002/prompts/medical-system-prompt`
2. Go to **Versions** tab
3. Select the previous version
4. Click **Deploy to development**
5. Wait for cache TTL or restart agents
6. Test again to verify old version restored

## Success Indicators

✅ Agenta UI accessible at http://localhost:8002  
✅ All 11 prompts visible in Agenta  
✅ LM Studio models tested in playground  
✅ Agent logs show "Loaded prompt from agenta"  
✅ Prompt edits reflected in agent responses  
✅ Rollback works correctly  

## Troubleshooting

See main [Prompt Management Guide](../prompt-management.md#troubleshooting) for detailed troubleshooting steps.

Quick fixes:
```bash
# Restart Agenta
./instant package down -n agenta
./instant package up -n agenta -d

# Use YAML fallback temporarily
export PROMPT_BACKEND=yaml
cd projects/med-agent-hub
poetry run honcho -f Procfile.dev start

# Check database
docker exec -it <postgres-container> psql -U agenta -d agenta
\dt  # List tables
```

