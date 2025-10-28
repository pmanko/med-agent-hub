# Agent Reference

## Available Agents

| Agent | Port | Purpose | Skill |
|-------|------|---------|-------|
| **Router** | 9100 | Query orchestration | `route_query` |
| **MedGemma** | 9101 | Medical Q&A | `answer_medical_question` |
| **Clinical** | 9102 | Clinical research | `clinical_research` |

## Router Agent

**Purpose**: Orchestrates queries to appropriate specialists using LLM analysis.

**Configuration**:
- Model: `ORCHESTRATOR_MODEL` (default: meta-llama-3.1-8b-instruct)
- Supports "simple" and "react" orchestration modes
- Falls back to MedGemma if routing fails

**Process**:
1. Analyzes query content with LLM
2. Returns JSON with agent selection and reasoning
3. Forwards to selected specialist
4. Returns aggregated response

## MedGemma Agent  

**Purpose**: Provides evidence-based medical information with appropriate disclaimers.

**Configuration**:
- Model: `MED_MODEL` (default: medgemma-4b-it-mlx)
- Temperature: 0.1 for consistency
- Max tokens: 1000

**Features**:
- Patient-friendly language
- Automatic medical disclaimers
- No diagnosis or prescriptions
- Recommends professional consultation

**Example Query**: "What are the symptoms of diabetes?"

## Clinical Research Agent

**Purpose**: Answers clinical research questions with statistical insights.

**Configuration**:
- Model: `CLINICAL_RESEARCH_MODEL` (default: gemma-3-1b-it)
- Temperature: 0.3 for balanced responses
- Max tokens: 1500

**Expertise Areas**:
- Clinical trial design
- Epidemiological research
- Statistical analysis
- Medical literature review
- Clinical guidelines

**Example Query**: "What are common endpoints in oncology trials?"

## Agent Communication

### Message Format
```json
{
  "jsonrpc": "2.0",
  "method": "send_message",
  "params": {
    "message": {
      "message_id": "uuid",
      "role": "user",
      "parts": [{
        "type": "text",
        "text": "Query content"
      }]
    }
  }
}
```

### Routing Decision
The router uses this prompt pattern:
```
Available agents:
- medgemma: Medical Q&A
- clinical: Research analysis

Analyze query and respond with:
{"agent": "name", "reasoning": "why"}
```

## Prompt Management

All agent prompts are managed through a **dual-source system**:

### Primary: Agenta Web UI
- **Location**: `http://localhost:8002/prompts` (when Agenta package deployed)
- **Features**: Version control, playground testing, collaboration, A/B testing
- **Update flow**: Edit in UI → Test in playground → Auto-deploy to agents

### Fallback: YAML Files
- **Location**: `server/agent_configs/*.yaml`
- **Purpose**: Source of truth, fallback when Agenta unavailable
- **Update flow**: Edit YAML → Migrate to Agenta → Commit to git

### Prompt Types by Agent

**Router Agent:**
- `system_prompt_template` - Simple routing logic
- `react_system_prompt_template` - Multi-step ReAct orchestration

**Medical Agent:**
- `system_prompt` - Medical Q&A guidelines and disclaimers

**Clinical Agent:**
- `skill_routing_prompt_template` - Skill selection logic
- `skill_prompts.*` - Individual skill-specific prompts (4 skills)

**Administrative Agent:**
- `prompts.*` - Action routing and parameter extraction prompts

See [Prompt Management Guide](../prompt-management.md) for detailed workflow.

## Adding New Agents

New agents require three components:

1. **Agent Card** (`server/agent_cards/new_agent.json`)
2. **Executor** (`server/sdk_agents/new_executor.py`)
3. **Server** (`server/sdk_agents/new_server.py`)

Then update the router's agent registry and add to `Procfile.dev`.

**Prompts**: Add to both:
- `server/agent_configs/new_agent.yaml` (for fallback)
- Agenta UI (via migration script or manual creation)

See [Creating Agents](../development/creating-agents.md) for implementation details.