# Creating New Agents

## Overview

Each agent requires three components:
1. **Agent Card** (JSON) - Defines capabilities
2. **Executor** (Python) - Business logic
3. **Server** (Python) - HTTP server setup

## Implementation Steps

### 1. Create Agent Card
`server/agent_cards/pharmacy.json`:
```json
{
  "name": "Pharmacy Assistant",
  "description": "Handles medication queries",
  "url": "http://localhost:9103/",
  "version": "1.0.0",
  "preferredTransport": "jsonrpc",
  "skills": [{
    "id": "check_interactions",
    "name": "Check Drug Interactions",
    "description": "Analyze medication interactions"
  }]
}
```

### 2. Implement Executor
`server/sdk_agents/pharmacy_executor.py`:
```python
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import Part, TextPart, TaskState
from a2a.utils import new_agent_text_message, new_task
import httpx
import os

class PharmacyExecutor(AgentExecutor):
    def __init__(self):
        self.llm_base_url = os.getenv("LLM_BASE_URL")
        self.model = os.getenv("PHARMACY_MODEL", "llama-3-8b")
        self.http_client = httpx.AsyncClient(timeout=60.0)
    
    async def execute(self, context: RequestContext, event_queue: EventQueue):
        query = context.get_user_input()
        task = context.current_task or new_task(context.message)
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        
        try:
            # Update status
            await updater.update_status(
                TaskState.working,
                new_agent_text_message("Processing...", task.context_id, task.id)
            )
            
            # Call LLM
            response = await self.call_llm(query)
            
            # Add result
            await updater.add_artifact(
                [Part(root=TextPart(text=response))],
                name='pharmacy_response'
            )
            
            # Complete
            await updater.complete()
            
        except Exception as e:
            await updater.update_status(
                TaskState.failed,
                new_agent_text_message(f"Error: {e}", task.context_id, task.id)
            )
    
    async def call_llm(self, prompt: str) -> str:
        # LLM integration logic
        pass
    
    async def cancel(self, context: RequestContext, event_queue: EventQueue):
        # Optional cancellation
        pass
```

### 3. Create Server
`server/sdk_agents/pharmacy_server.py`:
```python
from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCard
from pathlib import Path
import json

def create_pharmacy_server():
    # Load agent card
    card_path = Path(__file__).parent.parent / 'agent_cards' / 'pharmacy.json'
    with open(card_path) as f:
        agent_card = AgentCard(**json.load(f))
    
    # Set runtime URL
    agent_card.url = "http://localhost:9103/"
    
    # Create server
    executor = PharmacyExecutor()
    handler = DefaultRequestHandler(executor, InMemoryTaskStore())
    server = A2AStarletteApplication(agent_card, handler)
    
    return server.build()

app = create_pharmacy_server()
```

### 4. Update Router

Add to `router_executor.py`:
```python
self.agents = {
    "medgemma": {...},
    "clinical": {...},
    "pharmacy": {
        "url": "http://localhost:9103",
        "name": "Pharmacy Assistant",
        "skills": ["check_interactions"]
    }
}
```

### 5. Add to Procfile.dev
```yaml
pharmacy: poetry run uvicorn server.sdk_agents.pharmacy_server:app --port 9103 --env-file $UVICORN_ENV_FILE
```

## Key Patterns

### LLM Integration
```python
async def call_llm(self, prompt: str) -> str:
    response = await self.http_client.post(
        f"{self.llm_base_url}/v1/chat/completions",
        json={
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3
        }
    )
    return response.json()["choices"][0]["message"]["content"]
```

### Error Handling
Always update task status on errors:
```python
except Exception as e:
    await updater.update_status(
        TaskState.failed,
        new_agent_text_message(f"Error: {str(e)}", task.context_id, task.id)
    )
```

## Testing

```python
# Test agent card
curl http://localhost:9103/.well-known/agent-card.json

# Test with A2A client
from a2a.client import ClientFactory
# ... (see test_models_direct.py for pattern)
```

## Best Practices

1. **Always use TaskUpdater** for state management
2. **Add meaningful artifacts** for all responses
3. **Handle errors gracefully** with status updates
4. **Load configuration from environment**
5. **Test with the SDK client** for compliance

## Next Steps

1. Add environment variables to `env.recommended`
2. Test with `test_models_direct.py`
3. Update documentation if adding new patterns
4. See Development Setup (docs/development/dev-setup.md) for environment and ports