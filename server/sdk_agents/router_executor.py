"""
Router Agent Executor for A2A SDK
Handles orchestration and routing to specialist agents.
"""

import httpx
import logging
import os
import uuid
from typing import Dict, Any, Optional
import json
import yaml
from pathlib import Path

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import (
    AgentCard,
    TextPart,
    Part,
    Task,
    TaskState,
    Message,
    Role,
    SendStreamingMessageSuccessResponse,
    TaskStatusUpdateEvent,
    TaskArtifactUpdateEvent,
)
from a2a.utils import new_agent_text_message, new_task
from a2a.client import ClientConfig, ClientFactory
from a2a.client.card_resolver import A2ACardResolver
from a2a.types import TransportProtocol

logger = logging.getLogger(__name__)


def load_agent_config(agent_name: str) -> Dict[str, Any]:
    """Loads agent configuration from a YAML file."""
    config_path = Path(__file__).resolve().parent.parent / 'agent_configs' / f'{agent_name}.yaml'
    with config_path.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f)


class RouterAgentExecutor(AgentExecutor):
    """Router Agent Executor - orchestrates other agents."""
    
    def __init__(self):
        config = load_agent_config('router')
        
        self.llm_base_url = os.getenv("LLM_BASE_URL", "http://localhost:1234")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.orchestrator_model = os.getenv("ORCHESTRATOR_MODEL", config.get('model'))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self.http_client = httpx.AsyncClient(timeout=180.0)
        
        self.system_prompt_template = config.get('system_prompt_template', '')
        
        # Agent registry - dynamically discover agents and their skills
        self.agents = self._discover_agents()
        
        logger.info(f"Router agent executor initialized with model: {self.orchestrator_model}")
        logger.info(f"Discovered agents: {json.dumps(self.agents, indent=2)}")
    
    def _discover_agents(self) -> Dict[str, Any]:
        """Synchronously discover agents and their skills from their cards."""
        import requests # Use synchronous requests for startup discovery
        
        agent_base_urls = {
            "medical": os.getenv("A2A_MEDICAL_URL", os.getenv("A2A_MEDGEMMA_URL", "http://localhost:9101")),
            "generalist": os.getenv("A2A_CLINICAL_URL", "http://localhost:9102"),
        }
        
        discovered_agents = {}
        for name, url in agent_base_urls.items():
            try:
                card_url = f"{url.rstrip('/')}/.well-known/agent-card.json"
                logger.info(f"Discovering agent '{name}' from {card_url}")
                response = requests.get(card_url, timeout=5)
                response.raise_for_status()
                card = response.json()
                
                discovered_agents[name] = {
                    "url": url,
                    "name": card.get("name", f"{name} Agent"),
                    "skills": [skill.get("id", skill.get("name")) for skill in card.get("skills", [])],
                }
            except requests.RequestException as e:
                logger.error(f"Failed to discover agent '{name}' at {url}: {e}")
                # Add a placeholder so the system can still run
                discovered_agents[name] = { "url": url, "name": f"{name.capitalize()} Agent (Unavailable)", "skills": [] }
        return discovered_agents
    
    async def _call_llm(self, messages: list[Dict[str, Any]]) -> str:
        """Call the LLM for routing decisions."""
        try:
            response = await self.http_client.post(
                f"{self.llm_base_url}/v1/chat/completions",
                json={
                    "model": self.orchestrator_model,
                    "messages": messages,
                    "temperature": self.temperature,
                    "max_tokens": 500
                },
                headers={"Authorization": f"Bearer {self.llm_api_key}"} if self.llm_api_key else {}
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return "Error analyzing query"
    
    async def _route_query(self, query: str) -> Dict[str, Any]:
        """Determine which agent should handle the query."""
        
        logger.info("Router: Building capabilities list for orchestrator.")
        # Create routing prompt
        agents_info = "\n".join([
            f"- {name}: {info['name']} (skills: {', '.join(info['skills'])})"
            for name, info in self.agents.items()
        ])
        
        system_prompt = self.system_prompt_template.format(agents_info=agents_info)
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]
        
        logger.info("Router: Calling orchestrator LLM for routing decision...")
        response = await self._call_llm(messages)
        logger.info(f"Router: Received raw response from orchestrator: {response}")
        
        try:
            # Parse LLM response
            result = json.loads(response)
            agent_name = result.get("agent", "medical")
            reasoning = result.get("reasoning", "")
            
            if agent_name not in self.agents:
                logger.warning(f"Router: LLM returned an unknown agent '{agent_name}'. Falling back to default.")
                agent_name = "medical"  # Default fallback
                
            return {
                "agent": agent_name,
                "reasoning": reasoning,
                "url": self.agents[agent_name]["url"]
            }
        except Exception:
            logger.error(f"Router: Failed to parse JSON from orchestrator response. Falling back to keyword routing. Response: {response}")
            # Default to medical agent on parse error
            return {
                "agent": "medical",
                "reasoning": "Default routing to medical agent due to orchestrator failure",
                "url": self.agents["medical"]["url"]
            }
    
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Execute routing logic."""
        
        query = context.get_user_input()
        logger.info(f"Router received query: {query}")
        
        task = context.current_task
        if not task:
            task = new_task(context.message)
            await event_queue.enqueue_event(task)
        
        task_updater = TaskUpdater(event_queue, task.id, task.context_id)
        
        try:
            await task_updater.update_status(
                TaskState.working,
                new_agent_text_message(
                    "Analyzing query and routing to appropriate agent...",
                    task.context_id,
                    task.id,
                ),
            )
            
            logger.info(f"[Task {task.id}] Routing query: '{query}'")
            routing_result = await self._route_query(query)
            agent_name = routing_result['agent']
            agent_url = routing_result["url"]
            logger.info(f"[Task {task.id}] Decision: route to agent '{agent_name}' at {agent_url}. Reasoning: {routing_result['reasoning']}")
            
            resolver = A2ACardResolver(self.http_client, agent_url)
            agent_card = await resolver.get_agent_card()

            client_config = ClientConfig(
                httpx_client=self.http_client,
                supported_transports=[TransportProtocol.jsonrpc],
                use_client_preference=False,
            )
            client = ClientFactory(client_config).create(agent_card)

            message = Message(
                messageId=str(uuid.uuid4()),
                role=Role.user, 
                parts=[Part(root=TextPart(text=query))],
                metadata=context.message.metadata if context.message else None,
            )
            
            logger.info(f"[Task {task.id}] Forwarding message to {agent_name} and awaiting events...")
            final_task = None
            async for event in client.send_message(message):
                # The event can be a tuple, the first element is the task/update
                final_task = event[0] if isinstance(event, tuple) else event
                logger.info(f"[Task {task.id}] Received event from {agent_name}: {type(final_task).__name__}")

            # After the stream is done, process the final task state
            if final_task and getattr(final_task, 'artifacts', None):
                logger.info(f"[Task {task.id}] Final task received with {len(final_task.artifacts)} artifacts. Forwarding to parent.")
                # Forward the last artifact from the downstream agent
                last_artifact = final_task.artifacts[-1]
                await task_updater.add_artifact(last_artifact.parts, name=last_artifact.name)
            else:
                logger.warning(f"[Task {task.id}] Final task received from {agent_name} had no artifacts.")
                summary_text = f"Task completed by {agent_name}, but no response artifact was found."
                await task_updater.add_artifact([Part(root=TextPart(text=summary_text))])
            
            logger.info(f"[Task {task.id}] Completing router task.")
            await task_updater.complete()

        except Exception as e:
            logger.error(f"[Task {task.id}] Router execution failed: {e}", exc_info=True)
            await task_updater.update_status(
                TaskState.failed,
                new_agent_text_message(f"Routing failed: {str(e)}", task.context_id, task.id)
            )
    
    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        """Handle task cancellation."""
        task_updater = TaskUpdater(
            event_queue,
            context.current_task.id if context.current_task else None,
            context.message.context_id if context.message else None,
        )
        
        await task_updater.update_status(
            state=TaskState.cancelled,
            message="Query routing cancelled"
        )
    
    # Agent card is provided via JSON in server/agent_cards/router.json
    async def get_agent_card(self) -> AgentCard:
        """Return agent capabilities for A2A discovery."""
        # This is now handled by the server loading the JSON file.
        # This method is here to satisfy the abstract base class.
        pass
