"""
ReAct-based Router Agent Executor.
This executor manages a multi-step reasoning and delegation process to fulfill
complex user requests by orchestrating multiple specialist agents.
"""

import logging
import json
import os
import re
import uuid
import httpx
from typing import Dict, Any, List

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import TaskState, Part, TextPart, Message, Role
from a2a.utils import new_agent_text_message, new_task
from a2a.client import ClientFactory, ClientConfig, A2ACardResolver
from a2a.types import TransportProtocol

# Import the config loader from the simple router
from .router_executor import load_agent_config

logger = logging.getLogger(__name__)

MAX_ITERATIONS = 5  # Prevent infinite loops

class ReactRouterExecutor(AgentExecutor):
    """
    A ReAct-based orchestrator that performs multi-step reasoning to fulfill a
    user's request by delegating to a series of specialist agents.
    """

    def __init__(self):
        config = load_agent_config('router')
        self.llm_base_url = os.getenv("LLM_BASE_URL", "http://localhost:1234")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.orchestrator_model = os.getenv("ORCHESTRATOR_MODEL", config.get('model'))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
        self.http_client = httpx.AsyncClient(timeout=180.0)

        self.system_prompt_template = config.get('react_system_prompt_template')
        self.agents, self.tools = self._discover_agents_and_build_tools()
        logger.info("ReAct Router executor initialized.")
        logger.info(f"Discovered agents: {json.dumps(self.agents, indent=2)}")

    def _discover_agents_and_build_tools(self) -> (Dict[str, Any], List[Dict[str, Any]]):
        """Discovers agents and builds a list of tool definitions for the LLM."""
        import requests
        agent_urls = {
            "medical": os.getenv("A2A_MEDICAL_URL", os.getenv("A2A_MEDGEMMA_URL", "http://localhost:9101")),
            "generalist": os.getenv("A2A_CLINICAL_URL", "http://localhost:9102"),
        }
        discovered_agents = {}
        tool_definitions = []
        for name, url in agent_urls.items():
            try:
                card_url = f"{url.rstrip('/')}/.well-known/agent-card.json"
                response = requests.get(card_url, timeout=5)
                response.raise_for_status()
                card = response.json()
                
                agent_description = card.get("description", "No description available.")
                discovered_agents[name] = {
                    "url": url,
                    "name": card.get("name", f"{name} Agent"),
                    "description": agent_description,
                }
                
                tool_definitions.append({
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": agent_description,
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": f"A concise, focused query for the {card.get('name')} related to its specialty.",
                                }
                            },
                            "required": ["query"],
                        },
                    },
                })
            except requests.RequestException as e:
                logger.error(f"Failed to discover agent '{name}' at {url}: {e}")
                discovered_agents[name] = {"url": url, "name": f"{name.capitalize()} Agent (Unavailable)", "description": "N/A"}
        return discovered_agents, tool_definitions

    def _extract_json_from_response(self, response: str) -> Dict[str, Any]:
        """Extract and parse JSON from LLM response, handling various formats."""
        # Remove common markdown formatting
        cleaned = response.strip()
        cleaned = re.sub(r'^```json\s*', '', cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r'^```\s*$', '', cleaned, flags=re.MULTILINE)
        cleaned = cleaned.strip()
        
        # Split by lines and find JSON objects
        lines = cleaned.split('\n')
        valid_jsons = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                try:
                    parsed = json.loads(line)
                    valid_jsons.append(parsed)
                except json.JSONDecodeError:
                    continue
        
        if not valid_jsons:
            # Fallback to regex approach
            json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
            matches = re.findall(json_pattern, cleaned, re.DOTALL)
            
            for match in matches:
                try:
                    parsed = json.loads(match)
                    valid_jsons.append(parsed)
                except json.JSONDecodeError:
                    continue
        
        if not valid_jsons:
            raise ValueError("No JSON object found in response")
        
        # Always return the FIRST valid JSON object to ensure proper ReAct flow
        # The LLM should only generate one action at a time
        return valid_jsons[0]

    async def _call_llm(self, messages: list) -> Dict[str, Any]:
        """Calls the orchestrator LLM with tool definitions."""
        try:
            response = await self.http_client.post(
                f"{self.llm_base_url}/v1/chat/completions",
                json={
                    "model": self.orchestrator_model,
                    "messages": messages,
                    "tools": self.tools,
                    "tool_choice": "auto",
                    "temperature": self.temperature,
                    "max_tokens": 1000,
                },
                headers={"Authorization": f"Bearer {self.llm_api_key}"} if self.llm_api_key else {}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"LLM call failed: {e}", exc_info=True)
            return {"choices": [{"message": {"content": json.dumps({"final_answer": "I apologize, but I encountered an error while processing your request."})}}]}

    async def _call_agent(self, agent_name: str, query: str) -> Dict[str, Any]:
        """Calls a specialist agent and returns the result."""
        if agent_name not in self.agents or not self.agents[agent_name].get("url"):
            return {"agent_name": agent_name, "task_status": "failed", "response": f"Error: Agent '{agent_name}' is not a known or available agent."}

        try:
            agent_info = self.agents[agent_name]
            resolver = A2ACardResolver(self.http_client, agent_info["url"])
            agent_card = await resolver.get_agent_card()

            client_config = ClientConfig(httpx_client=self.http_client, supported_transports=[TransportProtocol.jsonrpc])
            client = ClientFactory(client_config).create(agent_card)
            
            message = Message(
                role=Role.user, 
                parts=[Part(root=TextPart(text=query))],
                messageId=str(uuid.uuid4())
            )
            
            final_task = None
            async for event in client.send_message(message):
                final_task = event[0] if isinstance(event, tuple) else event
            
            if final_task and getattr(final_task, 'artifacts', None):
                last_artifact = final_task.artifacts[-1]
                if last_artifact and getattr(last_artifact, 'parts', None):
                    text_part = last_artifact.parts[0].root
                    if isinstance(text_part, TextPart):
                        return {
                            "agent_name": agent_name,
                            "task_status": str(final_task.status.state),
                            "response": text_part.text,
                        }
            return {
                "agent_name": agent_name,
                "task_status": str(final_task.status.state) if final_task else "unknown",
                "response": "The agent completed the task but returned no textual response.",
            }
        except Exception as e:
            logger.error(f"Failed to call agent '{agent_name}': {e}", exc_info=True)
            return {
                "agent_name": agent_name,
                "task_status": "failed",
                "response": f"Error: Could not get a response from agent '{agent_name}'.",
            }

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Executes the ReAct loop."""
        query = context.get_user_input()
        task = context.current_task or new_task(context.message)
        if not context.current_task:
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)

        try:
            # The system prompt is now simpler and doesn't need agents_info
            system_prompt = self.system_prompt_template.format(query=query)
            
            history = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ]
            
            await updater.update_status(TaskState.working, new_agent_text_message("Starting ReAct orchestration...", task.context_id, task.id))

            # Track which tools have been called in this session
            called_agents = set()
            synthesis_requested = False

            # Simple heuristic to detect multi-intent (general + medical)
            ql = (query or "").lower()
            is_medical_intent = any(k in ql for k in ["heatstroke", "symptom", "medical", "prevention", "treatment", "risk", "hypertension", "health"])
            is_general_intent = any(k in ql for k in ["city", "cities", "weather", "climate", "hot", "heat", "us", "america", "list"])

            for i in range(MAX_ITERATIONS):
                logger.info(f"[ReAct Loop {i+1}/{MAX_ITERATIONS}] History: {history}")

                llm_response = await self._call_llm(history)
                
                # Handle cases where the response might be malformed
                if not llm_response.get("choices") or not llm_response["choices"][0]:
                    logger.error("Invalid LLM response structure.")
                    history.append({"role": "user", "content": "Observation: Invalid response from LLM. Please try again."})
                    continue

                response_message = llm_response["choices"][0]["message"]
                finish_reason = llm_response["choices"][0].get("finish_reason")

                # Append the assistant message (may include tool_calls)
                history.append(response_message)

                if finish_reason == "tool_calls" and response_message.get("tool_calls"):
                    tool_calls = response_message["tool_calls"]

                    for tool_call in tool_calls:
                        function_name = tool_call["function"]["name"]
                        try:
                            function_args = json.loads(tool_call["function"]["arguments"])
                            agent_query = function_args["query"]
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.error(f"Error parsing tool call arguments: {e}")
                            observation = {"status": "error", "message": "Invalid arguments for tool call."}
                        else:
                            logger.info(f"Action: Call {function_name} with '{agent_query}'")
                            await updater.update_status(TaskState.working, new_agent_text_message(f"Delegating to {function_name} agent...", task.context_id, task.id))
                            observation = await self._call_agent(function_name, agent_query)
                            called_agents.add(function_name)

                        # Truncate very long observations to keep context manageable
                        obs_text = json.dumps(observation)
                        if len(obs_text) > 3000:
                            obs_text = obs_text[:3000] + "... [truncated]"

                        history.append({
                            "role": "tool",
                            "tool_call_id": tool_call["id"],
                            "content": obs_text,
                        })
                    
                    # If multi-intent and we now have both tools, prompt the model to synthesize
                    has_med = "medical" in called_agents
                    has_gen = "generalist" in called_agents
                    if (is_medical_intent and is_general_intent) and has_med and has_gen and not synthesis_requested:
                        history.append({
                            "role": "user",
                            "content": "Synthesize a single, comprehensive final answer that integrates information from all tool results. Include concrete facts from the generalist (e.g., city examples if provided) and actionable medical prevention guidance. Be concise and avoid restating your plan."
                        })
                        synthesis_requested = True
                        continue
                    
                    # Otherwise, proceed to let the LLM decide the next step
                    continue

                elif response_message.get("content"):
                    # Gate finalization if the query appears multi-intent and not all relevant tools were used
                    needs_med = is_medical_intent and ("medical" not in called_agents)
                    needs_gen = is_general_intent and ("generalist" not in called_agents)
                    if needs_med or needs_gen:
                        missing = "medical" if needs_med else "generalist"
                        logger.info(f"Request appears multi-intent; prompting model to call missing tool: {missing}")
                        history.append({
                            "role": "user",
                            "content": f"Observation: Additional information needed. Use the {missing} tool next with a concise, focused query relevant to its specialty."
                        })
                        continue

                    # If multi-intent and both tools have been called but we haven't explicitly asked for synthesis yet, do so
                    has_med = "medical" in called_agents
                    has_gen = "generalist" in called_agents
                    if (is_medical_intent and is_general_intent) and has_med and has_gen and not synthesis_requested:
                        history.append({
                            "role": "user",
                            "content": "Synthesize a single, comprehensive final answer that integrates information from all tool results. Include concrete city examples and medical prevention guidance in one cohesive response."
                        })
                        synthesis_requested = True
                        continue

                    final_answer = response_message["content"]
                    logger.info(f"Final synthesized answer received: {final_answer}")
                    await updater.add_artifact([Part(root=TextPart(text=final_answer))], name='react_final_response')
                    await updater.complete()
                    return

                else:
                    logger.warning(f"[ReAct Loop {i+1}] No tool calls or content in response. Continuing.")
                    history.append({"role": "user", "content": "Observation: No action taken. Please decide on the next step."})
                    continue

            # If loop finishes, it means we hit max iterations
            logger.warning("Max iterations reached. Synthesizing final answer from history.")
            history.append({
                "role": "user",
                "content": "You have reached the maximum number of steps. Synthesize the information you have gathered so far into a final answer."
            })
            final_llm_response = await self._call_llm(history)
            final_answer = final_llm_response.get("choices", [{}])[0].get("message", {}).get("content", "Could not determine a final answer.")

            await updater.add_artifact([Part(root=TextPart(text=final_answer))], name='react_max_iterations_response')
            await updater.complete()

        except Exception as e:
            logger.error(f"ReAct Router execution failed: {e}", exc_info=True)
            await updater.update_status(
                state=TaskState.failed,
                message=new_agent_text_message(f"ReAct routing failed: {str(e)}", task.context_id, task.id)
            )

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Handle task cancellation for the ReAct router."""
        task_updater = TaskUpdater(event_queue, context.current_task.id if context.current_task else None, context.message.context_id if context.message else None)
        await task_updater.update_status(state=TaskState.cancelled, message=new_agent_text_message("ReAct routing task was cancelled.", context.message.context_id if context.message else None))
