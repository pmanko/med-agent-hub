"""
Clinical Research Agent Executor using A2A SDK
Handles clinical data queries and research questions
"""

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import (
    AgentCard,
    AgentCapabilities,
    AgentSkill,
    Part,
    TextPart,
    Task,
    TaskState,
    UnsupportedOperationError,
)
from a2a.utils import new_agent_text_message, new_task
from a2a.utils.errors import ServerError
import httpx
import logging
import os
import json

from .router_executor import load_agent_config

logger = logging.getLogger(__name__)


class ClinicalExecutor(AgentExecutor):
    """Clinical research agent executor"""
    
    def __init__(self):
        config = load_agent_config('clinical')
        
        self.llm_base_url = os.getenv("LLM_BASE_URL", "http://localhost:1234")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.general_model = os.getenv("CLINICAL_RESEARCH_MODEL", config.get('model'))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self.http_client = httpx.AsyncClient(timeout=180.0)
        
        self.skill_routing_prompt_template = config.get('skill_routing_prompt_template', '')
        self.skills = {skill['id']: skill for skill in config.get('card', {}).get('skills', [])}
        
        # Clinical data sources (optional)
        self.fhir_base_url = os.getenv("OPENMRS_FHIR_BASE_URL")
        self.fhir_username = os.getenv("OPENMRS_USERNAME")
        self.fhir_password = os.getenv("OPENMRS_PASSWORD")
        
        logger.info(f"Clinical executor initialized with model: {self.general_model}")
    
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Execute clinical research request"""
        query = context.get_user_input()
        task = context.current_task
        
        # Create a new task if none exists
        if not task:
            task = new_task(context.message)
            await event_queue.enqueue_event(task)
        
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        
        try:
            # First, determine which skill to use based on the query.
            skills_info = "\n".join([f"- {name}: {skill['description']}" for name, skill in self.skills.items()])
            skill_choice_prompt = self.skill_routing_prompt_template.format(
                skills_info=skills_info,
                query=query
            )
            
            skill_messages = [{"role": "system", "content": "You are a helpful assistant that chooses the best skill for a query."}, {"role": "user", "content": skill_choice_prompt}]
            skill_response_raw = await self._call_llm(skill_messages, max_tokens=50) # Short response needed
            
            # Clean the response to handle potential markdown code blocks
            cleaned_response = skill_response_raw.strip().removeprefix("```json").removesuffix("```").strip()
            skill_choice = json.loads(cleaned_response).get("skill", "general_knowledge")

            logger.info(f"[Task {task.id}] Determined skill for query '{query[:30]}...': {skill_choice}")

            if skill_choice in self.skills:
                system_prompt = self.skills[skill_choice].get('system_prompt', self._get_general_knowledge_prompt())
                artifact_name = self.skills[skill_choice].get('id', 'general_knowledge_response')
                status_message = f"Executing skill: {skill_choice}..."
            else: # Default to general knowledge
                system_prompt = self._get_general_knowledge_prompt()
                artifact_name = "general_knowledge_response"
                status_message = "Answering general knowledge question..."

            await updater.update_status(
                TaskState.working,
                new_agent_text_message(status_message, task.context_id, task.id)
            )
            
            # Call LLM with the chosen persona
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ]
            answer = await self._call_llm(messages)
            
            if not answer:
                answer = "I apologize, but I couldn't generate a response. Please try rephrasing your question."
            
            # Add the response as an artifact with the correct name
            await updater.add_artifact(
                [Part(root=TextPart(text=answer))],
                name=artifact_name
            )
            
            # Complete the task
            logger.info(f"[Task {task.id}] Task completed successfully using skill: {skill_choice}.")
            await updater.complete()
            
        except httpx.HTTPStatusError as e:
            logger.error(f"[Task {task.id}] HTTP error calling LLM: {e}")
            error_msg = f"Error processing clinical research question: {str(e)}"
            await updater.update_status(
                TaskState.failed,
                new_agent_text_message(error_msg, task.context_id, task.id)
            )
            
        except Exception as e:
            logger.error(f"[Task {task.id}] Error processing clinical query: {e}", exc_info=True)
            error_msg = f"Error processing clinical research question: {str(e)}"
            await updater.update_status(
                TaskState.failed,
                new_agent_text_message(error_msg, task.context_id, task.id)
            )
    
    async def _call_llm(self, messages: list, max_tokens: int = 1500) -> str:
        """Helper to call the LLM."""
        headers = {"Content-Type": "application/json"}
        if self.llm_api_key:
            headers["Authorization"] = f"Bearer {self.llm_api_key}"
        
        request_data = {
            "model": self.general_model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": max_tokens,
        }
        
        response = await self.http_client.post(
            f"{self.llm_base_url}/v1/chat/completions",
            headers=headers,
            json=request_data
        )
        response.raise_for_status()
        
        result = response.json()
        return result.get("choices", [{}])[0].get("message", {}).get("content", "")

    def _get_general_knowledge_prompt(self) -> str:
        """Returns the system prompt for general queries."""
        return self.skills.get('general_knowledge', {}).get('system_prompt', "You are a helpful general-purpose assistant.")

    def _get_clinical_research_prompt(self) -> str:
        """Returns the system prompt for the clinical research persona."""
        prompt = self.skills.get('clinical_research', {}).get('system_prompt', "")
        
        if self.fhir_base_url:
            prompt += "\n\nNote: You have access to clinical data from a FHIR server, though direct queries are not implemented in this demo."
        return prompt

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> Task | None:
        """Cancel is not supported for this agent"""
        raise ServerError(error=UnsupportedOperationError(
            message="Cancel operation is not supported for Clinical agent"
        ))
    
    async def cleanup(self):
        """Clean up resources"""
        await self.http_client.aclose()
        logger.info("Clinical executor cleanup completed")

    def get_agent_card(self) -> AgentCard:
        """Return agent capabilities for A2A discovery."""
        return AgentCard(
            name="Clinical Research Agent",
            description="Provides clinical research analysis and general clinical answers",
            url="http://localhost:9102/",
            version="1.0.0",
            default_input_modes=["text", "text/plain"],
            default_output_modes=["text", "text/plain"],
            capabilities=AgentCapabilities(streaming=True),
            skills=[
                AgentSkill(
                    id="general_knowledge",
                    name="general_knowledge",
                    description="Answers general knowledge questions and provides information on a wide variety of topics.",
                    tags=["general", "q&a"],
                    input_schema={
                        "type": "object",
                        "properties": { "query": { "type": "string", "description": "The general knowledge question to be answered." }},
                        "required": ["query"],
                    },
                    output_schema={
                        "type": "object",
                        "properties": { "response": { "type": "string", "description": "The answer to the question." }},
                    },
                ),
                AgentSkill(
                    id="clinical_research",
                    name="clinical_research",
                    description="Retrieve and analyze clinical data with scope-based access",
                    tags=["clinical", "research"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string", "description": "Clinical question"},
                            "scope": {
                                "type": "string",
                                "enum": ["facility", "hie"],
                                "default": "hie",
                            },
                            "facility_id": {"type": "string"},
                            "org_ids": {"type": "array", "items": {"type": "string"}},
                            "data_source": {
                                "type": "string",
                                "enum": ["auto", "fhir", "sql"],
                                "default": "auto",
                            },
                        },
                        "required": ["query"],
                    },
                    output_schema={
                        "type": "object",
                        "properties": {
                            "response": {"type": "string"},
                            "data_source": {"type": "string"},
                            "scope": {"type": "string"},
                            "records_found": {"type": "integer"},
                            "query_executed": {"type": "string"},
                        },
                    },
                )
            ],
        )
