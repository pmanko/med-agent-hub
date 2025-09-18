"""
Administrative Agent Executor for appointment management
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
    TaskState,
    UnsupportedOperationError,
)
from a2a.utils import new_agent_text_message, new_task
from a2a.utils.errors import ServerError
import httpx
import logging
import os
import json
from typing import Dict, Any, Optional

# Import MCP tools
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from mcp.base import MCPToolRegistry
from mcp.appointment_tool import AppointmentTool

logger = logging.getLogger(__name__)


class AdministrativeExecutor(AgentExecutor):
    """Administrative tasks executor for healthcare operations"""
    
    def __init__(self):
        # LLM configuration
        self.llm_base_url = os.getenv("LLM_BASE_URL", "http://localhost:1234")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.model = os.getenv("ADMIN_MODEL", os.getenv("GENERAL_MODEL", "llama-3-8b-instruct"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self.http_client = httpx.AsyncClient(timeout=180.0)
        
        # Initialize MCP tool registry
        self.tool_registry = MCPToolRegistry()
        self._register_tools()
        
        logger.info(f"Administrative executor initialized with model: {self.model}")
    
    def _register_tools(self):
        """Register administrative MCP tools"""
        
        # Appointment tool
        appointment_config = {
            'rest_base_url': os.getenv('OPENMRS_REST_BASE_URL', 
                                      os.getenv('OPENMRS_FHIR_BASE_URL', '').replace('/ws/fhir2/R4', '/ws/rest/v1')),
            'username': os.getenv('OPENMRS_USERNAME'),
            'password': os.getenv('OPENMRS_PASSWORD')
        }
        
        self.tool_registry.register(AppointmentTool(appointment_config))
        logger.info("Registered appointment management tool")
    
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Execute administrative request"""
        query = context.get_user_input()
        task = context.current_task
        
        # Create a new task if none exists
        if not task:
            task = new_task(context.message)
            await event_queue.enqueue_event(task)
        
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        
        try:
            # Determine which skill to use
            skill_name = await self._determine_skill(query)
            logger.info(f"[Task {task.id}] Determined skill: {skill_name}")
            
            await updater.update_status(
                TaskState.working,
                new_agent_text_message(f"Processing {skill_name} request...", task.context_id, task.id)
            )
            
            # Execute the skill
            if skill_name == "review_appointments":
                result = await self._handle_review_appointments(query)
            elif skill_name == "schedule_appointment":
                result = await self._handle_schedule_appointment(query)
            else:
                result = "I can help with reviewing or scheduling appointments. Please specify what you need."
            
            # Add result as artifact
            await updater.add_artifact(
                [Part(root=TextPart(text=result))],
                name=f"{skill_name}_response"
            )
            
            # Complete the task
            logger.info(f"[Task {task.id}] Task completed successfully")
            await updater.complete()
            
        except Exception as e:
            logger.error(f"[Task {task.id}] Error processing administrative request: {e}", exc_info=True)
            await updater.update_status(
                TaskState.failed,
                new_agent_text_message(f"Error: {str(e)}", task.context_id, task.id)
            )
    
    async def _determine_skill(self, query: str) -> str:
        """Determine which administrative skill to use"""
        
        routing_prompt = f"""Determine the administrative action for this query.

Available actions:
- review_appointments: Check existing appointments, view schedule
- schedule_appointment: Book a new appointment

Query: {query}

Respond with JSON: {{"action": "review_appointments" or "schedule_appointment"}}"""
        
        messages = [
            {"role": "system", "content": "You classify administrative healthcare requests."},
            {"role": "user", "content": routing_prompt}
        ]
        
        response = await self._call_llm(messages, max_tokens=50)
        
        try:
            cleaned = response.strip().removeprefix("```json").removesuffix("```").strip()
            result = json.loads(cleaned)
            return result.get("action", "review_appointments")
        except:
            # Default to review if we can't determine
            if "schedule" in query.lower() or "book" in query.lower():
                return "schedule_appointment"
            return "review_appointments"
    
    async def _handle_review_appointments(self, query: str) -> str:
        """Handle appointment review requests"""
        
        # Extract parameters from query
        param_prompt = f"""Extract appointment review parameters from this query.

Query: {query}

Extract:
- patient_id (if mentioned)
- date range (start_date, end_date)
- status filter (scheduled, completed, cancelled)

Respond with JSON: {{"patient_id": "...", "filters": {{"start_date": "YYYY-MM-DD", ...}}}}"""
        
        messages = [
            {"role": "system", "content": "You extract appointment search parameters."},
            {"role": "user", "content": param_prompt}
        ]
        
        param_response = await self._call_llm(messages, max_tokens=200)
        
        try:
            cleaned = param_response.strip().removeprefix("```json").removesuffix("```").strip()
            params = json.loads(cleaned)
        except:
            params = {}
        
        # Invoke appointment tool
        tool = self.tool_registry.get_tool("appointment_manager")
        tool_params = {
            "action": "review",
            "patient_id": params.get("patient_id"),
            "filters": params.get("filters", {})
        }
        
        result = await tool.safe_invoke(tool_params)
        
        if not result.get("success"):
            return f"Failed to review appointments: {result.get('error')}"
        
        # Format the appointments for user
        appointments = result.get("result", {}).get("appointments", [])
        
        if not appointments:
            return "No appointments found matching your criteria."
        
        # Create a summary
        summary = f"Found {len(appointments)} appointment(s):\n\n"
        for apt in appointments:
            summary += f"• {apt.get('date', 'Date TBD')} - {apt.get('patient', 'Patient')} with {apt.get('provider', 'Provider')}\n"
            summary += f"  Service: {apt.get('service', 'General')}, Status: {apt.get('status', 'Unknown')}\n"
            if apt.get('reason'):
                summary += f"  Reason: {apt['reason']}\n"
            summary += "\n"
        
        return summary
    
    async def _handle_schedule_appointment(self, query: str) -> str:
        """Handle appointment scheduling requests"""
        
        # Extract scheduling parameters
        param_prompt = f"""Extract appointment scheduling details from this query.

Query: {query}

Extract:
- patient_id (required)
- date (YYYY-MM-DD format)
- time (HH:MM format)
- provider_uuid (if mentioned)
- service/type of appointment
- reason for visit
- location

If date/time not specific, suggest next available (use tomorrow 10:00 as default).

Respond with JSON: {{"patient_id": "...", "appointment_details": {{"date": "YYYY-MM-DD", "time": "HH:MM", ...}}}}"""
        
        messages = [
            {"role": "system", "content": "You extract appointment scheduling parameters."},
            {"role": "user", "content": param_prompt}
        ]
        
        param_response = await self._call_llm(messages, max_tokens=300)
        
        try:
            cleaned = param_response.strip().removeprefix("```json").removesuffix("```").strip()
            params = json.loads(cleaned)
        except Exception as e:
            return f"Could not parse scheduling details: {e}. Please provide patient ID, date, and time."
        
        if not params.get("patient_id"):
            return "Patient ID is required to schedule an appointment. Please specify the patient."
        
        if not params.get("appointment_details", {}).get("date"):
            return "Please specify a date for the appointment."
        
        # Invoke appointment tool
        tool = self.tool_registry.get_tool("appointment_manager")
        tool_params = {
            "action": "schedule",
            "patient_id": params["patient_id"],
            "appointment_details": params.get("appointment_details", {})
        }
        
        result = await tool.safe_invoke(tool_params)
        
        if not result.get("success"):
            return f"Failed to schedule appointment: {result.get('error')}"
        
        appointment_result = result.get("result", {})
        
        return f"""Appointment successfully scheduled:
- Patient: {params['patient_id']}
- Date: {params['appointment_details']['date']}
- Time: {params['appointment_details']['time']}
- Service: {params['appointment_details'].get('service', 'General Consultation')}
- Appointment ID: {appointment_result.get('appointment_id', 'Pending confirmation')}

{appointment_result.get('message', '')}"""
    
    async def _call_llm(self, messages: list, max_tokens: int = 500) -> str:
        """Helper to call the LLM"""
        headers = {"Content-Type": "application/json"}
        if self.llm_api_key:
            headers["Authorization"] = f"Bearer {self.llm_api_key}"
        
        request_data = {
            "model": self.model,
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
    
    async def cancel(self, context: RequestContext, event_queue: EventQueue):
        """Cancel is not supported for this agent"""
        raise ServerError(error=UnsupportedOperationError(
            message="Cancel operation is not supported for Administrative agent"
        ))
    
    async def cleanup(self):
        """Clean up resources"""
        await self.http_client.aclose()
        # Clean up tool resources
        for tool in self.tool_registry.tools.values():
            if hasattr(tool, 'cleanup'):
                await tool.cleanup()
        logger.info("Administrative executor cleanup completed")
    
    def get_agent_card(self) -> AgentCard:
        """Return agent capabilities for A2A discovery"""
        return AgentCard(
            name="Administrative Assistant",
            description="Handles healthcare administrative tasks including appointment management",
            url=os.getenv("A2A_ADMIN_URL", "http://localhost:9103/"),
            version="1.0.0",
            default_input_modes=["text", "text/plain"],
            default_output_modes=["text", "text/plain"],
            capabilities=AgentCapabilities(streaming=False),
            skills=[
                AgentSkill(
                    id="review_appointments",
                    name="Review Appointments",
                    description="Check existing appointments and view schedules",
                    tags=["appointments", "schedule", "review"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"}
                        },
                        "required": ["query"]
                    }
                ),
                AgentSkill(
                    id="schedule_appointment",
                    name="Schedule Appointment",
                    description="Book new appointments for patients",
                    tags=["appointments", "booking", "schedule"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"}
                        },
                        "required": ["query"]
                    }
                )
            ]
        )
