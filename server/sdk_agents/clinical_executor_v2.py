"""
Enhanced Clinical Research Agent Executor with MCP Tool Integration
Handles clinical data queries using Spark, FHIR, and medical search tools
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
from typing import Dict, Any, Optional

# Import MCP tools
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from mcp.base import MCPToolRegistry
from mcp.spark_tools import SparkPopulationAnalyticsTool, SparkPatientLongitudinalTool
from mcp.fhir_tool import FHIRSearchTool
from mcp.medical_search_tool import MedicalSearchTool

from .router_executor import load_agent_config

logger = logging.getLogger(__name__)


class ClinicalExecutorV2(AgentExecutor):
    """Enhanced clinical research agent executor with MCP tool integration"""
    
    def __init__(self):
        config = load_agent_config('clinical')
        
        # LLM configuration
        self.llm_base_url = os.getenv("LLM_BASE_URL", "http://localhost:1234")
        self.llm_api_key = os.getenv("LLM_API_KEY", "")
        self.general_model = os.getenv("CLINICAL_RESEARCH_MODEL", config.get('model'))
        self.sql_model = os.getenv("SQL_MODEL", self.general_model)  # Can use specialized model for SQL
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self.http_client = httpx.AsyncClient(timeout=180.0)
        
        # Initialize MCP tool registry
        self.tool_registry = MCPToolRegistry()
        self._register_tools()
        
        # Load skill configurations
        self.skill_routing_prompt_template = config.get('skill_routing_prompt_template', '')
        self.skills = self._load_enhanced_skills()
        
        logger.info(f"Clinical executor V2 initialized with {len(self.tool_registry.tools)} MCP tools")
    
    def _register_tools(self):
        """Register all available MCP tools"""
        
        # Spark tools
        spark_config = {
            'host': os.getenv('SPARK_THRIFT_HOST'),
            'port': int(os.getenv('SPARK_THRIFT_PORT', '10001')),
            'database': os.getenv('SPARK_THRIFT_DATABASE', 'default')
        }
        
        if spark_config['host']:
            self.tool_registry.register(SparkPopulationAnalyticsTool(spark_config))
            self.tool_registry.register(SparkPatientLongitudinalTool(spark_config))
            logger.info("Registered Spark analytics tools")
        
        # FHIR tool
        fhir_config = {
            'base_url': os.getenv('OPENMRS_FHIR_BASE_URL'),
            'username': os.getenv('OPENMRS_USERNAME'),
            'password': os.getenv('OPENMRS_PASSWORD')
        }
        
        if fhir_config['base_url']:
            self.tool_registry.register(FHIRSearchTool(fhir_config))
            logger.info("Registered FHIR search tool")
        
        # Medical search (always available as placeholder)
        self.tool_registry.register(MedicalSearchTool())
        logger.info("Registered medical search tool")
    
    def _load_enhanced_skills(self) -> Dict[str, Dict]:
        """Define enhanced skills that map to MCP tools"""
        return {
            "population_analytics": {
                "description": "Analyze population-level health statistics",
                "tool": "spark_population_analytics",
                "prompt_template": """You are a clinical data analyst. Convert this query into population analytics parameters.
                
Query: {query}

Determine the appropriate analysis type and parameters.
Examples:
- "Is flu common now?" → {{"analysis_type": "trends", "condition": "influenza", "timeframe": "last_month"}}
- "Diabetes prevalence" → {{"analysis_type": "prevalence", "condition": "diabetes"}}
- "Comorbidities with hypertension" → {{"analysis_type": "comorbidities", "condition": "hypertension"}}

Respond with JSON only."""
            },
            
            "patient_longitudinal": {
                "description": "Retrieve complete patient health record",
                "tool": "spark_patient_longitudinal",
                "prompt_template": """Extract patient ID and format requirements from this query.

Query: {query}

Determine:
1. Patient ID (if mentioned)
2. Desired format (ips, timeline, summary, full)
3. Specific sections needed

Respond with JSON: {{"patient_id": "...", "format": "...", "sections": [...]}}"""
            },
            
            "fhir_patient_search": {
                "description": "Search specific FHIR resources",
                "tool": "fhir_search",
                "prompt_template": """Convert this query into FHIR search parameters.

Query: {query}

Determine:
1. Resource type (Patient, Observation, Condition, etc.)
2. Patient ID (if mentioned)
3. Search parameters (code, date, status, etc.)

Respond with JSON: {{"resource_type": "...", "patient_id": "...", "search_params": {{}}}}"""
            },
            
            "medical_search": {
                "description": "Search medical literature and resources",
                "tool": "medical_search",
                "prompt_template": """Convert this medical literature query into search parameters.

Query: {query}

Determine search type and filters.
Respond with JSON: {{"query": "...", "search_type": "...", "filters": {{}}}}"""
            }
        }
    
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Execute clinical research request with MCP tools"""
        query = context.get_user_input()
        task = context.current_task
        
        # Create a new task if none exists
        if not task:
            task = new_task(context.message)
            await event_queue.enqueue_event(task)
        
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        
        try:
            # Determine which skill to use
            skill_name = await self._route_to_skill(query)
            logger.info(f"[Task {task.id}] Routed to skill: {skill_name}")
            
            await updater.update_status(
                TaskState.working,
                new_agent_text_message(f"Using {skill_name} to process query...", task.context_id, task.id)
            )
            
            # Execute the appropriate skill
            if skill_name in self.skills:
                result = await self._execute_skill(skill_name, query)
            else:
                result = await self._handle_general_query(query)
            
            # Add result as artifact
            await updater.add_artifact(
                [Part(root=TextPart(text=result))],
                name=f"{skill_name}_response"
            )
            
            # Complete the task
            logger.info(f"[Task {task.id}] Task completed successfully")
            await updater.complete()
            
        except Exception as e:
            logger.error(f"[Task {task.id}] Error processing query: {e}", exc_info=True)
            await updater.update_status(
                TaskState.failed,
                new_agent_text_message(f"Error: {str(e)}", task.context_id, task.id)
            )
    
    async def _route_to_skill(self, query: str) -> str:
        """Determine which skill to use for the query"""
        
        # Build skill descriptions for routing
        skills_info = "\n".join([
            f"- {name}: {skill['description']}" 
            for name, skill in self.skills.items()
        ])
        
        routing_prompt = f"""Determine the best skill for this query.

Available skills:
{skills_info}

Query: {query}

Respond with JSON: {{"skill": "skill_name"}}"""
        
        messages = [
            {"role": "system", "content": "You route queries to appropriate data retrieval skills."},
            {"role": "user", "content": routing_prompt}
        ]
        
        response = await self._call_llm(messages, max_tokens=50)
        
        try:
            # Parse response
            cleaned = response.strip().removeprefix("```json").removesuffix("```").strip()
            result = json.loads(cleaned)
            return result.get("skill", "general")
        except:
            # Default to general if parsing fails
            return "general"
    
    async def _execute_skill(self, skill_name: str, query: str) -> str:
        """Execute a specific skill using its MCP tool"""
        
        skill = self.skills[skill_name]
        tool_name = skill.get("tool")
        
        if not tool_name or not self.tool_registry.has_tool(tool_name):
            return f"Tool {tool_name} not available for skill {skill_name}"
        
        # Step 1: Generate tool parameters using LLM
        prompt = skill["prompt_template"].format(query=query)
        messages = [
            {"role": "system", "content": "You convert queries to tool parameters."},
            {"role": "user", "content": prompt}
        ]
        
        param_response = await self._call_llm(messages, max_tokens=200)
        
        try:
            # Parse parameters
            cleaned = param_response.strip().removeprefix("```json").removesuffix("```").strip()
            tool_params = json.loads(cleaned)
        except Exception as e:
            logger.error(f"Failed to parse tool parameters: {e}")
            return f"Failed to parse parameters for {skill_name}"
        
        # Step 2: Invoke MCP tool
        tool = self.tool_registry.get_tool(tool_name)
        tool_result = await tool.safe_invoke(tool_params)
        
        if not tool_result.get("success"):
            return f"Tool execution failed: {tool_result.get('error')}"
        
        # Step 3: Synthesize results with clinical context
        synthesis_prompt = f"""Interpret these clinical data results for the query: "{query}"

Data retrieved:
{json.dumps(tool_result.get('result', {}), indent=2)}

Provide a clear, clinically relevant interpretation of this data.
Include key findings, patterns, and any clinical significance."""
        
        synthesis_messages = [
            {"role": "system", "content": "You are a clinical data interpreter providing insights from health data."},
            {"role": "user", "content": synthesis_prompt}
        ]
        
        final_response = await self._call_llm(synthesis_messages, max_tokens=1000)
        
        return final_response
    
    async def _handle_general_query(self, query: str) -> str:
        """Handle queries that don't map to specific skills"""
        
        messages = [
            {"role": "system", "content": "You are a helpful clinical research assistant."},
            {"role": "user", "content": query}
        ]
        
        return await self._call_llm(messages)
    
    async def _call_llm(self, messages: list, max_tokens: int = 1500) -> str:
        """Helper to call the LLM"""
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
    
    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> Task | None:
        """Cancel is not supported for this agent"""
        raise ServerError(error=UnsupportedOperationError(
            message="Cancel operation is not supported for Clinical agent"
        ))
    
    async def cleanup(self):
        """Clean up resources"""
        await self.http_client.aclose()
        # Clean up any tool resources
        for tool_name, tool in self.tool_registry.tools.items():
            if hasattr(tool, 'cleanup'):
                await tool.cleanup()
        logger.info("Clinical executor V2 cleanup completed")
    
    def get_agent_card(self) -> AgentCard:
        """Return enhanced agent capabilities for A2A discovery"""
        return AgentCard(
            name="Clinical Research Agent V2",
            description="Advanced clinical data retrieval and analytics with MCP tools",
            url=os.getenv("A2A_CLINICAL_URL", "http://localhost:9102/"),
            version="2.0.0",
            default_input_modes=["text", "text/plain"],
            default_output_modes=["text", "text/plain"],
            capabilities=AgentCapabilities(streaming=True),
            skills=[
                AgentSkill(
                    id="population_analytics",
                    name="Population Analytics",
                    description="Analyze population-level health trends and statistics",
                    tags=["analytics", "population", "spark"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"}
                        },
                        "required": ["query"]
                    }
                ),
                AgentSkill(
                    id="patient_longitudinal",
                    name="Patient Longitudinal Record",
                    description="Retrieve complete patient health history",
                    tags=["patient", "ehr", "longitudinal"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "patient_id": {"type": "string"}
                        },
                        "required": ["query"]
                    }
                ),
                AgentSkill(
                    id="fhir_patient_search",
                    name="FHIR Patient Search",
                    description="Search specific FHIR resources",
                    tags=["fhir", "search", "clinical"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"}
                        },
                        "required": ["query"]
                    }
                ),
                AgentSkill(
                    id="medical_search",
                    name="Medical Literature Search",
                    description="Search medical literature and guidelines",
                    tags=["literature", "research", "guidelines"],
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
