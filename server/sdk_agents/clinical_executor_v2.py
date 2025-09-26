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
import re
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
        self.general_model = config.get('model')
        self.sql_model = os.getenv("SQL_MODEL", self.general_model)  # Can use specialized model for SQL
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self.http_client = httpx.AsyncClient(timeout=180.0)
        
        # Initialize MCP tool registry
        self.tool_registry = MCPToolRegistry()
        self._register_tools()
        
        # Load skill configurations
        self.skill_routing_prompt_template = config.get('skill_routing_prompt_template', '')
        self.skill_prompts = config.get('skill_prompts', {})
        self.skills = self._load_enhanced_skills()
        
        logger.info(
            f"ClinicalExecutorV2 init: llm_base_url={self.llm_base_url}, "
            f"general_model={self.general_model}, sql_model={self.sql_model}, "
            f"temperature={self.temperature}, tools={len(self.tool_registry.tools)}"
        )
    
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
                "prompt_template": self.skill_prompts.get("population_analytics", "")
            },
            
            "patient_longitudinal": {
                "description": "Retrieve complete patient health record",
                "tool": "spark_patient_longitudinal",
                "prompt_template": self.skill_prompts.get("patient_longitudinal", "")
            },
            
            "fhir_patient_search": {
                "description": "Search specific FHIR resources",
                "tool": "fhir_search",
                "prompt_template": self.skill_prompts.get("fhir_patient_search", "")
            },
            
            "medical_search": {
                "description": "Search medical literature and resources",
                "tool": "medical_search",
                "prompt_template": self.skill_prompts.get("medical_search", "")
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
        
        if self.skill_routing_prompt_template:
            routing_prompt = self.skill_routing_prompt_template.format(skills_info=skills_info, query=query)
        else:
            routing_prompt = f"""You are a multi-skilled clinical agent. Choose the best skill.

Available skills:
{skills_info}

User query: "{query}"

Respond with JSON only: {{"skill": "skill_name"}}"""
        
        messages = [
            {"role": "system", "content": "Return JSON only. Route queries to appropriate clinical skills."},
            {"role": "user", "content": routing_prompt}
        ]
        
        response = await self._call_llm(messages, max_tokens=50)
        
        try:
            result = self._extract_first_json(response)
            return result.get("skill", "general")
        except:
            # Default to general if parsing fails
            return "general"
    
    async def _execute_skill(self, skill_name: str, query: str) -> str:
        """Execute a specific skill using its MCP tool"""
        
        skill_key = (skill_name or "").strip().lower()
        if skill_key not in self.skills:
            # best-effort fuzzy mapping
            if "population" in skill_key:
                skill_key = "population_analytics"
            elif "longitudinal" in skill_key or "patient" in skill_key:
                skill_key = "patient_longitudinal"
            elif "fhir" in skill_key:
                skill_key = "fhir_patient_search"
            elif "medical" in skill_key or "literature" in skill_key:
                skill_key = "medical_search"
        skill = self.skills.get(skill_key, {})
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
        tool_params = self._extract_first_json(param_response)
        except Exception as e:
            logger.error(f"Failed to parse tool parameters: {e}")
            # Fallback to normalization from raw query
            tool_params = {}

        # Normalize/fallback parameters per skill to avoid validation errors
        tool_params = self._normalize_tool_params(skill_key, tool_params, query)

        # Final guard: ensure required fields for tool exist with safe defaults
        try:
            tool_obj = self.tool_registry.get_tool(tool_name)
            required = (tool_obj.schema or {}).get("input_schema", {}).get("required", []) if tool_obj else []
            if skill_key == "population_analytics":
                if "analysis_type" in required and not tool_params.get("analysis_type"):
                    tool_params["analysis_type"] = "prevalence"
                if not tool_params.get("timeframe"):
                    tool_params["timeframe"] = "all_time"
            if skill_key == "patient_longitudinal":
                if "patient_id" in required and not tool_params.get("patient_id"):
                    m = re.search(r"patient\s*([A-Za-z0-9_-]+)", query, re.IGNORECASE)
                    if m:
                        tool_params["patient_id"] = m.group(1)
                if not tool_params.get("format"):
                    tool_params["format"] = "summary"
            if skill_key == "fhir_patient_search":
                if not tool_params.get("resource_type"):
                    ql = query.lower()
                    tool_params["resource_type"] = "Observation" if any(k in ql for k in ["lab","result","observation","hba1c"]) else "Patient"
                sp = tool_params.get("search_params") or {}
                sp.setdefault("_count", 5)
                tool_params["search_params"] = sp
        except Exception:
            pass
        
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

    def _extract_first_json(self, content: str) -> Dict[str, Any]:
        """Extract and parse the first JSON object from LLM content robustly."""
        if not content:
            return {}
        text = content.strip()
        # Remove fenced code if present
        if text.startswith("```"):
            # Strip first fence line
            parts = text.split("```", 2)
            if len(parts) >= 3:
                text = parts[1] if not parts[1].strip() else parts[1].split("\n", 1)[1] if parts[1].startswith("json") else parts[1]
                text = text.strip()
        # Try direct JSON load
        try:
            return json.loads(text)
        except Exception:
            pass
        # Regex search for first {...} block
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            candidate = match.group(0)
            # Remove inline comments (// ... ) defensively
            candidate = re.sub(r"//.*", "", candidate)
            try:
                return json.loads(candidate)
            except Exception:
                pass
        return {}

    def _normalize_tool_params(self, skill_name: str, params: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Normalize tool parameters with sensible defaults and mappings."""
        normalized: Dict[str, Any] = dict(params or {})

        if skill_name == "patient_longitudinal":
            # Extract patient id if missing
            if not normalized.get("patient_id"):
                import re
                m = re.search(r"patient\s*([A-Za-z0-9_-]+)", query, re.IGNORECASE)
                if m:
                    normalized["patient_id"] = m.group(1)
            # Default format
            if normalized.get("format") not in {"ips", "timeline", "summary", "full"}:
                normalized["format"] = "summary"
            # Sections normalization
            allowed = {"demographics", "conditions", "medications", "observations", "encounters", "procedures"}
            sections = normalized.get("sections")
            if isinstance(sections, list):
                mapped = []
                for s in sections:
                    s_lower = str(s).strip().lower()
                    if "complete" in s_lower or "history" in s_lower:
                        mapped = []  # empty → include all
                        break
                    if s_lower in allowed:
                        mapped.append(s_lower)
                if mapped:
                    normalized["sections"] = mapped
                else:
                    normalized.pop("sections", None)
            elif sections is not None:
                normalized.pop("sections", None)

        elif skill_name == "fhir_patient_search":
            # Resource type default and normalization
            rt_allowed = {"Patient", "Observation", "Condition", "MedicationRequest", "Encounter", "Procedure", "DiagnosticReport", "AllergyIntolerance"}
            rt = str(normalized.get("resource_type", "")).strip()
            if not rt:
                ql = query.lower()
                if any(k in ql for k in ["lab", "result", "observation", "hba1c"]):
                    rt = "Observation"
                else:
                    rt = "Patient"
            for cand in rt_allowed:
                if cand.lower() == rt.lower():
                    rt = cand
                    break
            if rt not in rt_allowed:
                rt = "Observation"
            normalized["resource_type"] = rt

            # Patient id inference
            if not normalized.get("patient_id"):
                import re
                m = re.search(r"patient\s*([A-Za-z0-9_-]+)", query, re.IGNORECASE)
                if m:
                    normalized["patient_id"] = m.group(1)

            # Search params default
            sp = normalized.get("search_params") or {}
            sp.setdefault("_count", 5)
            normalized["search_params"] = sp

        elif skill_name == "medical_search":
            normalized.setdefault("query", query)
            mapping = {
                "literature review": "literature",
                "literature review/research articles": "literature",
                "research articles": "literature",
                "research": "literature",
                "guideline": "guidelines",
                "protocol": "protocols",
                "drug": "drug_info",
            }
            st = str(normalized.get("search_type", "general")).strip().lower()
            st = mapping.get(st, st)
            if st not in {"literature", "guidelines", "protocols", "drug_info", "general"}:
                st = "general"
            normalized["search_type"] = st

        elif skill_name == "population_analytics":
            ql = query.lower()
            # analysis_type
            at_allowed = {"prevalence", "trends", "demographics", "comorbidities", "custom"}
            at = str(normalized.get("analysis_type", "")).strip().lower()
            if not at:
                at = "trends" if any(k in ql for k in ["trend", "recent", "increasing", "common now"]) else "prevalence"
            if at not in at_allowed:
                at = "prevalence"
            normalized["analysis_type"] = at

            # condition
            if not normalized.get("condition"):
                import re
                m = re.search(r"\b(diabetes|hypertension|flu|influenza)\b", query, re.IGNORECASE)
                if m:
                    normalized["condition"] = m.group(1).lower()

            # timeframe
            tf_allowed = {"all_time", "last_year", "last_month", "last_week", "custom"}
            tf = str(normalized.get("timeframe", "")).strip().lower()
            if not tf:
                if any(k in ql for k in ["overall", "historical", "all time", "ever"]):
                    tf = "all_time"
                elif any(k in ql for k in ["month", "recent", "now", "current"]):
                    tf = "last_month"
                else:
                    tf = "all_time"
            # normalize common synonyms
            if tf in {"overall patient population", "overall", "historical"}:
                tf = "all_time"
            if tf not in tf_allowed:
                tf = "all_time"
            normalized["timeframe"] = tf

        return normalized
    
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
        
        url = f"{self.llm_base_url}/v1/chat/completions"
        logger.info(
            f"ClinicalExecutorV2 LLM call: url={url}, model={self.general_model}, max_tokens={max_tokens}"
        )
        response = await self.http_client.post(url, headers=headers, json=request_data)
        logger.info(f"ClinicalExecutorV2 LLM response: status={response.status_code}")
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
            capabilities=AgentCapabilities(streaming=False),
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
