"""
Base classes for MCP (Model Context Protocol) tool integration.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import os
import yaml
import jsonschema
import logging

logger = logging.getLogger(__name__)


class MCPToolRegistry:
    """Registry for discovering and managing MCP tools."""
    
    def __init__(self):
        self.tools: Dict[str, 'MCPTool'] = {}
        logger.info("MCP Tool Registry initialized")
    
    def register(self, tool: 'MCPTool') -> None:
        """Register a new MCP tool."""
        self.tools[tool.name] = tool
        logger.info(f"Registered MCP tool: {tool.name}")
    
    def get_tool(self, name: str) -> Optional['MCPTool']:
        """Get a tool by name."""
        return self.tools.get(name)
    
    def list_tools(self) -> Dict[str, Dict]:
        """List all available tools with their schemas."""
        return {name: tool.schema for name, tool in self.tools.items()}
    
    def has_tool(self, name: str) -> bool:
        """Check if a tool is registered."""
        return name in self.tools


class SparkProfile:
    """Config-driven logical-to-physical mapping for Spark views/columns."""

    def __init__(self, profile_name: Optional[str] = None, profiles_dir: Optional[str] = None):
        self.profile_name = profile_name or os.getenv("SPARK_PROFILE", "parquet_on_fhir_flat")
        self.profiles_dir = profiles_dir or os.getenv("SPARK_PROFILES_DIR", "server/mcp/config/spark_profiles")
        self.mapping: Dict[str, Any] = {}
        self.capabilities: Dict[str, bool] = {}

    def load(self) -> None:
        path = os.path.join(self.profiles_dir, f"{self.profile_name}.yaml")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Spark profile not found: {path}")
        with open(path, "r") as f:
            self.mapping = yaml.safe_load(f) or {}

    def compute_capabilities_via_introspection(self, connection) -> None:
        """Inspect Spark catalogs to determine if mapped tables/columns exist."""
        supported: Dict[str, bool] = {}
        # Build a set of available columns per table
        tables: Dict[str, set] = {}
        try:
            cursor = connection.cursor()
        except Exception:
            cursor = None

        for view_name, view_cfg in self.mapping.get("views", {}).items():
            table = view_cfg.get("table")
            tables[table] = set()
            if cursor and table:
                try:
                    cursor.execute(f"DESCRIBE {table}")
                    rows = cursor.fetchall() or []
                    for row in rows:
                        col = row[0]
                        if isinstance(col, str) and col and not col.startswith("#"):
                            tables[table].add(col.strip())
                except Exception:
                    # Table missing
                    tables[table] = set()

        # Evaluate features
        for feature, cfg in self.mapping.get("features", {}).items():
            reqs = cfg.get("requires", [])
            ok = True
            for ref in reqs:
                parts = ref.split('.')
                if len(parts) != 2:
                    ok = False
                    break
                view, col = parts
                table = self.get_table(view)
                mapped_col = self.get_column(view, col)
                if not table or not mapped_col:
                    ok = False
                    break
                # If mapped_col is an expression, skip physical check; otherwise verify presence
                if cursor and mapped_col.isidentifier():
                    if mapped_col not in tables.get(table, set()):
                        ok = False
                        break
            supported[feature] = ok

        self.capabilities = supported

    def get_table(self, logical_view: str) -> Optional[str]:
        return self.mapping.get("views", {}).get(logical_view, {}).get("table")

    def get_column(self, logical_view: str, logical_column: str) -> Optional[str]:
        return self.mapping.get("views", {}).get(logical_view, {}).get("columns", {}).get(logical_column)

    def list_required_for(self, feature: str) -> List[str]:
        return self.mapping.get("features", {}).get(feature, {}).get("requires", [])

    def set_capability(self, feature: str, supported: bool) -> None:
        self.capabilities[feature] = supported

    def is_supported(self, feature: str) -> bool:
        return self.capabilities.get(feature, False)


class MCPTool(ABC):
    """Abstract base class for MCP-compliant tools."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this tool."""
        pass
    
    @property
    @abstractmethod
    def schema(self) -> Dict[str, Any]:
        """
        MCP tool schema including:
        - name: Tool name
        - description: What this tool does
        - input_schema: JSON Schema for input validation
        - output_schema: JSON Schema for output format
        """
        pass
    
    def validate_input(self, params: Dict[str, Any]) -> None:
        """
        Validate input parameters against the tool's input schema.
        Raises jsonschema.ValidationError if invalid.
        """
        input_schema = self.schema.get("input_schema", {})
        if input_schema:
            jsonschema.validate(params, input_schema)
    
    @abstractmethod
    async def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the tool with validated parameters.
        
        Args:
            params: Input parameters (will be validated against schema)
            
        Returns:
            Dict containing the tool's output
        """
        pass
    
    async def safe_invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Safely invoke the tool with validation and error handling.
        """
        try:
            self.validate_input(params)
            result = await self.invoke(params)
            logger.debug(f"Tool {self.name} executed successfully")
            return {
                "success": True,
                "result": result
            }
        except jsonschema.ValidationError as e:
            logger.error(f"Tool {self.name} validation error: {e}")
            return {
                "success": False,
                "error": f"Validation error: {e.message}"
            }
        except Exception as e:
            logger.error(f"Tool {self.name} execution error: {e}")
            return {
                "success": False,
                "error": f"Execution error: {str(e)}"
            }


class CompositeMCPTool(MCPTool):
    """
    A tool that combines multiple sub-tools into one interface.
    Useful for grouping related functionality.
    """
    
    def __init__(self, name: str, description: str, sub_tools: List[MCPTool]):
        self._name = name
        self._description = description
        self.sub_tools = {tool.name: tool for tool in sub_tools}
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "name": self._name,
            "description": self._description,
            "input_schema": {
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "enum": list(self.sub_tools.keys())
                    },
                    "params": {
                        "type": "object",
                        "description": "Parameters for the selected sub-tool"
                    }
                },
                "required": ["tool_name", "params"]
            },
            "sub_tools": {
                name: tool.schema for name, tool in self.sub_tools.items()
            }
        }
    
    async def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the specified sub-tool."""
        tool_name = params["tool_name"]
        tool_params = params["params"]
        
        if tool_name not in self.sub_tools:
            raise ValueError(f"Unknown sub-tool: {tool_name}")
        
        sub_tool = self.sub_tools[tool_name]
        return await sub_tool.invoke(tool_params)
