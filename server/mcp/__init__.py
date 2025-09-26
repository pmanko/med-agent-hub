"""
MCP (Model Context Protocol) Tool Integration
Provides wrappers for external services as MCP-compliant tools
"""

from .base import MCPTool, MCPToolRegistry

__all__ = ['MCPTool', 'MCPToolRegistry']
