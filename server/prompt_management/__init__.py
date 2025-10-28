"""
Prompt Management Module for Med Agent Hub

Provides abstraction layer for loading prompts from multiple backends:
- Agenta API (primary, for web-based prompt management)
- YAML files (fallback, for resilience)
"""

from .loader import PromptLoader

__all__ = ['PromptLoader']

