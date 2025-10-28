"""
Unified Prompt Loader with multi-backend support.
Supports Agenta API (primary) with YAML fallback for resilience.
"""

import os
import logging
import yaml
from pathlib import Path
from typing import Dict, Optional, Any
import asyncio
from threading import Lock

from .agenta_client import AgentaPromptClient

logger = logging.getLogger(__name__)


class PromptLoader:
    """
    Singleton prompt loader with multi-backend support.
    
    Backends:
    - 'agenta': Use Agenta API exclusively (fail if unavailable)
    - 'yaml': Use YAML files exclusively
    - 'auto': Try Agenta first, fall back to YAML (default)
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.backend_mode = os.getenv("PROMPT_BACKEND", "auto").lower()
        self.agenta_client: Optional[AgentaPromptClient] = None
        self._yaml_cache: Dict[str, Dict[str, Any]] = {}
        
        # Initialize Agenta client if needed
        if self.backend_mode in ["agenta", "auto"]:
            try:
                self.agenta_client = AgentaPromptClient()
                logger.info(f"PromptLoader initialized with backend mode: {self.backend_mode}")
            except Exception as e:
                logger.warning(f"Failed to initialize Agenta client: {e}")
                if self.backend_mode == "agenta":
                    raise
                logger.info("Falling back to YAML-only mode")
                self.backend_mode = "yaml"
        else:
            logger.info(f"PromptLoader using YAML-only mode")
        
        self._initialized = True
    
    def _load_yaml_config(self, agent_name: str) -> Dict[str, Any]:
        """Load and cache YAML config for an agent."""
        if agent_name in self._yaml_cache:
            return self._yaml_cache[agent_name]
        
        # Determine config path
        config_path = Path(__file__).resolve().parent.parent / 'agent_configs' / f'{agent_name}.yaml'
        
        if not config_path.exists():
            logger.error(f"YAML config not found for agent '{agent_name}' at {config_path}")
            return {}
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self._yaml_cache[agent_name] = config
                logger.debug(f"Loaded YAML config for agent '{agent_name}'")
                return config
        except Exception as e:
            logger.error(f"Error loading YAML config for '{agent_name}': {e}")
            return {}
    
    def _get_from_yaml(self, agent_name: str, prompt_key: str) -> Optional[str]:
        """Retrieve prompt from YAML config."""
        config = self._load_yaml_config(agent_name)
        
        # Handle nested keys (e.g., 'skill_prompts.population_analytics')
        if '.' in prompt_key:
            parts = prompt_key.split('.')
            value = config
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value if isinstance(value, str) else None
        
        # Direct key access
        return config.get(prompt_key)
    
    def _get_dict_from_yaml(self, agent_name: str, dict_key: str) -> Dict[str, str]:
        """Retrieve a dict of prompts from YAML config."""
        config = self._load_yaml_config(agent_name)
        result = config.get(dict_key, {})
        return result if isinstance(result, dict) else {}
    
    async def _get_from_agenta(self, agent_name: str, prompt_key: str) -> Optional[str]:
        """Retrieve prompt from Agenta API."""
        if not self.agenta_client:
            return None
        
        # Map agent/prompt_key to Agenta prompt naming convention
        prompt_name = self._build_agenta_prompt_name(agent_name, prompt_key)
        
        try:
            content = await self.agenta_client.get_prompt(prompt_name)
            return content
        except Exception as e:
            logger.error(f"Error fetching from Agenta: {e}")
            return None
    
    def _build_agenta_prompt_name(self, agent_name: str, prompt_key: str) -> str:
        """
        Build Agenta prompt name from agent and key.
        
        Examples:
        - ('router', 'system_prompt_template') -> 'router-system-prompt-template'
        - ('clinical', 'skill_population_analytics') -> 'clinical-skill-population-analytics'
        """
        # Replace underscores with hyphens, remove 'prompts.' prefix if present
        clean_key = prompt_key.replace('prompts.', '').replace('_', '-')
        return f"{agent_name}-{clean_key}"
    
    def load_prompt(self, agent_name: str, prompt_key: str) -> str:
        """
        Load a prompt from configured backend with fallback.
        
        Args:
            agent_name: Agent identifier (router, medical, clinical, administrative)
            prompt_key: Prompt key from YAML structure
            
        Returns:
            Prompt content as string (empty string if not found)
        """
        prompt_content = ""
        source = "none"
        
        # Try Agenta first if in agenta or auto mode
        if self.backend_mode in ["agenta", "auto"] and self.agenta_client:
            try:
                # Run async operation in sync context
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're already in an async context, create a task
                    prompt_content = asyncio.create_task(
                        self._get_from_agenta(agent_name, prompt_key)
                    ).result()
                else:
                    prompt_content = loop.run_until_complete(
                        self._get_from_agenta(agent_name, prompt_key)
                    )
                
                if prompt_content:
                    source = "agenta"
            except Exception as e:
                logger.debug(f"Agenta fetch failed for '{agent_name}.{prompt_key}': {e}")
        
        # Fall back to YAML if needed
        if not prompt_content and self.backend_mode in ["yaml", "auto"]:
            prompt_content = self._get_from_yaml(agent_name, prompt_key)
            if prompt_content:
                source = "yaml"
        
        # Log the result
        if prompt_content:
            logger.info(f"Loaded prompt '{agent_name}.{prompt_key}' from {source} ({len(prompt_content)} chars)")
        else:
            logger.warning(f"Prompt '{agent_name}.{prompt_key}' not found in any backend")
        
        return prompt_content or ""
    
    def load_prompt_dict(self, agent_name: str, dict_key: str) -> Dict[str, str]:
        """
        Load a dictionary of prompts (e.g., skill_prompts, prompts).
        
        Args:
            agent_name: Agent identifier
            dict_key: Key for the prompt dictionary
            
        Returns:
            Dictionary of prompt keys to content
        """
        result = {}
        source = "none"
        
        # For now, load from YAML as Agenta stores individual prompts
        # Future enhancement: support grouped prompts in Agenta
        if self.backend_mode in ["yaml", "auto"]:
            result = self._get_dict_from_yaml(agent_name, dict_key)
            if result:
                source = "yaml"
        
        if result:
            logger.info(f"Loaded prompt dict '{agent_name}.{dict_key}' from {source} ({len(result)} prompts)")
        else:
            logger.warning(f"Prompt dict '{agent_name}.{dict_key}' not found")
        
        return result
    
    def reload_cache(self):
        """Clear all caches to force fresh load."""
        if self.agenta_client:
            self.agenta_client.clear_cache()
        self._yaml_cache.clear()
        logger.info("Cleared all prompt caches")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check health of prompt backends.
        
        Returns:
            Status dict with backend availability
        """
        status = {
            "mode": self.backend_mode,
            "agenta_available": False,
            "yaml_available": False
        }
        
        # Check Agenta
        if self.agenta_client:
            status["agenta_available"] = await self.agenta_client.health_check()
        
        # Check YAML (just verify configs directory exists)
        config_dir = Path(__file__).resolve().parent.parent / 'agent_configs'
        status["yaml_available"] = config_dir.exists() and config_dir.is_dir()
        
        return status

