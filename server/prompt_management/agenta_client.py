"""
Agenta API Client for prompt management.
Provides caching and graceful error handling.
"""

import os
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)


class AgentaPromptClient:
    """Client for interacting with Agenta API for prompt management."""
    
    def __init__(
        self,
        api_url: Optional[str] = None,
        environment: Optional[str] = None,
        cache_ttl: Optional[int] = None
    ):
        """
        Initialize Agenta client.
        
        Args:
            api_url: Agenta API base URL (defaults to env var or localhost)
            environment: Agenta environment to use (dev/staging/prod)
            cache_ttl: Cache TTL in seconds (default: 300)
        """
        self.api_url = (api_url or os.getenv("AGENTA_API_URL", "http://localhost:8001/api/v1")).rstrip("/")
        self.environment = environment or os.getenv("AGENTA_ENVIRONMENT", "development")
        self.cache_ttl = cache_ttl or int(os.getenv("PROMPT_CACHE_TTL", "300"))
        
        # In-memory cache: {prompt_name: (content, timestamp)}
        self._cache: Dict[str, tuple[str, float]] = {}
        
        # HTTP client with timeout
        self.client = httpx.AsyncClient(timeout=10.0)
        
        logger.info(f"AgentaPromptClient initialized: url={self.api_url}, env={self.environment}, cache_ttl={self.cache_ttl}s")
    
    def _is_cache_valid(self, prompt_name: str) -> bool:
        """Check if cached prompt is still valid."""
        if prompt_name not in self._cache:
            return False
        
        _, timestamp = self._cache[prompt_name]
        age = time.time() - timestamp
        return age < self.cache_ttl
    
    async def get_prompt(
        self,
        prompt_name: str,
        environment: Optional[str] = None,
        use_cache: bool = True
    ) -> Optional[str]:
        """
        Fetch a prompt from Agenta.
        
        Args:
            prompt_name: Name of the prompt (e.g., 'router-system-prompt-template')
            environment: Override default environment
            use_cache: Whether to use cached version
            
        Returns:
            Prompt content as string, or None if not found
        """
        # Check cache first
        if use_cache and self._is_cache_valid(prompt_name):
            content, _ = self._cache[prompt_name]
            logger.debug(f"Cache hit for prompt '{prompt_name}'")
            return content
        
        # Fetch from Agenta API
        env = environment or self.environment
        
        try:
            # Agenta API endpoint for fetching prompts
            # Note: Actual endpoint may vary based on Agenta version
            # This is a common pattern for prompt retrieval
            url = f"{self.api_url}/prompts/{prompt_name}"
            params = {"environment": env}
            
            response = await self.client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                # Extract prompt content (structure depends on Agenta API)
                content = data.get("content") or data.get("template", "")
                
                # Cache the result
                self._cache[prompt_name] = (content, time.time())
                logger.info(f"Fetched prompt '{prompt_name}' from Agenta (env: {env})")
                
                return content
            elif response.status_code == 404:
                logger.warning(f"Prompt '{prompt_name}' not found in Agenta")
                return None
            else:
                logger.error(f"Agenta API error {response.status_code} for prompt '{prompt_name}'")
                return None
                
        except httpx.ConnectError as e:
            logger.warning(f"Cannot connect to Agenta at {self.api_url}: {e}")
            return None
        except httpx.TimeoutException:
            logger.warning(f"Timeout fetching prompt '{prompt_name}' from Agenta")
            return None
        except Exception as e:
            logger.error(f"Error fetching prompt '{prompt_name}' from Agenta: {e}")
            return None
    
    async def create_prompt(
        self,
        prompt_name: str,
        content: str,
        parameters: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
        environment: Optional[str] = None
    ) -> bool:
        """
        Create a new prompt in Agenta.
        
        Args:
            prompt_name: Unique prompt identifier
            content: Prompt template content
            parameters: List of template variable names (e.g., ['query', 'agents_info'])
            tags: Metadata tags for organization
            environment: Target environment
            
        Returns:
            True if successful, False otherwise
        """
        env = environment or self.environment
        
        try:
            url = f"{self.api_url}/prompts"
            payload = {
                "name": prompt_name,
                "content": content,
                "parameters": parameters or [],
                "tags": tags or {},
                "environment": env
            }
            
            response = await self.client.post(url, json=payload)
            
            if response.status_code in [200, 201]:
                logger.info(f"Created prompt '{prompt_name}' in Agenta (env: {env})")
                # Invalidate cache for this prompt
                self._cache.pop(prompt_name, None)
                return True
            else:
                logger.error(f"Failed to create prompt '{prompt_name}': {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating prompt '{prompt_name}': {e}")
            return False
    
    async def update_prompt(
        self,
        prompt_name: str,
        content: str,
        environment: Optional[str] = None
    ) -> bool:
        """
        Update an existing prompt in Agenta.
        
        Args:
            prompt_name: Prompt identifier
            content: New prompt content
            environment: Target environment
            
        Returns:
            True if successful, False otherwise
        """
        env = environment or self.environment
        
        try:
            url = f"{self.api_url}/prompts/{prompt_name}"
            payload = {
                "content": content,
                "environment": env
            }
            
            response = await self.client.put(url, json=payload)
            
            if response.status_code == 200:
                logger.info(f"Updated prompt '{prompt_name}' in Agenta (env: {env})")
                # Invalidate cache
                self._cache.pop(prompt_name, None)
                return True
            else:
                logger.error(f"Failed to update prompt '{prompt_name}': {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating prompt '{prompt_name}': {e}")
            return False
    
    async def list_prompt_versions(self, prompt_name: str) -> List[Dict[str, Any]]:
        """
        List all versions of a prompt.
        
        Args:
            prompt_name: Prompt identifier
            
        Returns:
            List of version metadata dicts
        """
        try:
            url = f"{self.api_url}/prompts/{prompt_name}/versions"
            response = await self.client.get(url)
            
            if response.status_code == 200:
                return response.json().get("versions", [])
            else:
                logger.warning(f"Failed to list versions for '{prompt_name}': {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error listing versions for '{prompt_name}': {e}")
            return []
    
    def clear_cache(self, prompt_name: Optional[str] = None):
        """
        Clear cached prompts.
        
        Args:
            prompt_name: Specific prompt to clear, or None for all prompts
        """
        if prompt_name:
            self._cache.pop(prompt_name, None)
            logger.debug(f"Cleared cache for prompt '{prompt_name}'")
        else:
            self._cache.clear()
            logger.info("Cleared all prompt cache")
    
    async def health_check(self) -> bool:
        """
        Check if Agenta API is reachable.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            url = f"{self.api_url}/health"
            response = await self.client.get(url, timeout=5.0)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Agenta health check failed: {e}")
            return False
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

