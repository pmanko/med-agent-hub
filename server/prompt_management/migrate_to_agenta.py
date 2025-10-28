#!/usr/bin/env python3
"""
Migration script to transfer YAML prompts to Agenta API.

Usage:
    poetry run python -m server.prompt_management.migrate_to_agenta [options]
    
Options:
    --dry-run           Preview migration without creating prompts
    --agent AGENT       Migrate specific agent only (router, medical, clinical, administrative)
    --force            Overwrite existing prompts in Agenta
    --environment ENV   Target environment (development, staging, production)
"""

import argparse
import asyncio
import logging
import re
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, List, Set

from .agenta_client import AgentaPromptClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PromptMigrator:
    """Migrates prompts from YAML configs to Agenta."""
    
    def __init__(
        self,
        dry_run: bool = False,
        force: bool = False,
        environment: str = "development"
    ):
        self.dry_run = dry_run
        self.force = force
        self.environment = environment
        self.agenta_client = AgentaPromptClient(environment=environment)
        
        # Migration statistics
        self.stats = {
            "total": 0,
            "created": 0,
            "skipped": 0,
            "failed": 0
        }
    
    def _extract_template_parameters(self, content: str) -> List[str]:
        """
        Extract template parameter names from prompt content.
        
        Examples:
        - '{agents_info}' -> ['agents_info']
        - '{query} and {skills_info}' -> ['query', 'skills_info']
        """
        if not content:
            return []
        
        # Find all {variable} patterns
        pattern = r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}'
        matches = re.findall(pattern, content)
        
        # Return unique parameters in order
        seen: Set[str] = set()
        params = []
        for match in matches:
            if match not in seen:
                seen.add(match)
                params.append(match)
        
        return params
    
    def _build_agenta_prompt_name(self, agent_name: str, prompt_key: str) -> str:
        """Build Agenta prompt name from agent and key."""
        clean_key = prompt_key.replace('prompts.', '').replace('_', '-')
        return f"{agent_name}-{clean_key}"
    
    async def migrate_agent(self, agent_name: str) -> None:
        """Migrate all prompts for a specific agent."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Migrating Agent: {agent_name}")
        logger.info(f"{'='*60}")
        
        # Load YAML config
        config_path = Path(__file__).parent.parent / 'agent_configs' / f'{agent_name}.yaml'
        
        if not config_path.exists():
            logger.error(f"Config file not found: {config_path}")
            return
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Extract prompts based on agent structure
        prompts_to_migrate = self._extract_prompts(agent_name, config)
        
        # Migrate each prompt
        for prompt_key, prompt_content in prompts_to_migrate.items():
            await self._migrate_prompt(agent_name, prompt_key, prompt_content, config)
    
    def _extract_prompts(self, agent_name: str, config: Dict[str, Any]) -> Dict[str, str]:
        """Extract all prompts from agent config."""
        prompts = {}
        
        if agent_name == "router":
            # Router has template prompts
            if config.get('system_prompt_template'):
                prompts['system_prompt_template'] = config['system_prompt_template']
            if config.get('react_system_prompt_template'):
                prompts['react_system_prompt_template'] = config['react_system_prompt_template']
        
        elif agent_name == "medical":
            # Medical has a single system prompt
            if config.get('system_prompt'):
                prompts['system_prompt'] = config['system_prompt']
        
        elif agent_name == "clinical":
            # Clinical has routing template and skill prompts
            if config.get('skill_routing_prompt_template'):
                prompts['skill_routing_prompt_template'] = config['skill_routing_prompt_template']
            
            # Extract individual skill prompts
            skill_prompts = config.get('skill_prompts', {})
            for skill_name, skill_prompt in skill_prompts.items():
                prompts[f'skill_{skill_name}'] = skill_prompt
        
        elif agent_name == "administrative":
            # Administrative has a prompts dict
            admin_prompts = config.get('prompts', {})
            for prompt_name, prompt_content in admin_prompts.items():
                prompts[f'prompts.{prompt_name}'] = prompt_content
        
        return prompts
    
    async def _migrate_prompt(
        self,
        agent_name: str,
        prompt_key: str,
        prompt_content: str,
        config: Dict[str, Any]
    ) -> None:
        """Migrate a single prompt to Agenta."""
        self.stats["total"] += 1
        
        agenta_name = self._build_agenta_prompt_name(agent_name, prompt_key)
        
        # Extract template parameters
        parameters = self._extract_template_parameters(prompt_content)
        
        # Build tags for metadata
        tags = {
            "agent": agent_name,
            "role": config.get('role', 'unknown'),
            "type": prompt_key.split('.')[0] if '.' in prompt_key else "system",
            "version": "1.0.0",
            "source": "yaml_migration"
        }
        
        logger.info(f"\nPrompt: {agenta_name}")
        logger.info(f"  Length: {len(prompt_content)} chars")
        logger.info(f"  Parameters: {parameters}")
        logger.info(f"  Tags: {tags}")
        
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would create prompt in Agenta")
            self.stats["skipped"] += 1
            return
        
        # Create prompt in Agenta
        try:
            success = await self.agenta_client.create_prompt(
                prompt_name=agenta_name,
                content=prompt_content,
                parameters=parameters,
                tags=tags,
                environment=self.environment
            )
            
            if success:
                logger.info(f"  ✅ Created in Agenta")
                self.stats["created"] += 1
            else:
                logger.warning(f"  ⚠️  Failed to create (may already exist)")
                if self.force:
                    # Try update instead
                    updated = await self.agenta_client.update_prompt(
                        prompt_name=agenta_name,
                        content=prompt_content,
                        environment=self.environment
                    )
                    if updated:
                        logger.info(f"  ✅ Updated existing prompt")
                        self.stats["created"] += 1
                    else:
                        self.stats["failed"] += 1
                else:
                    self.stats["skipped"] += 1
        
        except Exception as e:
            logger.error(f"  ❌ Error migrating prompt: {e}")
            self.stats["failed"] += 1
    
    async def migrate_all(self, agents: List[str]) -> None:
        """Migrate prompts for all specified agents."""
        logger.info(f"\n{'='*80}")
        logger.info("Agenta Prompt Migration Tool")
        logger.info(f"{'='*80}")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Force overwrite: {self.force}")
        logger.info(f"Agents to migrate: {', '.join(agents)}")
        
        # Check Agenta health
        if not self.dry_run:
            healthy = await self.agenta_client.health_check()
            if not healthy:
                logger.error("\n❌ Agenta API is not reachable. Please ensure Agenta is running.")
                logger.error("   Deploy with: ./instant package init -n agenta -d")
                sys.exit(1)
            logger.info("✅ Agenta API is reachable\n")
        
        # Migrate each agent
        for agent_name in agents:
            try:
                await self.migrate_agent(agent_name)
            except Exception as e:
                logger.error(f"Failed to migrate agent '{agent_name}': {e}")
                self.stats["failed"] += 1
        
        # Print summary
        self._print_summary()
    
    def _print_summary(self):
        """Print migration summary."""
        logger.info(f"\n{'='*80}")
        logger.info("Migration Summary")
        logger.info(f"{'='*80}")
        logger.info(f"Total prompts processed: {self.stats['total']}")
        logger.info(f"Created/Updated: {self.stats['created']}")
        logger.info(f"Skipped: {self.stats['skipped']}")
        logger.info(f"Failed: {self.stats['failed']}")
        
        if self.dry_run:
            logger.info("\n💡 This was a dry run. Use --no-dry-run to actually migrate prompts.")
        elif self.stats['failed'] == 0:
            logger.info("\n✅ Migration completed successfully!")
            logger.info("\nNext steps:")
            logger.info("1. Access Agenta UI: http://localhost:8002")
            logger.info("2. Configure LM Studio models in Agenta → Models")
            logger.info("3. Test prompts in Agenta playground")
            logger.info("4. Restart med-agent-hub agents to use Agenta prompts")
        else:
            logger.warning(f"\n⚠️  Migration completed with {self.stats['failed']} failures")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.agenta_client:
            await self.agenta_client.close()


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate YAML prompts to Agenta API",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview migration without creating prompts'
    )
    parser.add_argument(
        '--agent',
        choices=['router', 'medical', 'clinical', 'administrative', 'all'],
        default='all',
        help='Migrate specific agent only (default: all)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing prompts in Agenta'
    )
    parser.add_argument(
        '--environment',
        choices=['development', 'staging', 'production'],
        default='development',
        help='Target Agenta environment (default: development)'
    )
    
    args = parser.parse_args()
    
    # Determine which agents to migrate
    if args.agent == 'all':
        agents = ['router', 'medical', 'clinical', 'administrative']
    else:
        agents = [args.agent]
    
    # Run migration
    migrator = PromptMigrator(
        dry_run=args.dry_run,
        force=args.force,
        environment=args.environment
    )
    
    try:
        await migrator.migrate_all(agents)
    finally:
        await migrator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

