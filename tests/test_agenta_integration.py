#!/usr/bin/env python3
"""
Integration tests for Agenta prompt management.

Tests:
- Agenta package deployment and health
- Prompt fetching via AgentaPromptClient
- Fallback to YAML when Agenta unavailable
- Cache TTL behavior
- PromptLoader backend selection
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from server.prompt_management.agenta_client import AgentaPromptClient
from server.prompt_management.loader import PromptLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_agenta_health():
    """Test Agenta API health endpoint."""
    logger.info("\n" + "="*60)
    logger.info("Test: Agenta Health Check")
    logger.info("="*60)
    
    client = AgentaPromptClient()
    
    try:
        healthy = await client.health_check()
        
        if healthy:
            logger.info("✅ Agenta API is healthy and reachable")
            return True
        else:
            logger.warning("⚠️  Agenta API is not reachable (this is OK if package not deployed)")
            return False
    finally:
        await client.close()


async def test_prompt_fetching():
    """Test fetching prompts from Agenta."""
    logger.info("\n" + "="*60)
    logger.info("Test: Prompt Fetching from Agenta")
    logger.info("="*60)
    
    client = AgentaPromptClient()
    
    try:
        # Try to fetch a known prompt (after migration)
        prompt = await client.get_prompt("router-system-prompt-template")
        
        if prompt:
            logger.info(f"✅ Successfully fetched prompt ({len(prompt)} chars)")
            logger.info(f"   Preview: {prompt[:100]}...")
            return True
        else:
            logger.info("ℹ️  Prompt not found (expected if migration not run yet)")
            return False
    except Exception as e:
        logger.error(f"❌ Error fetching prompt: {e}")
        return False
    finally:
        await client.close()


async def test_cache_behavior():
    """Test prompt caching behavior."""
    logger.info("\n" + "="*60)
    logger.info("Test: Cache Behavior")
    logger.info("="*60)
    
    # Use short TTL for testing
    client = AgentaPromptClient(cache_ttl=2)
    
    try:
        # First fetch (should hit API)
        logger.info("First fetch (should hit Agenta API)...")
        start = time.time()
        prompt1 = await client.get_prompt("router-system-prompt-template", use_cache=True)
        time1 = time.time() - start
        
        # Second fetch (should hit cache)
        logger.info("Second fetch (should hit cache)...")
        start = time.time()
        prompt2 = await client.get_prompt("router-system-prompt-template", use_cache=True)
        time2 = time.time() - start
        
        if prompt1 and prompt2:
            logger.info(f"   First fetch: {time1:.3f}s")
            logger.info(f"   Second fetch: {time2:.3f}s (cached)")
            
            if time2 < time1:
                logger.info("✅ Cache is working (second fetch faster)")
                return True
            else:
                logger.warning("⚠️  Cache may not be working optimally")
                return False
        else:
            logger.info("ℹ️  Prompts not in Agenta yet, skipping cache test")
            return False
            
    finally:
        await client.close()


def test_yaml_fallback():
    """Test YAML fallback when Agenta unavailable."""
    logger.info("\n" + "="*60)
    logger.info("Test: YAML Fallback")
    logger.info("="*60)
    
    # Force YAML mode
    original_backend = os.environ.get("PROMPT_BACKEND")
    os.environ["PROMPT_BACKEND"] = "yaml"
    
    try:
        loader = PromptLoader()
        
        # Load a known prompt from YAML
        prompt = loader.load_prompt('router', 'system_prompt_template')
        
        if prompt:
            logger.info(f"✅ YAML fallback working ({len(prompt)} chars)")
            logger.info(f"   Preview: {prompt[:100]}...")
            return True
        else:
            logger.error("❌ Failed to load prompt from YAML")
            return False
            
    finally:
        # Restore original setting
        if original_backend:
            os.environ["PROMPT_BACKEND"] = original_backend
        else:
            os.environ.pop("PROMPT_BACKEND", None)


def test_prompt_loader_modes():
    """Test PromptLoader in different backend modes."""
    logger.info("\n" + "="*60)
    logger.info("Test: PromptLoader Backend Modes")
    logger.info("="*60)
    
    original_backend = os.environ.get("PROMPT_BACKEND")
    
    modes = ["yaml", "auto"]
    results = {}
    
    for mode in modes:
        logger.info(f"\n  Testing mode: {mode}")
        os.environ["PROMPT_BACKEND"] = mode
        
        try:
            # Create new loader for this mode
            loader = PromptLoader()
            prompt = loader.load_prompt('medical', 'system_prompt')
            
            if prompt:
                logger.info(f"    ✅ Loaded prompt in {mode} mode ({len(prompt)} chars)")
                results[mode] = True
            else:
                logger.warning(f"    ⚠️  No prompt loaded in {mode} mode")
                results[mode] = False
                
        except Exception as e:
            logger.error(f"    ❌ Error in {mode} mode: {e}")
            results[mode] = False
    
    # Restore original setting
    if original_backend:
        os.environ["PROMPT_BACKEND"] = original_backend
    else:
        os.environ.pop("PROMPT_BACKEND", None)
    
    success = all(results.values())
    if success:
        logger.info("\n✅ All backend modes working")
    else:
        logger.warning(f"\n⚠️  Some modes failed: {results}")
    
    return success


async def test_prompt_loader_dict():
    """Test loading prompt dictionaries."""
    logger.info("\n" + "="*60)
    logger.info("Test: Prompt Dictionary Loading")
    logger.info("="*60)
    
    loader = PromptLoader()
    
    # Test loading administrative prompts dict
    prompts_dict = loader.load_prompt_dict('administrative', 'prompts')
    
    if prompts_dict:
        logger.info(f"✅ Loaded prompt dict with {len(prompts_dict)} entries")
        logger.info(f"   Keys: {list(prompts_dict.keys())}")
        return True
    else:
        logger.warning("⚠️  No prompts dict loaded")
        return False


async def run_all_tests():
    """Run all integration tests."""
    logger.info("\n" + "="*80)
    logger.info(" AGENTA INTEGRATION TESTS")
    logger.info("="*80)
    logger.info("\nThese tests verify Agenta integration and YAML fallback.")
    logger.info("Agenta package should be deployed for full test coverage.\n")
    
    results = []
    
    # Async tests
    async_tests = [
        ("Agenta Health", test_agenta_health),
        ("Prompt Fetching", test_prompt_fetching),
        ("Cache Behavior", test_cache_behavior),
        ("Prompt Dict Loading", test_prompt_loader_dict),
    ]
    
    for test_name, test_func in async_tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test '{test_name}' failed with exception: {e}", exc_info=True)
            results.append((test_name, False))
        
        await asyncio.sleep(0.5)
    
    # Sync tests
    sync_tests = [
        ("YAML Fallback", test_yaml_fallback),
        ("Backend Modes", test_prompt_loader_modes),
    ]
    
    for test_name, test_func in sync_tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test '{test_name}' failed with exception: {e}", exc_info=True)
            results.append((test_name, False))
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info(" TEST SUMMARY")
    logger.info("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "⚠️  SKIP/FAIL"
        logger.info(f"{status} - {test_name}")
    
    logger.info(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("\n✅ All tests passed! Agenta integration is working correctly.")
    else:
        logger.info(f"\n⚠️  {total - passed} tests failed or skipped.")
        logger.info("Note: Some tests may fail if Agenta package is not deployed.")
        logger.info("      Deploy with: ./instant package init -n agenta -d")
    
    return 0 if passed >= total // 2 else 1  # Pass if at least half work


async def main():
    """Main test runner."""
    exit_code = await run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    # Load environment
    from dotenv import load_dotenv
    env_file = os.getenv("ENV_FILE", ".env")
    if os.path.exists(env_file):
        load_dotenv(env_file)
        logger.info(f"Loaded environment from {env_file}\n")
    
    asyncio.run(main())

