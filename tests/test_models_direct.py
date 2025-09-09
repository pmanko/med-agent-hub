#!/usr/bin/env python3
"""
Direct agent tests for boot-up, card retrieval, and simple queries.
- MedGemma agent (medical Q&A)
- Clinical Research agent (general clinical question)
- Router agent (simple routing task)

Usage:
  poetry run python test_models_direct.py --env-file env.recommended
"""

import asyncio
import logging
import httpx
import os
import sys
from uuid import uuid4
from a2a.client import ClientFactory, ClientConfig
from a2a.types import AgentCard, Message, Role, Part, TextPart, TransportProtocol

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def fetch_agent_card(base_url: str, httpx_client: httpx.AsyncClient) -> AgentCard:
    """Fetches agent card, trying both new and deprecated endpoints."""
    for path in ("/.well-known/agent-card.json", "/.well-known/agent.json"):
        try:
            resp = await httpx_client.get(f"{base_url}{path}")
            if resp.status_code == 200:
                if path.endswith("agent.json"):
                    logger.warning(f"Agent at {base_url} is using a deprecated agent card path.")
                return AgentCard(**resp.json())
        except (httpx.ConnectError, httpx.ReadTimeout):
            raise  # Immediately fail on connection errors
        except Exception:
            continue
    raise RuntimeError(f"Could not fetch a valid AgentCard from {base_url}")


async def test_agent(name: str, url: str, query: str) -> bool:
    """Generic test function for an A2A agent."""
    logger.info("\n" + "=" * 60)
    logger.info(f"Testing Agent: {name} at {url}")
    logger.info("=" * 60)
    
    httpx_client = httpx.AsyncClient(timeout=180)
    try:
        # 1. Test Boot-up and Card Retrieval
        logger.info("Step 1: Fetching agent card...")
        card = await fetch_agent_card(url, httpx_client)
        logger.info(f"  -> SUCCESS: Retrieved card for '{card.name}'")

        # 2. Test Simple Query
        logger.info(f"Step 2: Sending test query: '{query}'")
        client = ClientFactory(ClientConfig(
            httpx_client=httpx_client,
            supported_transports=[TransportProtocol.jsonrpc],
            use_client_preference=False,
        )).create(card)
        
        msg = Message(role=Role.user, parts=[Part(root=TextPart(text=query))], messageId=str(uuid4()))
        
        final_response_text = None
        async for event in client.send_message(msg):
            # Extract text from the last artifact for verification
            task = event[0] if isinstance(event, tuple) else event
            if task and getattr(task, 'artifacts', None):
                last_artifact = task.artifacts[-1]
                if last_artifact and getattr(last_artifact, 'parts', None):
                    text_part = last_artifact.parts[0].root
                    if isinstance(text_part, TextPart):
                        final_response_text = text_part.text
        
        if final_response_text:
            logger.info(f"  -> SUCCESS: Received response: '{final_response_text[:150]}...'")
            return True
        else:
            logger.error("  -> FAILED: Agent returned no valid response text.")
            return False

    except Exception as e:
        logger.error(f"  -> FAILED: Test for '{name}' failed with an exception: {e}", exc_info=True)
        return False
    finally:
        await httpx_client.aclose()


async def test_llm_direct_connection() -> bool:
    """Tests the direct connection to the LLM endpoint to verify network and config."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Direct Connection to LLM")
    logger.info("=" * 60)
    
    base_url = os.getenv("LLM_BASE_URL")
    model = os.getenv("ORCHESTRATOR_MODEL")
    api_key = os.getenv("LLM_API_KEY", "")

    if not base_url or not model:
        logger.error("  -> FAILED: LLM_BASE_URL or ORCHESTRATOR_MODEL not configured.")
        return False
        
    logger.info(f"Attempting to connect to {base_url} with model {model}...")
    
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Say hello."}],
        "max_tokens": 10,
    }
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(f"{base_url.rstrip('/')}/v1/chat/completions", json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if text:
                logger.info(f"  -> SUCCESS: Received response from LLM: '{text.strip()}'")
                return True
            else:
                logger.error("  -> FAILED: LLM returned an empty response.")
                return False
    except httpx.ConnectError as e:
        logger.error(f"  -> FAILED: Connection to LLM failed. Is LM Studio running and accessible at {base_url}?")
        logger.error(f"     Details: {e}")
        return False
    except Exception as e:
        logger.error(f"  -> FAILED: An unexpected error occurred while contacting the LLM: {e}", exc_info=True)
        return False


async def main():
    logger.info("Starting A2A agent tests...")

    # Step 1: Verify core connectivity to the LLM first.
    llm_ok = await test_llm_direct_connection()
    if not llm_ok:
        logger.error("Cannot proceed with agent tests until LLM connection is resolved.")
        sys.exit(1)
    
    # Load agent URLs from config
    router_url = os.getenv("A2A_ROUTER_URL", "http://localhost:9100")
    medgemma_url = os.getenv("A2A_MEDGEMMA_URL", "http://localhost:9101")
    clinical_url = os.getenv("A2A_CLINICAL_URL", "http://localhost:9102")

    tests = [
        ("Router Agent", router_url, "What are the symptoms of diabetes?"),
        ("MedGemma Agent", medgemma_url, "What are the symptoms of hypertension?"),
        ("Clinical Research Agent", clinical_url, "Explain what a randomized controlled trial is."),
    ]

    results = []
    for name, url, query in tests:
        # Run tests one by one to avoid overwhelming the local LLM
        result = await test_agent(name=name, url=url, query=query)
        results.append(result)
    
    success_count = sum(1 for r in results if r)
    logger.info("\n" + "=" * 60)
    logger.info(f"Test Summary: {success_count} out of {len(tests)} agents passed.")
    logger.info("=" * 60)

    if success_count != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    # Optional --env-file loader
    env_file = None
    if "--env-file" in sys.argv:
        try:
            index = sys.argv.index("--env-file") + 1
            env_file = sys.argv[index]
        except (ValueError, IndexError):
            pass
    
    if env_file:
        from dotenv import load_dotenv
        if load_dotenv(env_file):
            logger.info(f"Loaded environment variables from {env_file}")
        else:
            logger.error(f"Could not load environment file: {env_file}")
            sys.exit(1)
            
    asyncio.run(main())


