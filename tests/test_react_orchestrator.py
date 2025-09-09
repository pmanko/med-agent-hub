#!/usr/bin/env python3
"""
Targeted test script for the ReAct Orchestrator.
"""

import asyncio
import logging
import httpx
import os
import sys
from uuid import uuid4
from a2a.client import ClientFactory, ClientConfig
from a2a.types import AgentCard, Message, Role, Part, TextPart, TransportProtocol, Task, TaskState

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def fetch_agent_card(base_url: str, httpx_client: httpx.AsyncClient) -> AgentCard:
    """Fetches agent card from the .well-known endpoint."""
    url = f"{base_url.rstrip('/')}/.well-known/agent-card.json"
    try:
        resp = await httpx_client.get(url)
        resp.raise_for_status()
        return AgentCard(**resp.json())
    except Exception as e:
        logger.error(f"Could not fetch AgentCard from {url}: {e}")
        raise

async def test_react_orchestration():
    """Tests the ReAct orchestrator with a multi-step query."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing ReAct Orchestrator")
    logger.info("=" * 60)

    router_url = os.getenv("A2A_ROUTER_URL", "http://localhost:9100")

    query = "I want to know what major us cities have high risk of dangerous hot weather spells, and what best to do to prevent heatstroke in these places."

    httpx_client = httpx.AsyncClient(timeout=300.0)
    try:
        card = await fetch_agent_card(router_url, httpx_client)
        client = ClientFactory(ClientConfig(
            httpx_client=httpx_client,
            supported_transports=[TransportProtocol.jsonrpc],
        )).create(card)

        logger.info(f"Sending multi-step query to ReAct orchestrator: \"{query}\"")

        message = Message(
            role=Role.user,
            parts=[Part(root=TextPart(text=query))],
            messageId=str(uuid4()),
            metadata={"orchestrator_mode": "react"} # Explicitly request ReAct mode
        )

        final_task: Task = None
        async for event in client.send_message(message):
            task_event = event[0] if isinstance(event, tuple) else event
            if isinstance(task_event, Task):
                final_task = task_event

            # Log progress
            if hasattr(task_event, 'status') and hasattr(task_event.status, 'message') and task_event.status.message:
                progress_text = task_event.status.message.parts[0].root.text
                logger.info(f"  -> Progress: {progress_text}")

        assert final_task is not None, "Did not receive a final task object."
        assert final_task.status.state == TaskState.completed, f"Task did not complete successfully. State: {final_task.status.state}"

        final_artifact = final_task.artifacts[-1]
        final_response = final_artifact.parts[0].root.text

        logger.info(f"\nFinal synthesized response:\n{final_response}")

        # Assertions to check if the response contains elements from both agents
        response_lower = final_response.lower()
        assert "heatstroke" in response_lower, "Response should contain info on heatstroke."
        assert "cities" in response_lower or "phoenix" in response_lower or "dallas" in response_lower, "Response should contain info on cities."
        assert "weather" in response_lower or "hot" in response_lower, "Response should contain weather info."

        logger.info("\n✅ ReAct orchestrator test passed!")

    except Exception as e:
        logger.error(f"ReAct orchestrator test failed: {e}", exc_info=True)
        raise
    finally:
        await httpx_client.aclose()


async def main():
    logger.info("Starting ReAct orchestrator test...")
    # Give services a moment to be ready
    await asyncio.sleep(2)
    await test_react_orchestration()

if __name__ == "__main__":
    env_file = "env.recommended"
    if "--env-file" in sys.argv:
        try:
            index = sys.argv.index("--env-file") + 1
            env_file = sys.argv[index]
        except (ValueError, IndexError):
            pass

    from dotenv import load_dotenv
    load_dotenv(env_file)

    asyncio.run(main())
