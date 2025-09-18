#!/usr/bin/env python3
"""
Router-only A2A tests:
- Send MedGemma-suitable question through Router
- Small end-to-end conversation routed via Router
"""

import asyncio
import logging
import httpx
import os
from uuid import uuid4
from a2a.client import ClientFactory, ClientConfig
from a2a.types import AgentCard, Message, Role, Part, TextPart, TransportProtocol


async def fetch_agent_card(base_url: str, httpx_client: httpx.AsyncClient) -> AgentCard:
    for path in ("/.well-known/agent.json", "/.well-known/agent-card.json"):
        try:
            resp = await httpx_client.get(f"{base_url}{path}")
            if resp.status_code == 200:
                return AgentCard(**resp.json())
        except Exception:
            continue
    raise RuntimeError(f"Could not fetch AgentCard from {base_url}")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_router_med_question():
    logger.info("\n" + "=" * 60)
    logger.info("Router: MedGemma-style question")
    logger.info("=" * 60)

    httpx_client = httpx.AsyncClient(timeout=180)
    router_url = os.getenv("A2A_ROUTER_URL", "http://localhost:9100")
    card = await fetch_agent_card(router_url, httpx_client)
    client = ClientFactory(ClientConfig(
        httpx_client=httpx_client,
        supported_transports=[TransportProtocol.jsonrpc],
        use_client_preference=False,
    )).create(card)
    try:
        msg = Message(role=Role.user, parts=[Part(root=TextPart(text="What are common symptoms of hypertension?"))], messageId=str(uuid4()))
        last = None
        async for ev in client.send_message(msg):
            last = str(ev)
        logger.info(f"Router last event: {(last or '<none>')[:600]}")
    finally:
        await client.close()
        await httpx_client.aclose()


async def test_router_conversation():
    logger.info("\n" + "=" * 60)
    logger.info("Router: short conversation")
    logger.info("=" * 60)

    httpx_client = httpx.AsyncClient(timeout=180)
    router_url = os.getenv("A2A_ROUTER_URL", "http://localhost:9100")
    card = await fetch_agent_card(router_url, httpx_client)
    client = ClientFactory(ClientConfig(
        httpx_client=httpx_client,
        supported_transports=[TransportProtocol.jsonrpc],
        use_client_preference=False,
    )).create(card)
    try:
        for q in [
            "What is hypertension?",
            "What medications are typically prescribed for high blood pressure?",
        ]:
            msg = Message(role=Role.user, parts=[Part(root=TextPart(text=q))], messageId=str(uuid4()))
            last = None
            async for ev in client.send_message(msg):
                last = str(ev)
            logger.info(f"Router turn result: {(last or '<none>')[:600]}")
    finally:
        await client.close()
        await httpx_client.aclose()


async def main():
    logger.info("Waiting for Router...")
    await asyncio.sleep(3)
    await test_router_med_question()
    await test_router_conversation()


if __name__ == "__main__":
    import sys
    env_file = None
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--env-file" and i + 1 < len(sys.argv[1:]):
            env_file = sys.argv[i + 2]
            break
    if env_file:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    asyncio.run(main())


