"""
Router Agent Server for A2A SDK
Sets up the FastAPI application for the Router agent.
"""

import uvicorn
import logging
import json
from pathlib import Path
from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCapabilities, TransportProtocol, AgentCard, AgentSkill
from .dispatch_executor import DispatchingExecutor
from ..config import a2a_endpoints
from .router_executor import load_agent_config

logger = logging.getLogger(__name__)


def create_router_server():
    """
    Creates and returns the FastAPI application for the Router agent.
    """
    agent_executor = DispatchingExecutor()
    request_handler = DefaultRequestHandler(
        agent_executor=agent_executor,
        task_store=InMemoryTaskStore()
    )
    
    # Load agent card from YAML config
    config = load_agent_config('router')
    card_data = config.get('card', {})
    
    # Convert skills data to AgentSkill objects
    skills_data = card_data.get('skills', [])
    card_data['skills'] = [AgentSkill(**skill) for skill in skills_data]
    
    agent_card = AgentCard(**card_data)
    # Ensure runtime URL is sourced from config
    agent_card.url = a2a_endpoints.router_url
    # Ensure compatible transport preference
    agent_card.preferred_transport = TransportProtocol.jsonrpc
    agent_card.capabilities = AgentCapabilities(streaming=True)
    
    # Create the server application (Starlette to expose .well-known)
    server_app = A2AStarletteApplication(
        agent_card=agent_card,
        http_handler=request_handler,
    )
    
    logger.info("Router A2A server application created.")
    return server_app.build()


app = create_router_server()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=9100)
