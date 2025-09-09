#!/usr/bin/env python3
"""
Medical Agent Server
Runs the Medical Q&A agent as an A2A-compliant service
"""

import uvicorn
import logging
from dotenv import load_dotenv

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCard, TransportProtocol, AgentSkill
from .medical_executor import MedicalExecutor
from ..config import a2a_endpoints
from .router_executor import load_agent_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_medical_server():
    """Creates and returns the FastAPI application for the Medical agent."""
    # Load agent card from YAML config
    config = load_agent_config('medical')
    card_data = config.get('card', {})

    # Convert skills data to AgentSkill objects
    skills_data = card_data.get('skills', [])
    card_data['skills'] = [AgentSkill(**skill) for skill in skills_data]

    # Ensure URL reflects runtime host/port from config
    agent_card = AgentCard(**card_data)
    agent_card.url = a2a_endpoints.medgemma_url
    # Ensure compatible transport preference
    agent_card.preferred_transport = TransportProtocol.jsonrpc

    # Create executor and request handler
    agent_executor = MedicalExecutor()
    request_handler = DefaultRequestHandler(
        agent_executor=agent_executor,
        task_store=InMemoryTaskStore()
    )

    # Create and run server (Starlette application exposes .well-known)
    server = A2AStarletteApplication(
        agent_card=agent_card,
        http_handler=request_handler
    )

    logger.info("Medical agent server application created.")
    return server.build()


app = create_medical_server()

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=9101)


