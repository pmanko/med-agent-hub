"""
Administrative Agent Server for A2A SDK
Sets up the FastAPI application for the Administrative agent.
"""

import uvicorn
import logging
from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import TransportProtocol
from .administrative_executor import AdministrativeExecutor

logger = logging.getLogger(__name__)


def create_administrative_server():
    """
    Creates and returns the FastAPI application for the Administrative agent.
    """
    agent_executor = AdministrativeExecutor()
    request_handler = DefaultRequestHandler(
        agent_executor=agent_executor,
        task_store=InMemoryTaskStore(),
    )

    # Get agent card from executor
    agent_card = agent_executor.get_agent_card()
    agent_card.preferred_transport = TransportProtocol.jsonrpc

    # Create server app
    server_app = A2AStarletteApplication(
        agent_card=agent_card,
        http_handler=request_handler,
    )

    logger.info("Administrative A2A server application created.")
    return server_app.build()


app = create_administrative_server()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=9103)



