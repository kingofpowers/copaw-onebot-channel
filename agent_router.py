# -*- coding: utf-8 -*-
"""Global MultiAgentManager access for OneBot Channel routing.

This module provides a global access point for MultiAgentManager,
allowing OneBot Channel to route messages to different agents.
"""

import logging
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from copaw.app.multi_agent_manager import MultiAgentManager

logger = logging.getLogger(__name__)

# Global reference to MultiAgentManager
_multi_agent_manager: Optional["MultiAgentManager"] = None


def set_multi_agent_manager(manager: "MultiAgentManager") -> None:
    """Set the global MultiAgentManager reference.

    Called during app startup to allow channels to access the manager.
    """
    global _multi_agent_manager
    _multi_agent_manager = manager
    logger.debug("Global MultiAgentManager reference set")


def get_multi_agent_manager() -> Optional["MultiAgentManager"]:
    """Get the global MultiAgentManager reference.

    Returns None if not set (e.g., during testing or legacy mode).
    """
    global _multi_agent_manager

    # If already set, return it
    if _multi_agent_manager is not None:
        return _multi_agent_manager

    # Try to get or create a MultiAgentManager instance
    # This allows the channel to work even if not explicitly set
    try:
        from copaw.app.multi_agent_manager import MultiAgentManager

        _multi_agent_manager = MultiAgentManager()
        logger.info("Created new MultiAgentManager instance for OneBot routing")
        return _multi_agent_manager
    except Exception as e:
        logger.warning(f"Failed to create MultiAgentManager: {e}")
        return None


async def get_agent_runner(agent_id: str):
    """Get the Runner for a specific agent.

    Args:
        agent_id: Agent ID to get runner for

    Returns:
        AgentRunner for the specified agent, or None if not available
    """
    manager = get_multi_agent_manager()
    if not manager:
        logger.warning("MultiAgentManager not available, using default process")
        return None

    try:
        workspace = await manager.get_agent(agent_id)
        if workspace and workspace.runner:
            logger.debug(f"Got runner for agent {agent_id}")
            return workspace.runner
    except ValueError as e:
        logger.warning(f"Agent {agent_id} not found in configuration: {e}")
    except Exception as e:
        logger.error(f"Failed to get agent {agent_id}: {e}")

    return None
