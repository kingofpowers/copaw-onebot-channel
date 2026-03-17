# -*- coding: utf-8 -*-
"""OneBot Channel configuration."""
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class OneBotInstanceConfig(BaseModel):
    """Configuration for a single OneBot instance (NapCat)."""

    name: str = Field(
        default="default",
        description="Instance name for identification",
    )
    ws_url: str = Field(
        default="",
        description="WebSocket URL (e.g., ws://127.0.0.1:3001)",
    )
    access_token: str = Field(
        default="",
        description="Access token for authentication",
    )
    enabled: bool = Field(
        default=True,
        description="Whether this instance is enabled",
    )
    qq_id: int = Field(
        default=0,
        description="Bot's QQ number for @ mention detection (auto-detected if not set)",
    )
    require_mention: bool = Field(
        default=True,
        description="Whether bot must be @ mentioned in group messages (default: True)",
    )


class OneBotRoutingRule(BaseModel):
    """A single routing rule mapping message source to agent."""

    match: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Match conditions. Supported keys: "
            "instance, group_id, user_id, message_type"
        ),
    )
    agent_id: str = Field(
        default="default",
        description="Target agent ID for matched messages",
    )


class AgentOutputOptions(BaseModel):
    """Output options for a specific agent."""

    show_reply: bool = Field(
        default=True,
        description="Whether to show agent reply messages",
    )
    show_thinking: bool = Field(
        default=False,
        description="Whether to show thinking/reasoning content",
    )
    show_tool_calls: bool = Field(
        default=False,
        description="Whether to show tool call details (call and output)",
    )


class OutputOptions(BaseModel):
    """Output options configuration for message rendering.
    
    Controls what content is sent to users for different agents.
    """

    show_reply: bool = Field(
        default=True,
        description="Default: show agent reply messages",
    )
    show_thinking: bool = Field(
        default=False,
        description="Default: hide thinking/reasoning content",
    )
    show_tool_calls: bool = Field(
        default=False,
        description="Default: hide tool call details",
    )
    agents: Dict[str, AgentOutputOptions] = Field(
        default_factory=dict,
        description="Per-agent output options (override defaults)",
    )

    def get_options(self, agent_id: str) -> AgentOutputOptions:
        """Get output options for a specific agent.
        
        Args:
            agent_id: Agent identifier
            
        Returns:
            AgentOutputOptions with agent-specific or default values
        """
        if agent_id in self.agents:
            return self.agents[agent_id]
        # Return defaults
        return AgentOutputOptions(
            show_reply=self.show_reply,
            show_thinking=self.show_thinking,
            show_tool_calls=self.show_tool_calls,
        )


class OneBotChannelConfig(BaseModel):
    """OneBot channel configuration.

    Add this to config.json under "channels" section:

    {
      "channels": {
        "onebot": {
          "enabled": true,
          "instances": [...],
          "routing_rules": [...],
          ...
        }
      }
    }
    """

    enabled: bool = Field(
        default=False,
        description="Whether OneBot channel is enabled",
    )
    bot_prefix: str = Field(
        default="",
        description="Prefix to add to bot messages",
    )
    instances: List[OneBotInstanceConfig] = Field(
        default_factory=list,
        description="List of OneBot (NapCat) instances",
    )
    routing_rules: List[OneBotRoutingRule] = Field(
        default_factory=list,
        description="Rules for routing messages to different agents",
    )
    default_agent: str = Field(
        default="default",
        description="Default agent ID for unmatched messages",
    )
    media_dir: str = Field(
        default="",
        description="Directory for storing received media files",
    )
    dm_policy: Literal["open", "allowlist"] = Field(
        default="open",
        description="Direct message policy",
    )
    group_policy: Literal["open", "allowlist"] = Field(
        default="open",
        description="Group message policy",
    )
    allow_from: List[str] = Field(
        default_factory=list,
        description="Allowed user/group IDs (for allowlist policy)",
    )
    deny_message: str = Field(
        default="",
        description="Message to send when access is denied",
    )
    require_mention: bool = Field(
        default=True,
        description="Whether bot must be @ mentioned in group messages (default: True, can be overridden per instance)",
    )
    output_options: Optional[OutputOptions] = Field(
        default=None,
        description="Output options for message rendering (reply, thinking, tool calls)",
    )
