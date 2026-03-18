# -*- coding: utf-8 -*-
# pylint: disable=too-many-branches,too-many-statements
"""OneBot Channel.

Implements OneBot 11 protocol for QQ bots via NapCatQQ.
Supports multiple WebSocket connections (multiple bot instances).
Routes messages to different agents based on routing rules.

Protocol: https://github.com/botuniverse/onebot-11
NapCat: https://napneko.github.io/
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import aiohttp

from agentscope_runtime.engine.schemas.agent_schemas import (
    TextContent,
    ImageContent,
    FileContent,
    ContentType,
)

from copaw.app.channels.base import (
    BaseChannel,
    OnReplySent,
    OutgoingContentPart,
    ProcessHandler,
)
from copaw.app.channels.schema import ChannelType
from copaw.config.utils import WORKING_DIR

if TYPE_CHECKING:
    from agentscope_runtime.engine.schemas.agent_schemas import AgentRequest

logger = logging.getLogger("copaw.onebot")

# Channel type identifier
CHANNEL_TYPE_ONEBOT: ChannelType = "onebot"

# Media directory for received files
DEFAULT_MEDIA_DIR = WORKING_DIR / "media" / "onebot"

# Global message deduplication cache (shared across all OneBotChannel instances)
# Key: "bot_id:message_type:group_or_user_id:message_id"
_GLOBAL_PROCESSED_MESSAGES: set = set()
_MAX_DEDUP_CACHE_SIZE = 2000

# Global registry for active OneBot channels (by bot QQ number)
# This allows tools to access the channel without knowing tokens
_active_channels: Dict[int, "OneBotChannel"] = {}

# Global connection manager - shared across all OneBotChannel instances
# This ensures only one WebSocket connection per NapCat instance
# Key: instance name (e.g., "napcat", "napcat2")
_global_connections: Dict[str, Dict[str, Any]] = {}
_global_connection_lock = asyncio.Lock()
_global_connection_refs: Dict[str, int] = {}  # Reference count per instance
_pending_api_calls: Dict[str, asyncio.Future] = {}
_api_call_counter = 0
_api_call_lock = asyncio.Lock()

# WebSocket reconnection settings
RECONNECT_DELAYS = [1, 2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 100

# API response codes
RETCODE_OK = 0


@dataclass
class OneBotInstance:
    """Configuration for a single OneBot instance (NapCat)."""
    name: str
    ws_url: str
    access_token: str = ""
    enabled: bool = True
    
    # Bot identification and behavior
    qq_id: int = 0  # Bot's QQ number for @ detection
    require_mention: bool = True  # Default: only respond to @ mentions in group chats
    
    # Per-group @ mention policy: {group_id: require_mention}
    # Overrides require_mention for specific groups
    # Example: {"123456": false, "789012": true}
    group_mention_policy: Dict[int, bool] = field(default_factory=dict)
    
    # Content type emoji markers (prepended to content for identification)
    # These are just markers, not filters. Sending is controlled by filter config.
    thinking_emoji: str = "🤔"  # Thinking/reasoning content
    tool_emoji: str = "📝"     # Tool call/result content
    skip_emoji: str = "💤"     # Bot can use this to mark meaningless content

    # Runtime state
    self_id: int = 0
    nickname: str = ""
    ws: Optional[aiohttp.ClientWebSocketResponse] = None
    session: Optional[aiohttp.ClientSession] = None
    heartbeat_task: Optional[asyncio.Task] = None
    receive_task: Optional[asyncio.Task] = None
    reconnect_count: int = 0
    last_heartbeat_ack: float = 0.0


@dataclass
class RoutingRule:
    """A single routing rule mapping message source to agent."""
    match: Dict[str, Any]
    agent_id: str


@dataclass
class OneBotConfig:
    """Configuration for OneBot channel."""
    enabled: bool = False
    bot_prefix: str = ""
    instances: List[Dict[str, Any]] = field(default_factory=list)
    routing_rules: List[Dict[str, Any]] = field(default_factory=list)
    default_agent: str = "default"
    media_dir: str = ""

    # Per-instance configs (parsed from instances list)
    _parsed_instances: List[OneBotInstance] = field(default_factory=list, repr=False)
    _parsed_rules: List[RoutingRule] = field(default_factory=list, repr=False)


class OneBotChannel(BaseChannel):
    """OneBot 11 channel: WebSocket receive/send.

    Supports multiple NapCat instances, each with its own WebSocket connection.
    Routes messages to different agents based on routing rules.

    Session ID format (uses bot QQ number for portability):
    - Group chat: onebot:group:{bot_qq}:{group_id}
    - Private chat: onebot:private:{bot_qq}:{user_id}
    """

    channel = CHANNEL_TYPE_ONEBOT

    def __init__(
        self,
        process: ProcessHandler,
        enabled: bool = False,
        instances: Optional[List[Dict[str, Any]]] = None,
        routing_rules: Optional[List[Dict[str, Any]]] = None,
        default_agent: str = "default",
        bot_prefix: str = "",
        media_dir: str = "",
        on_reply_sent: OnReplySent = None,
        show_tool_details: bool = True,
        filter_tool_messages: bool = False,
        filter_thinking: bool = False,
        dm_policy: str = "open",
        group_policy: str = "open",
        allow_from: Optional[list] = None,
        deny_message: str = "",
        require_mention: bool = False,
        output_options: Optional[Dict[str, Any]] = None,
        # Top-level emoji config (can be overridden per-instance)
        thinking_emoji: str = "🤔",
        tool_emoji: str = "📝",
        skip_emoji: str = "💤",
    ):
        super().__init__(
            process=process,
            on_reply_sent=on_reply_sent,
            show_tool_details=show_tool_details,
            filter_tool_messages=filter_tool_messages,
            filter_thinking=filter_thinking,
            dm_policy=dm_policy,
            group_policy=group_policy,
            allow_from=set(allow_from or []),
            deny_message=deny_message,
            require_mention=require_mention,
        )

        self._enabled = enabled
        self._bot_prefix = bot_prefix or ""
        self._default_agent = default_agent or "default"
        self._media_dir = Path(media_dir) if media_dir else DEFAULT_MEDIA_DIR
        self._media_dir.mkdir(parents=True, exist_ok=True)

        # Store top-level emoji config
        self._thinking_emoji = thinking_emoji
        self._tool_emoji = tool_emoji
        self._skip_emoji = skip_emoji

        # Parse output options
        self._output_options = self._parse_output_options(output_options)

        # Parse instances
        self._instances: Dict[str, OneBotInstance] = {}
        if instances:
            for inst_cfg in instances:
                # Parse group_mention_policy, convert string keys to int
                group_policy = inst_cfg.get("group_mention_policy", {}) or {}
                group_mention_policy = {
                    int(k): v for k, v in group_policy.items()
                }
                
                inst = OneBotInstance(
                    name=inst_cfg.get("name", "default"),
                    ws_url=inst_cfg.get("ws_url", ""),
                    access_token=inst_cfg.get("access_token", ""),
                    enabled=inst_cfg.get("enabled", True),
                    qq_id=inst_cfg.get("qq_id", 0),
                    require_mention=inst_cfg.get("require_mention", True),
                    group_mention_policy=group_mention_policy,
                    # Use instance-specific emoji or fall back to top-level
                    thinking_emoji=inst_cfg.get("thinking_emoji", thinking_emoji),
                    tool_emoji=inst_cfg.get("tool_emoji", tool_emoji),
                    skip_emoji=inst_cfg.get("skip_emoji", skip_emoji),
                )
                if inst.ws_url:
                    self._instances[inst.name] = inst

        # Parse routing rules
        self._routing_rules: List[RoutingRule] = []
        if routing_rules:
            for rule_cfg in routing_rules:
                rule = RoutingRule(
                    match=rule_cfg.get("match", {}),
                    agent_id=rule_cfg.get("agent_id", self._default_agent),
                )
                self._routing_rules.append(rule)

        # HTTP session for media downloads
        self._http: Optional[aiohttp.ClientSession] = None

        # Running flag
        self._running = False

        # Lock for WebSocket operations
        self._ws_lock = asyncio.Lock()

    @classmethod
    def from_env(
        cls,
        process: ProcessHandler,
        on_reply_sent: OnReplySent = None,
    ) -> "OneBotChannel":
        """Create channel from environment variables."""
        import os

        # Parse instances from env (comma-separated URLs)
        instances = []
        ws_urls = os.getenv("ONEBOT_WS_URLS", "")
        tokens = os.getenv("ONEBOT_ACCESS_TOKENS", "")

        if ws_urls:
            url_list = [u.strip() for u in ws_urls.split(",") if u.strip()]
            token_list = [t.strip() for t in tokens.split(",")] if tokens else []
            for i, url in enumerate(url_list):
                instances.append({
                    "name": f"bot-{i+1}",
                    "ws_url": url,
                    "access_token": token_list[i] if i < len(token_list) else "",
                })

        return cls(
            process=process,
            enabled=os.getenv("ONEBOT_ENABLED", "0") == "1",
            instances=instances,
            bot_prefix=os.getenv("ONEBOT_BOT_PREFIX", ""),
            default_agent=os.getenv("ONEBOT_DEFAULT_AGENT", "default"),
            media_dir=os.getenv("ONEBOT_MEDIA_DIR", ""),
            on_reply_sent=on_reply_sent,
            dm_policy=os.getenv("ONEBOT_DM_POLICY", "open"),
            group_policy=os.getenv("ONEBOT_GROUP_POLICY", "open"),
            allow_from=os.getenv("ONEBOT_ALLOW_FROM", "").split(",") if os.getenv("ONEBOT_ALLOW_FROM") else [],
            deny_message=os.getenv("ONEBOT_DENY_MESSAGE", ""),
            require_mention=os.getenv("ONEBOT_REQUIRE_MENTION", "0") == "1",
        )

    @classmethod
    def from_config(
        cls,
        process: ProcessHandler,
        config: Any,
        on_reply_sent: OnReplySent = None,
        show_tool_details: bool = True,
        filter_tool_messages: bool = False,
        filter_thinking: bool = False,
    ) -> "OneBotChannel":
        """Create channel from config object.

        Args:
            process: Handler for agent requests.
            config: OneBot channel configuration (dict or object).
            on_reply_sent: Callback when reply is sent.
            show_tool_details: Whether to show tool execution details.
            filter_tool_messages: Whether to filter out tool messages.
            filter_thinking: Whether to filter thinking/reasoning blocks.

        Returns:
            Configured OneBotChannel instance.
        """
        # Support both dict and object config (Pydantic extra fields are dicts)
        if isinstance(config, dict):
            get_val = lambda key, default=None: config.get(key, default)
        else:
            get_val = lambda key, default=None: getattr(config, key, default)

        return cls(
            process=process,
            enabled=get_val("enabled", False),
            instances=get_val("instances", []) or [],
            routing_rules=get_val("routing_rules", []) or [],
            default_agent=get_val("default_agent", "default") or "default",
            bot_prefix=get_val("bot_prefix", "") or "",
            media_dir=get_val("media_dir", "") or "",
            on_reply_sent=on_reply_sent,
            show_tool_details=show_tool_details,
            filter_tool_messages=filter_tool_messages,
            filter_thinking=filter_thinking,
            dm_policy=get_val("dm_policy", "open") or "open",
            group_policy=get_val("group_policy", "open") or "open",
            allow_from=get_val("allow_from", []) or [],
            deny_message=get_val("deny_message", "") or "",
            require_mention=bool(get_val("require_mention", False)),
            output_options=get_val("output_options"),
            # Top-level emoji config
            thinking_emoji=get_val("thinking_emoji", "🤔") or "🤔",
            tool_emoji=get_val("tool_emoji", "📝") or "📝",
            skip_emoji=get_val("skip_emoji", "💤") or "💤",
        )

    @property
    def bot_prefix(self) -> str:
        return self._bot_prefix

    def _parse_output_options(
        self,
        output_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, bool]]:
        """Parse output options configuration.
        
        Args:
            output_options: Raw output options dict from config
            
        Returns:
            Dict mapping agent_id to its output options
        """
        from .config import OutputOptions, AgentOutputOptions
        
        result: Dict[str, Dict[str, bool]] = {}
        
        if not output_options:
            # Default: show reply, hide thinking and tool calls
            result["default"] = {
                "show_reply": True,
                "show_thinking": False,
                "show_tool_calls": False,
            }
            return result
        
        try:
            options = OutputOptions(**output_options)
            
            # Store default options
            result["default"] = {
                "show_reply": options.show_reply,
                "show_thinking": options.show_thinking,
                "show_tool_calls": options.show_tool_calls,
            }
            
            # Store per-agent options
            for agent_id, agent_opts in options.agents.items():
                result[agent_id] = {
                    "show_reply": agent_opts.show_reply,
                    "show_thinking": agent_opts.show_thinking,
                    "show_tool_calls": agent_opts.show_tool_calls,
                }
                
        except Exception as e:
            logger.warning(f"Failed to parse output_options: {e}, using defaults")
            result["default"] = {
                "show_reply": True,
                "show_thinking": False,
                "show_tool_calls": False,
            }
            
        return result

    def _get_output_options(self, agent_id: str) -> Dict[str, bool]:
        """Get output options for a specific agent.
        
        Args:
            agent_id: Agent identifier
            
        Returns:
            Dict with show_reply, show_thinking, show_tool_calls
        """
        if agent_id in self._output_options:
            return self._output_options[agent_id]
        return self._output_options.get("default", {
            "show_reply": True,
            "show_thinking": False,
            "show_tool_calls": False,
        })

    def resolve_session_id(
        self,
        sender_id: str,
        channel_meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate session ID from message meta.
        
        Format:
        - Group chat: onebot:group:{bot_qq}:{group_id}
        - Private chat: onebot:private:{bot_qq}:{user_id}
        
        Uses bot's QQ number (self_id) instead of instance name for portability.
        """
        meta = channel_meta or {}
        message_type = meta.get("message_type", "private")
        
        # Use bot's QQ number (self_id) for session ID
        bot_qq = meta.get("self_id", 0) or 0
        bot_id = str(bot_qq) if bot_qq else "unknown"

        if message_type == "group":
            group_id = meta.get("group_id", 0)
            return f"onebot:group:{bot_id}:{group_id}"
        else:
            return f"onebot:private:{bot_id}:{sender_id}"

    def build_agent_request_from_native(
        self,
        native_payload: Any,
    ) -> "AgentRequest":
        """Build AgentRequest from OneBot native message."""
        from agentscope_runtime.engine.schemas.agent_schemas import AgentRequest
        from agentscope_runtime.engine.schemas.agent_schemas import TextContent, ContentType

        payload = native_payload if isinstance(native_payload, dict) else {}
        sender_id = str(payload.get("sender_id", "") or payload.get("user_id", ""))
        content_parts = payload.get("content_parts", [])
        meta = payload.get("meta", {})

        # Get target agent from metadata (set by _handle_chat_message)
        target_agent = meta.get("target_agent", self._default_agent)

        # Only inject persona for fallback (when multi-agent routing is unavailable)
        # The actual routing happens in _run_process_loop
        try:
            from .agent_router import get_multi_agent_manager

            manager = get_multi_agent_manager()
            if not manager or target_agent == "default":
                # Fallback: inject persona for non-multi-agent mode
                if target_agent and target_agent != "default":
                    persona_prompt = self._get_agent_persona(target_agent)
                    if persona_prompt:
                        persona_content = TextContent(
                            type=ContentType.TEXT,
                            text=f"[系统提示：你现在扮演 {target_agent}，人格设定如下]\n{persona_prompt}\n[用户消息开始]",
                        )
                        content_parts = [persona_content] + content_parts
        except ImportError:
            # agent_router not available, use persona injection fallback
            if target_agent and target_agent != "default":
                persona_prompt = self._get_agent_persona(target_agent)
                if persona_prompt:
                    persona_content = TextContent(
                        type=ContentType.TEXT,
                        text=f"[系统提示：你现在扮演 {target_agent}，人格设定如下]\n{persona_prompt}\n[用户消息开始]",
                    )
                    content_parts = [persona_content] + content_parts

        session_id = self.resolve_session_id(sender_id, meta)

        request = self.build_agent_request_from_user_content(
            channel_id=self.channel,
            sender_id=sender_id,
            session_id=session_id,
            content_parts=content_parts,
            channel_meta=meta,
        )
        setattr(request, "channel_meta", meta)
        return request

    def _get_agent_persona(self, agent_id: str) -> Optional[str]:
        """Get persona prompt for an agent from workspace AGENTS.md."""
        from pathlib import Path

        # Check workspace directory
        workspace_dir = Path.home() / ".copaw" / "workspaces" / agent_id
        agents_file = workspace_dir / "AGENTS.md"

        if not agents_file.exists():
            # Fallback to working directory
            agents_file = Path(WORKING_DIR) / "workspaces" / agent_id / "AGENTS.md"

        if not agents_file.exists():
            logger.debug(f"No AGENTS.md found for agent {agent_id}")
            return None

        try:
            content = agents_file.read_text(encoding="utf-8")
            # Extract relevant persona info (skip the title)
            lines = content.strip().split("\n")
            # Filter out empty lines at the start and the first heading
            persona_lines = []
            skip_first_heading = True
            for line in lines:
                if skip_first_heading and line.startswith("# "):
                    skip_first_heading = False
                    continue
                persona_lines.append(line)
            return "\n".join(persona_lines).strip()
        except Exception as e:
            logger.warning(f"Failed to read persona for {agent_id}: {e}")
            return None

    async def _run_process_loop(
        self,
        request: "AgentRequest",
        to_handle: str,
        send_meta: Dict[str, Any],
    ) -> None:
        """Run process loop with multi-agent routing support.

        Overrides BaseChannel._run_process_loop to route messages to
        different agents based on routing rules. Falls back to default
        process if multi-agent routing is not available.
        """
        from agentscope_runtime.engine.schemas.agent_schemas import RunStatus

        # Get target agent from request metadata
        meta = getattr(request, "channel_meta", None) or {}
        target_agent = meta.get("target_agent", self._default_agent)

        # Add output options to send_meta for message filtering
        output_opts = self._get_output_options(target_agent)
        send_meta["_output_options"] = output_opts
        send_meta["_target_agent"] = target_agent

        # Try to get target agent's runner for true multi-agent routing
        process = self._process  # Default process
        try:
            from .agent_router import get_agent_runner

            if target_agent and target_agent != "default":
                runner = await get_agent_runner(target_agent)
                if runner:
                    process = runner.stream_query
                    logger.info(
                        f"OneBot: routing to agent '{target_agent}' "
                        f"with dedicated runner"
                    )
                else:
                    # Fallback: inject persona and use default process
                    logger.debug(
                        f"OneBot: agent '{target_agent}' not available, "
                        f"using persona injection fallback"
                    )
        except Exception as e:
            logger.warning(f"Failed to get agent runner: {e}, using default process")

        # Run the process loop with selected process
        last_response = None
        event_count = 0
        try:
            async for event in process(request):
                event_count += 1
                obj = getattr(event, "object", None)
                status = getattr(event, "status", None)
                msg_type = getattr(event, "type", None)
                logger.info(
                    f"OneBot: event #{event_count}, obj={obj}, status={status}, type={msg_type}"
                )
                if obj == "message" and status == RunStatus.Completed:
                    await self.on_event_message_completed(
                        request,
                        to_handle,
                        event,
                        send_meta,
                    )
                elif obj == "response":
                    last_response = event
                    await self.on_event_response(request, event)

            err_msg = self._get_response_error_message(last_response)
            if err_msg:
                await self._on_consume_error(
                    request,
                    to_handle,
                    f"Error: {err_msg}",
                )

            if self._on_reply_sent:
                args = self.get_on_reply_sent_args(request, to_handle)
                self._on_reply_sent(self.channel, *args)

        except Exception:
            logger.exception("OneBot channel process loop failed")
            await self._on_consume_error(
                request,
                to_handle,
                "An error occurred while processing your request.",
            )

    async def on_event_message_completed(
        self,
        request: "AgentRequest",
        to_handle: str,
        event: Any,
        send_meta: Dict[str, Any],
    ) -> None:
        """Override to apply output options filtering.
        
        Filters message content based on output_options configuration:
        - show_reply: Whether to show assistant reply messages
        - show_thinking: Whether to show thinking/reasoning content
        - show_tool_calls: Whether to show tool call details
        """
        # Get output options from send_meta
        output_opts = send_meta.get("_output_options", {})
        target_agent = send_meta.get("_target_agent", "default")
        
        # Check message type to decide filtering
        from agentscope_runtime.engine.schemas.agent_schemas import MessageType
        msg_type = getattr(event, "type", None)
        
        # Debug: log event structure
        logger.info(
            f"OneBot: on_event_message_completed, msg_type={msg_type}, "
            f"output_opts={output_opts}, event.content type={type(getattr(event, 'content', None))}"
        )
        
        # Get emoji markers from instance config (with top-level fallback)
        parsed = self._parse_to_handle(to_handle)
        bot_qq = parsed.get("instance", "")
        inst = None
        for i in self._instances.values():
            if str(i.self_id) == bot_qq or i.name == bot_qq:
                inst = i
                break
        
        # Use instance-specific emoji or fall back to top-level config
        thinking_emoji = inst.thinking_emoji if inst else self._thinking_emoji
        tool_emoji = inst.tool_emoji if inst else self._tool_emoji
        
        # Filter thinking/reasoning content: add emoji marker when showing
        if msg_type == MessageType.REASONING:
            if output_opts.get("show_thinking", False):
                # Add thinking emoji to mark this as internal thinking
                logger.info(
                    f"OneBot: marking thinking message with {thinking_emoji} for agent '{target_agent}'"
                )
                event = self._prepend_emoji_to_event(event, thinking_emoji)
            else:
                # Not showing thinking, skip this message
                logger.debug(
                    f"OneBot: skipping thinking message for agent '{target_agent}' (show_thinking=False)"
                )
                return
        
        # Filter tool calls: add emoji marker when showing
        if msg_type in (
            MessageType.FUNCTION_CALL,
            MessageType.PLUGIN_CALL,
            MessageType.MCP_TOOL_CALL,
            MessageType.FUNCTION_CALL_OUTPUT,
            MessageType.PLUGIN_CALL_OUTPUT,
            MessageType.MCP_TOOL_CALL_OUTPUT,
        ):
            if output_opts.get("show_tool_calls", False):
                # Add tool emoji to mark this as tool content
                logger.info(
                    f"OneBot: marking tool message with {tool_emoji} for agent '{target_agent}'"
                )
                event = self._prepend_emoji_to_event(event, tool_emoji)
            else:
                # Not showing tool calls, skip this message
                logger.debug(
                    f"OneBot: skipping tool message for agent '{target_agent}' (show_tool_calls=False)"
                )
                return
        
        # Check if this is a reply message (assistant response)
        role = getattr(event, "role", None)
        if role == "assistant" and not output_opts.get("show_reply", True):
            logger.debug(
                f"OneBot: filtering reply message for agent '{target_agent}'"
            )
            return
        
        # Use base class method to send message
        await self.send_message_content(to_handle, event, send_meta)

    def _prepend_emoji_to_event(self, event: Any, emoji: str) -> Any:
        """Prepend an emoji to event content.
        
        Args:
            event: The event object
            emoji: Emoji to prepend
            
        Returns:
            Modified event with emoji prepended to content
        """
        # Get content from event
        content = getattr(event, "content", None)
        if content is None:
            logger.warning(f"OneBot: _prepend_emoji_to_event - content is None")
            return event
        
        # Handle different content types
        if isinstance(content, str):
            # Simple string content
            new_content = f"{emoji} {content}"
            setattr(event, "content", new_content)
        elif isinstance(content, list):
            # List of content blocks (could be dict or TextContent objects)
            for block in content:
                # Handle dict format
                if isinstance(block, dict) and block.get("type") == "text":
                    original_text = block.get("text", "")
                    block["text"] = f"{emoji} {original_text}"
                    break
                # Handle TextContent object
                elif hasattr(block, "type") and getattr(block, "type", None) == "text":
                    original_text = getattr(block, "text", "")
                    if original_text:
                        setattr(block, "text", f"{emoji} {original_text}")
                        break
            else:
                # No text block found, prepend as new dict block
                content.insert(0, {"type": "text", "text": emoji})
        
        return event

    def merge_native_items(self, items: List[Any]) -> Any:
        """Merge multiple native payloads from same session."""
        if not items:
            return None

        first = items[0] if isinstance(items[0], dict) else {}
        merged_parts: List[Any] = []
        for it in items:
            p = it if isinstance(it, dict) else {}
            merged_parts.extend(p.get("content_parts") or [])

        last = items[-1] if isinstance(items[-1], dict) else {}
        return {
            "channel_id": self.channel,
            "sender_id": last.get("sender_id", first.get("sender_id", "")),
            "user_id": last.get("user_id", first.get("user_id", "")),
            "session_id": last.get("session_id", first.get("session_id", "")),
            "content_parts": merged_parts,
            "meta": dict(last.get("meta") or {}),
        }

    def get_to_handle_from_request(self, request: "AgentRequest") -> str:
        """Extract send target from request.
        
        Format:
        - Group chat: onebot:group:{bot_qq}:{group_id}
        - Private chat: onebot:private:{bot_qq}:{user_id}
        """
        meta = getattr(request, "channel_meta", None) or {}
        message_type = meta.get("message_type", "private")
        
        # Use bot's QQ number (self_id) for to_handle
        bot_qq = meta.get("self_id", 0) or 0
        bot_id = str(bot_qq) if bot_qq else "unknown"

        if message_type == "group":
            group_id = meta.get("group_id", 0)
            return f"onebot:group:{bot_id}:{group_id}"
        else:
            user_id = getattr(request, "user_id", "")
            return f"onebot:private:{bot_id}:{user_id}"

    def _route_agent(self, meta: Dict[str, Any]) -> str:
        """Determine target agent based on routing rules."""
        for rule in self._routing_rules:
            match = rule.match
            matched = True

            for key, value in match.items():
                if key == "instance":
                    if meta.get("instance") != value:
                        matched = False
                        break
                elif key == "group_id":
                    if meta.get("group_id") != value:
                        matched = False
                        break
                elif key == "user_id":
                    if meta.get("user_id") != value:
                        matched = False
                        break
                elif key == "message_type":
                    if meta.get("message_type") != value:
                        matched = False
                        break
                else:
                    if meta.get(key) != value:
                        matched = False
                        break

            if matched:
                logger.info(f"OneBot routing: {match} -> agent {rule.agent_id}")
                return rule.agent_id

        return self._default_agent

    async def start(self) -> None:
        """Start WebSocket connections (singleton pattern).
        
        Multiple OneBotChannel instances (one per agent) share the same WebSocket
        connections. Only the first instance to call start() actually establishes
        connections; subsequent instances reuse them.
        """
        global _active_channels, _global_connections, _global_connection_refs, _global_connection_lock
        
        if not self._enabled:
            logger.info("OneBot channel is disabled")
            return

        if not self._instances:
            logger.warning("OneBot channel enabled but no instances configured")
            return

        self._running = True
        
        async with _global_connection_lock:
            if self._http is None:
                self._http = aiohttp.ClientSession()
            
            # Start connections for each instance (if not already connected)
            for inst in self._instances.values():
                if not inst.enabled:
                    continue
                    
                inst_name = inst.name
                
                # Check if already connected globally
                if inst_name in _global_connections:
                    # Reuse existing connection (or pending connection)
                    global_conn = _global_connections[inst_name]
                    if global_conn.get("pending"):
                        # Connection is being established, just increment ref count
                        _global_connection_refs[inst_name] = _global_connection_refs.get(inst_name, 0) + 1
                        logger.info(f"OneBot {inst_name}: waiting for pending connection (refs={_global_connection_refs[inst_name]})")
                    else:
                        # Connection already established, reuse it
                        inst.ws = global_conn["ws"]
                        inst.session = global_conn["session"]
                        inst.self_id = global_conn["self_id"]
                        inst.nickname = global_conn["nickname"]
                        inst.heartbeat_task = global_conn["heartbeat_task"]
                        inst.receive_task = global_conn["receive_task"]
                        _global_connection_refs[inst_name] = _global_connection_refs.get(inst_name, 0) + 1
                        logger.info(f"OneBot {inst_name}: reusing existing connection (refs={_global_connection_refs[inst_name]})")
                else:
                    # Create placeholder to prevent race condition
                    _global_connections[inst_name] = {"pending": True}
                    _global_connection_refs[inst_name] = 1
                    asyncio.create_task(self._connect_instance_shared(inst))
                    logger.info(f"OneBot {inst_name}: creating new connection")

        logger.info(f"OneBot channel started with {len(self._instances)} instance(s)")

    async def stop(self) -> None:
        """Stop WebSocket connections (reference counted).
        
        Only closes connections when the last instance calls stop().
        """
        global _active_channels, _global_connections, _global_connection_refs, _global_connection_lock
        
        self._running = False
        
        async with _global_connection_lock:
            for inst in self._instances.values():
                inst_name = inst.name
                
                if inst_name not in _global_connection_refs:
                    continue
                    
                _global_connection_refs[inst_name] -= 1
                
                if _global_connection_refs[inst_name] <= 0:
                    # Last reference, close connection
                    logger.info(f"OneBot {inst_name}: last reference, closing connection")
                    
                    # Unregister from global registry
                    if inst.self_id and inst.self_id in _active_channels:
                        del _active_channels[inst.self_id]
                    
                    if inst.heartbeat_task:
                        inst.heartbeat_task.cancel()
                    if inst.receive_task:
                        inst.receive_task.cancel()
                    if inst.ws and not inst.ws.closed:
                        await inst.ws.close()
                    if inst.session and not inst.session.closed:
                        await inst.session.close()
                    
                    # Clean up global state
                    if inst_name in _global_connections:
                        del _global_connections[inst_name]
                    if inst_name in _global_connection_refs:
                        del _global_connection_refs[inst_name]
                else:
                    logger.info(f"OneBot {inst_name}: connection kept (refs={_global_connection_refs[inst_name]})")

        if self._http and not self._http.closed:
            await self._http.close()
            self._http = None

        logger.info("OneBot channel stopped")

    async def _connect_instance_shared(self, inst: OneBotInstance) -> None:
        """Connect to a single OneBot instance and update global state."""
        global _active_channels, _global_connections, _global_connection_lock
        
        reconnect_idx = 0

        while self._running:
            try:
                logger.info(f"OneBot connecting to {inst.name}: {inst.ws_url}")

                headers = {}
                if inst.access_token:
                    headers["Authorization"] = f"Bearer {inst.access_token}"

                if inst.session is None or inst.session.closed:
                    inst.session = aiohttp.ClientSession()

                async with inst.session.ws_connect(
                    inst.ws_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                    heartbeat=30,  # Built-in heartbeat
                ) as ws:
                    inst.ws = ws
                    inst.reconnect_count = 0
                    reconnect_idx = 0
                    inst.last_heartbeat_ack = time.time()

                    logger.info(f"OneBot {inst.name} connected")

                    # Update global connection state
                    async with _global_connection_lock:
                        _global_connections[inst.name] = {
                            "ws": ws,
                            "session": inst.session,
                            "self_id": 0,
                            "nickname": "",
                            "heartbeat_task": None,
                            "receive_task": None,
                        }

                    # Request login info
                    await ws.send_json({
                        "action": "get_login_info",
                        "params": {},
                        "echo": f"init_{inst.name}"
                    })

                    # Start heartbeat task
                    inst.heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(inst)
                    )

                    # Receive messages
                    await self._receive_loop(inst, ws)

            except asyncio.CancelledError:
                logger.info(f"OneBot {inst.name} connection cancelled")
                break
            except Exception as e:
                logger.error(f"OneBot {inst.name} connection error: {e}")

                if not self._running:
                    break

                # Exponential backoff
                delay = RECONNECT_DELAYS[
                    min(reconnect_idx, len(RECONNECT_DELAYS) - 1)
                ]
                reconnect_idx += 1
                inst.reconnect_count += 1

                logger.info(
                    f"OneBot {inst.name} reconnecting in {delay}s "
                    f"(attempt {inst.reconnect_count})"
                )

                await asyncio.sleep(delay)

    async def _heartbeat_loop(self, inst: OneBotInstance) -> None:
        """Send periodic heartbeats to keep connection alive."""
        try:
            while self._running and inst.ws and not inst.ws.closed:
                await asyncio.sleep(30)

                if inst.ws and not inst.ws.closed:
                    await inst.ws.send_json({
                        "action": "get_status",
                        "params": {},
                        "echo": f"heartbeat_{inst.name}"
                    })
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"OneBot {inst.name} heartbeat error: {e}")

    async def _receive_loop(
        self,
        inst: OneBotInstance,
        ws: aiohttp.ClientWebSocketResponse,
    ) -> None:
        """Receive and process messages from WebSocket."""
        async for msg in ws:
            if not self._running:
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    post_type = data.get("post_type", "")
                    if post_type == "message":
                        logger.debug(f"Received message from {inst.name}")
                    await self._handle_message(inst, data)
                except json.JSONDecodeError as e:
                    logger.warning(f"OneBot {inst.name} invalid JSON: {e}")
                except Exception as e:
                    logger.error(f"OneBot {inst.name} message handling error: {e}")
                    logger.exception(e)

            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f"OneBot {inst.name} WS error: {ws.exception()}")
                break

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.info(f"OneBot {inst.name} connection closed")
                break

    async def _handle_message(
        self,
        inst: OneBotInstance,
        data: Dict[str, Any],
    ) -> None:
        """Handle a received message or event."""
        post_type = data.get("post_type", "")

        # Handle API responses
        if "echo" in data:
            await self._handle_api_response(inst, data)
            return

        # Handle lifecycle events
        if post_type == "meta_event":
            await self._handle_meta_event(inst, data)
            return

        # Handle messages
        if post_type == "message":
            await self._handle_chat_message(inst, data)
            return

        # Log other events (request, notice, etc.)
        logger.debug(f"OneBot {inst.name} unhandled event: {post_type}")

    async def _handle_api_response(
        self,
        inst: OneBotInstance,
        data: Dict[str, Any],
    ) -> None:
        """Handle API response."""
        global _active_channels, _global_connections
        echo = data.get("echo", "")
        status = data.get("status", "")
        retcode = data.get("retcode", -1)

        # Check for pending API calls first
        if echo and echo in _pending_api_calls:
            future = _pending_api_calls.pop(echo)
            if not future.done():
                future.set_result(data)
            return

        # Handle init response
        if echo.startswith("init_"):
            if status == "ok" and retcode == RETCODE_OK:
                info = data.get("data", {})
                inst.self_id = info.get("user_id", 0)
                inst.nickname = info.get("nickname", "")
                
                # Update global connection state
                if inst.name in _global_connections:
                    _global_connections[inst.name]["self_id"] = inst.self_id
                    _global_connections[inst.name]["nickname"] = inst.nickname
                
                # Register to global registry for API calls
                if inst.self_id:
                    _active_channels[inst.self_id] = self
                    logger.info(
                        f"OneBot {inst.name} registered: QQ={inst.self_id} ({inst.nickname})"
                    )
            else:
                logger.warning(
                    f"OneBot {inst.name} get_login_info failed: {data}"
                )

        # Handle heartbeat response
        elif echo.startswith("heartbeat_"):
            if status == "ok":
                inst.last_heartbeat_ack = time.time()
            else:
                logger.warning(f"OneBot {inst.name} heartbeat failed: {data}")

    async def _handle_meta_event(
        self,
        inst: OneBotInstance,
        data: Dict[str, Any],
    ) -> None:
        """Handle meta events (connect, heartbeat)."""
        global _global_connections
        meta_event_type = data.get("meta_event_type", "")

        if meta_event_type == "lifecycle":
            sub_type = data.get("sub_type", "")
            if sub_type == "connect":
                inst.self_id = data.get("self_id", 0)
                # Update global connection state
                if inst.name in _global_connections:
                    _global_connections[inst.name]["self_id"] = inst.self_id
                logger.info(f"OneBot {inst.name} lifecycle: connect (self_id={inst.self_id})")

        elif meta_event_type == "heartbeat":
            # OneBot sends heartbeat events periodically
            pass

    async def _handle_chat_message(
        self,
        inst: OneBotInstance,
        data: Dict[str, Any],
    ) -> None:
        """Handle chat message and enqueue for processing."""
        global _GLOBAL_PROCESSED_MESSAGES
        
        message_type = data.get("message_type", "private")
        user_id = data.get("user_id", 0)
        group_id = data.get("group_id", 0)
        message = data.get("message", [])
        self_id = data.get("self_id", 0)
        message_id = data.get("message_id", 0)
        
        # Message deduplication using GLOBAL cache
        # This handles the case where multiple channel instances (one per agent)
        # receive the same message from the same bot
        if message_id:
            bot_id = inst.self_id or self_id
            dedup_key = f"{bot_id}:{message_type}:{group_id if message_type == 'group' else user_id}:{message_id}"
            
            if dedup_key in _GLOBAL_PROCESSED_MESSAGES:
                logger.info(
                    f"OneBot {inst.name}: skipping duplicate message (global dedup), "
                    f"message_id={message_id}, bot={bot_id}"
                )
                return
            
            # Add to global cache
            _GLOBAL_PROCESSED_MESSAGES.add(dedup_key)
            logger.debug(f"OneBot {inst.name}: added to global dedup cache, key={dedup_key}")
            
            # Prevent memory leak: trim cache if too large
            if len(_GLOBAL_PROCESSED_MESSAGES) > _MAX_DEDUP_CACHE_SIZE:
                # Keep only the most recent entries
                _GLOBAL_PROCESSED_MESSAGES = set(list(_GLOBAL_PROCESSED_MESSAGES)[-_MAX_DEDUP_CACHE_SIZE // 2:])

        # Debug: log raw message data
        logger.info(
            f"OneBot {inst.name}: processing message_id={message_id}, "
            f"group={group_id}, user={user_id}, bot={inst.self_id or self_id}"
        )
        
        # Filter messages from bots (including self)
        # Rule 0: Ignore messages sent by any bot (user_id matches any known bot)
        if user_id and inst.self_id:
            # Check if sender is self (this bot)
            if user_id == inst.self_id:
                logger.info(
                    f"OneBot {inst.name}: ignoring self-sent message, "
                    f"user={user_id}, bot={inst.self_id}"
                )
                return
            # Check if sender is another bot in the same group
            # by checking if message starts with any bot emoji markers
            first_text = self._extract_first_text(message)
            if first_text:
                # Collect bot emojis: top-level + instance-specific overrides
                bot_emojis = {
                    self._thinking_emoji,
                    self._tool_emoji,
                    self._skip_emoji,
                }
                # Add instance-specific emojis (may have custom config)
                for i in self._instances.values():
                    bot_emojis.add(i.thinking_emoji)
                    bot_emojis.add(i.tool_emoji)
                    bot_emojis.add(i.skip_emoji)
                for emoji in bot_emojis:
                    if emoji and first_text.startswith(emoji):
                        logger.info(
                            f"OneBot {inst.name}: ignoring bot-generated message "
                            f"(starts with {emoji}), user={user_id}"
                        )
                        return

        # Check @ mention requirement for group messages
        if message_type == "group":
            # Determine @ mention policy for this group
            # Priority: group_mention_policy[group_id] > require_mention
            require_mention = inst.group_mention_policy.get(
                group_id, inst.require_mention
            )
            
            # Determine which QQ ID to check for @ mention
            # Priority: inst.qq_id > inst.self_id > message self_id
            check_qq_id = inst.qq_id or inst.self_id or self_id
            
            # Extract @ mentions from message
            at_qq_list = self._extract_at_mentions(message)
            
            # Log for debugging
            logger.info(
                f"OneBot {inst.name}: @ mention check, "
                f"group={group_id}, check_qq={check_qq_id}, at_list={at_qq_list}, "
                f"policy={'group_override' if group_id in inst.group_mention_policy else 'default'}"
            )
            
            # Rule 1: If message has @ mentions but bot is NOT among them, ignore
            # This applies regardless of require_mention setting
            if at_qq_list and check_qq_id and check_qq_id not in at_qq_list:
                logger.info(
                    f"OneBot {inst.name}: ignoring group message (@ others, not me), "
                    f"group={group_id}, my_qq={check_qq_id}, at_list={at_qq_list}"
                )
                return
            
            # Rule 2: If no @ mentions and require_mention is True, ignore
            if not at_qq_list and require_mention:
                logger.info(
                    f"OneBot {inst.name}: ignoring group message (no @ mention), "
                    f"group={group_id}, require_mention=True"
                )
                return
            
            # Rule 3: If qq_id unknown and there are @ mentions, we can't verify
            if at_qq_list and not check_qq_id:
                logger.warning(
                    f"OneBot {inst.name}: cannot verify @ mention (qq_id unknown), "
                    f"set qq_id in config or wait for get_login_info response. "
                    f"Ignoring message."
                )
                return

        # Parse message content
        content_parts = await self._parse_message_content(message)

        if not content_parts:
            return

        # Build meta
        meta = {
            "instance": inst.name,
            "self_id": self_id,
            "user_id": user_id,
            "message_type": message_type,
            "group_id": group_id,
            "raw_message": data,
        }

        # Determine target agent
        target_agent = self._route_agent(meta)
        meta["target_agent"] = target_agent

        # Build native payload
        native_payload = {
            "channel_id": self.channel,
            "sender_id": str(user_id),
            "user_id": str(user_id),
            "content_parts": content_parts,
            "meta": meta,
        }

        # Enqueue for processing
        if self._enqueue:
            self._enqueue(native_payload)
            logger.debug(
                f"OneBot {inst.name} enqueued message from "
                f"{'group' if message_type == 'group' else 'private'} "
                f"{group_id if message_type == 'group' else user_id} "
                f"-> agent {target_agent}"
            )

    async def _parse_message_content(
        self,
        message: Any,
    ) -> List[OutgoingContentPart]:
        """Parse OneBot message array to content parts."""
        parts: List[OutgoingContentPart] = []

        if isinstance(message, str):
            if message.strip():
                parts.append(TextContent(type=ContentType.TEXT, text=message))
            return parts

        if not isinstance(message, list):
            return parts

        for seg in message:
            if not isinstance(seg, dict):
                continue

            seg_type = seg.get("type", "")
            data = seg.get("data", {})

            if seg_type == "text":
                text = data.get("text", "")
                if text.strip():
                    parts.append(TextContent(type=ContentType.TEXT, text=text))

            elif seg_type == "image":
                url = data.get("url") or data.get("file", "")
                if url:
                    parts.append(ImageContent(
                        type=ContentType.IMAGE,
                        image_url=url,
                    ))

            elif seg_type == "at":
                qq = data.get("qq", "")
                if qq:
                    parts.append(TextContent(
                        type=ContentType.TEXT,
                        text=f"@{qq}",
                    ))

            # TODO: support more types (video, audio, file, reply, etc.)

        return parts

    def _extract_at_mentions(self, message: Any) -> List[int]:
        """Extract @ mentioned QQ IDs from OneBot message array.
        
        Args:
            message: OneBot message array
            
        Returns:
            List of QQ IDs that were mentioned via @
        """
        at_list: List[int] = []
        
        if isinstance(message, str):
            return at_list
            
        if not isinstance(message, list):
            return at_list
            
        for seg in message:
            if not isinstance(seg, dict):
                continue
                
            seg_type = seg.get("type", "")
            data = seg.get("data", {})
            
            if seg_type == "at":
                qq = data.get("qq", "")
                if qq:
                    try:
                        # Handle special cases: "all" for @全体成员
                        if qq == "all":
                            continue
                        at_list.append(int(qq))
                    except (ValueError, TypeError):
                        pass
                        
        return at_list

    def _extract_first_text(self, message: Any) -> str:
        """Extract first text content from OneBot message array.
        
        Args:
            message: OneBot message array
            
        Returns:
            First text string found, or empty string
        """
        if isinstance(message, str):
            return message
            
        if not isinstance(message, list):
            return ""
            
        for seg in message:
            if not isinstance(seg, dict):
                continue
                
            seg_type = seg.get("type", "")
            data = seg.get("data", {})
            
            if seg_type == "text":
                text = data.get("text", "")
                if text:
                    return text
                    
        return ""

    async def send(
        self,
        to_handle: str,
        text: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send a text message."""
        if not text or not text.strip():
            return

        # Parse to_handle
        parsed = self._parse_to_handle(to_handle)
        bot_qq = parsed.get("instance", "default")  # Actually bot QQ number
        message_type = parsed.get("message_type", "private")
        target_id = parsed.get("target_id", 0)

        # Find instance by bot QQ number
        inst = None
        for i in self._instances.values():
            if str(i.self_id) == bot_qq or i.name == bot_qq:
                inst = i
                break

        if not inst or not inst.ws or inst.ws.closed:
            logger.warning(f"OneBot instance {bot_qq} not connected")
            return

        # Build API call
        if message_type == "group":
            action = "send_group_msg"
            params = {"group_id": target_id, "message": text}
        else:
            action = "send_private_msg"
            params = {"user_id": target_id, "message": text}

        try:
            await inst.ws.send_json({
                "action": action,
                "params": params,
                "echo": f"send_{int(time.time() * 1000)}"
            })
            logger.debug(f"OneBot {inst.name} sent to {to_handle}")
        except Exception as e:
            logger.error(f"OneBot {inst.name} send failed: {e}")

    def _parse_to_handle(self, to_handle: str) -> Dict[str, Any]:
        """Parse to_handle string to components."""
        result = {
            "instance": "default",
            "message_type": "private",
            "target_id": 0,
        }

        # Format: onebot:{type}:{instance}:{id}
        parts = to_handle.split(":")
        if len(parts) >= 4 and parts[0] == "onebot":
            result["message_type"] = parts[1]  # group or private
            result["instance"] = parts[2]
            try:
                result["target_id"] = int(parts[3])
            except ValueError:
                pass

        return result

    async def send_content_parts(
        self,
        to_handle: str,
        parts: List[OutgoingContentPart],
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send content parts as a message."""
        # Merge text parts and send
        text_parts = []
        for p in parts:
            ptype = getattr(p, "type", None)
            if ptype == ContentType.TEXT:
                text = getattr(p, "text", "")
                if text:
                    text_parts.append(text)

        if text_parts:
            await self.send(to_handle, "\n".join(text_parts), meta)

        # TODO: support image, file, etc.

    async def send_media(
        self,
        to_handle: str,
        part: OutgoingContentPart,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send a media part (image, file, etc.)."""
        # TODO: implement media sending
        pass

    async def call_api(
        self,
        bot_qq: int,
        action: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """Call a OneBot API through the existing WebSocket connection.

        Args:
            bot_qq: Bot's QQ number to identify which instance to use
            action: API action name (e.g., "get_group_msg_history")
            params: API parameters
            timeout: Timeout in seconds

        Returns:
            API response data

        Raises:
            ValueError: If bot not found
            asyncio.TimeoutError: If timeout
        """
        global _api_call_counter

        # Find the instance with this bot_qq
        inst = None
        for i in self._instances.values():
            if i.self_id == bot_qq:
                inst = i
                break

        if not inst or not inst.ws or inst.ws.closed:
            raise ValueError(f"OneBot instance with QQ {bot_qq} not connected")

        # Generate unique echo for this call
        async with _api_call_lock:
            _api_call_counter += 1
            echo = f"api_{_api_call_counter}_{int(time.time() * 1000)}"

        # Create future for response
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        _pending_api_calls[echo] = future

        try:
            # Send API request
            await inst.ws.send_json({
                "action": action,
                "params": params or {},
                "echo": echo,
            })

            # Wait for response
            result = await asyncio.wait_for(future, timeout=timeout)
            return result

        except asyncio.TimeoutError:
            _pending_api_calls.pop(echo, None)
            raise
        except Exception as e:
            _pending_api_calls.pop(echo, None)
            raise


# Global functions for tools to access OneBot API
def get_active_onebot_channels() -> Dict[int, "OneBotChannel"]:
    """Get all active OneBot channels indexed by bot QQ number."""
    return _active_channels.copy()


async def call_onebot_api(
    bot_qq: int,
    action: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Call a OneBot API through the active channel.

    This is the main entry point for tools to call OneBot APIs
    without needing to know access tokens.

    Args:
        bot_qq: Bot's QQ number (identifies which bot instance to use)
        action: API action name (e.g., "get_group_msg_history")
        params: API parameters
        timeout: Timeout in seconds

    Returns:
        API response data

    Raises:
        ValueError: If bot not found
        asyncio.TimeoutError: If timeout

    Example:
        # Get group message history
        result = await call_onebot_api(
            bot_qq=3241818457,
            action="get_group_msg_history",
            params={"group_id": 549149294, "count": 20}
        )
        messages = result.get("data", {}).get("messages", [])
    """
    if bot_qq not in _active_channels:
        raise ValueError(f"OneBot instance with QQ {bot_qq} not active")

    channel = _active_channels[bot_qq]
    return await channel.call_api(bot_qq, action, params, timeout)


def get_onebot_bot_list() -> List[int]:
    """Get list of active OneBot bot QQ numbers."""
    return list(_active_channels.keys())
