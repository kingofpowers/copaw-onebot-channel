# copaw-onebot-channel

OneBot 11 协议 Channel 实现，用于 CoPaw 接入 QQ 机器人（NapCatQQ）。

## 兼容性

| CoPaw 版本 | 状态 | 说明 |
|------------|------|------|
| 0.1.0b3 | ✅ 兼容 | 需要手动修复 `manager.py` |

### ⚠️ 必须修改 CoPaw 代码

CoPaw 0.1.0b3 存在 bug，Custom Channel 无法正常加载。需要修改：

**文件**: `copaw/app/channels/manager.py` 第 183 行

```python
# 官方代码（有 bug）
ch_cfg = getattr(ch, key, None)
if ch_cfg is None and key in extra:
    from types import SimpleNamespace
    raw = extra[key]
    ch_cfg = (
        SimpleNamespace(**raw) if isinstance(raw, dict) else raw
    )

# 修复后
ch_cfg = getattr(ch, key, None)
# Pydantic v2: extra fields can be accessed via getattr as dict
# Convert dict to SimpleNamespace for consistent attribute access
if isinstance(ch_cfg, dict):
    from types import SimpleNamespace
    ch_cfg = SimpleNamespace(**ch_cfg)
elif ch_cfg is None and key in extra:
    from types import SimpleNamespace
    raw = extra[key]
    ch_cfg = (
        SimpleNamespace(**raw) if isinstance(raw, dict) else raw
    )
```

**问题原因**: Pydantic v2 下，`getattr(dict, "enabled", False)` 返回 `False` 而不是 dict 中的值，导致所有 Custom Channel 的 `enabled` 检查失败。

**相关 Issue**: [agentscope-ai/CoPaw#1611](https://github.com/agentscope-ai/CoPaw/issues/1611)

## 功能

- ✅ 多实例支持（多个 QQ 机器人）
- ✅ 群聊/私聊消息收发
- ✅ 分群 @ 提及策略
- ✅ 多 Agent 路由
- ✅ 图片、文件等富媒体支持
- ✅ 输出选项（按 Agent 配置显示思考/工具调用）
- ✅ Emoji 消息类型标记（思考/工具/跳过）
- ✅ Bot 消息过滤（防止自循环）
- ✅ 群历史消息获取（Skill）

## 安装

```bash
# 复制到 CoPaw 的 custom_channels 目录
cp -r onebot /app/working/custom_channels/
```

## 配置

在 `agent.json` 中配置：

```json
{
  "channels": {
    "onebot": {
      "enabled": true,
      "thinking_emoji": "🤔",
      "tool_emoji": "📝",
      "skip_emoji": "💤",
      "instances": [
        {
          "name": "napcat",
          "ws_url": "ws://napcat:3001",
          "access_token": "your_token",
          "require_mention": true,
          "group_mention_policy": {
            "549149294": false,
            "1091416099": true
          },
          "enabled": true
        },
        {
          "name": "napcat2",
          "ws_url": "ws://napcat2:3001",
          "access_token": "your_token2",
          "require_mention": true,
          "group_mention_policy": {
            "549149294": true,
            "1091416099": false
          },
          "enabled": true
        }
      ],
      "routing_rules": [
        {
          "match": {"group_id": 549149294},
          "agent_id": "group-assistant"
        },
        {
          "match": {"group_id": 1091416099},
          "agent_id": "process-assistant"
        },
        {
          "match": {"message_type": "private"},
          "agent_id": "private-assistant"
        }
      ],
      "default_agent": "default",
      "dm_policy": "open",
      "group_policy": "open",
      "output_options": {
        "show_reply": true,
        "show_thinking": false,
        "show_tool_calls": false,
        "agents": {
          "process-assistant": {
            "show_reply": true,
            "show_thinking": true,
            "show_tool_calls": true
          }
        }
      }
    }
  }
}
```

### 配置说明

#### 基础配置

| 字段 | 说明 |
|------|------|
| `enabled` | 是否启用 OneBot Channel |
| `instances` | 机器人实例列表 |
| `routing_rules` | 消息路由规则 |
| `default_agent` | 默认 Agent |
| `thinking_emoji` | 思考内容标记（默认 🤔）|
| `tool_emoji` | 工具调用标记（默认 📝）|
| `skip_emoji` | 无意义内容标记（默认 💤）|

> **Emoji 说明**：实例级别可覆盖顶层配置。这些标记用于下游 channel 过滤，不由 Bot 控制。

#### instances 配置

| 字段 | 说明 |
|------|------|
| `name` | 实例名（用于标识）|
| `ws_url` | WebSocket 地址 |
| `access_token` | NapCat 的 access_token |
| `require_mention` | 默认是否需要 @ 提及 |
| `group_mention_policy` | 分群的 @ 策略覆盖 |
| `enabled` | 是否启用该实例 |

#### output_options 配置

| 字段 | 说明 |
|------|------|
| `show_reply` | 显示回复内容 |
| `show_thinking` | 显示思考过程 |
| `show_tool_calls` | 显示工具调用 |
| `agents` | 按 Agent 覆盖输出选项 |

## Session ID 格式

```
群聊: onebot:group:{bot_qq}:{group_id}
私聊: onebot:private:{bot_qq}:{user_id}
```

示例：`onebot:group:3241818457:549149294`

## Skills

### onebot_history

获取群历史消息：

```bash
# 方式 1: session-id（推荐）
python3 skills/onebot_history/onebot_history.py --session-id "onebot:group:3241818457:549149294"

# 方式 2: instance + group-id
python3 skills/onebot_history/onebot_history.py --group-id 549149294 --instance napcat
```

## Agent 可用数据

### request_context

| 字段 | 说明 |
|------|------|
| session_id | 会话标识 |
| user_id | 发送者 QQ 号 |
| channel | `onebot` |
| agent_id | 当前 Agent |

### channel_meta

| 字段 | 说明 |
|------|------|
| self_id | bot QQ 号 |
| group_id | 群号 |
| instance | 实例名 |
| message_type | `group` / `private` |
| raw_message | 完整 OneBot 消息 |

## 版本历史

### v0.5.0

- 顶层 Emoji 配置（thinking/tool/skip）
- Bot 消息过滤（防止自循环）
- TextContent 处理修复

### v0.4.1

- Session ID 使用 bot QQ 号替代实例名
- 新增 `onebot_history` Skill

### v0.4.0

- 分群 @ 提及策略 (`group_mention_policy`)
- 输出选项配置
- Emoji 标记系统

## 协议参考

- [OneBot 11](https://github.com/botuniverse/onebot-11)
- [NapCatQQ](https://napneko.github.io/)
