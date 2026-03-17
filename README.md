# copaw-onebot-channel

OneBot 11 协议 Channel 实现，用于 CoPaw 接入 QQ 机器人（NapCatQQ）。

## 功能

- ✅ 多实例支持（多个 QQ 机器人）
- ✅ 群聊/私聊消息收发
- ✅ 分群 @ 提及策略
- ✅ 多 Agent 路由
- ✅ 图片、文件等富媒体支持
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
      "instances": [
        {
          "name": "napcat",
          "ws_url": "ws://napcat:3001",
          "access_token": "your_token",
          "require_mention": true,
          "group_mention_policy": {
            "549149294": false
          },
          "enabled": true
        }
      ],
      "routing_rules": [
        {
          "match": {"group_id": 549149294},
          "agent_id": "group-assistant"
        }
      ],
      "default_agent": "default"
    }
  }
}
```

### 配置说明

| 字段 | 说明 |
|------|------|
| `instances` | 机器人实例列表 |
| `instances[].name` | 实例名（用于标识）|
| `instances[].ws_url` | WebSocket 地址 |
| `instances[].access_token` | NapCat 的 access_token |
| `instances[].require_mention` | 默认是否需要 @ 提及 |
| `instances[].group_mention_policy` | 分群的 @ 策略覆盖 |
| `routing_rules` | 消息路由规则 |
| `default_agent` | 默认 Agent |

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

### v0.4.1

- Session ID 使用 bot QQ 号替代实例名
- 新增 `onebot_history` Skill

### v0.4.0

- 分群 @ 提及策略 (`group_mention_policy`)

## 协议参考

- [OneBot 11](https://github.com/botuniverse/onebot-11)
- [NapCatQQ](https://napneko.github.io/)
