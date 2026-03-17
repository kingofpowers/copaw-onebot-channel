---
name: onebot_history
description: 获取 QQ 群聊历史消息记录
metadata: { "copaw": { "emoji": "📜" } }
---

# OneBot 群聊历史消息

获取 QQ 群的历史消息记录，用于了解之前的对话上下文。

## 使用方法

```bash
# 方式 1: 从 session_id 获取（推荐，Agent 可直接传 session_id）
python3 /root/.copaw/scripts/onebot_history.py --session-id "onebot:group:3241818457:549149294"

# 方式 2: 指定 instance + group-id
python3 /root/.copaw/scripts/onebot_history.py --group-id 549149294 --instance napcat

# 输出 JSON（用于程序处理）
python3 /root/.copaw/scripts/onebot_history.py --session-id "..." --raw
```

## 参数说明

| 参数 | 必填 | 说明 |
|------|------|------|
| `--session-id` | 二选一 | OneBot session_id（自动解析 bot_qq 和 group_id）|
| `--group-id` | 与 --instance 配合 | 群号 |
| `--instance` | 与 --group-id 配合 | 实例名（napcat / napcat2）|
| `--count` | 否 | 消息数量，默认 20 |
| `--raw` | 否 | 输出原始 JSON 格式 |

## Agent 可用数据

Agent 在 OneBot Channel 中可通过 `session_id` 获取所有必要信息：

### session_id 格式
```
群聊: onebot:group:{bot_qq}:{group_id}
私聊: onebot:private:{bot_qq}:{user_id}
```

### request_context
```
session_id: "onebot:group:3241818457:549149294"
user_id:    "100399943"  (发送者 QQ 号)
```

## 鉴权机制

1. **Token 来源**：从 `agent.json` 自动读取
2. **实例匹配**：调用 `get_login_info` API 获取 bot QQ 号，匹配 session_id
3. **无需手动配置**：QQ 号在运行时自动获取

## 输出格式

### JSON 格式（--raw）

```json
{
  "count": 3,
  "messages": [
    {
      "time": "2026-03-17 05:47:28",
      "timestamp": 1773726448,
      "user_id": 100399943,
      "nickname": "星焰",
      "card": "",
      "role": "owner",
      "message": "现在你的人格是什么",
      "message_id": 248417923,
      "group_id": 549149294,
      "group_name": "人格调试"
    }
  ]
}
```

## 当前实例

| 实例名 | QQ 号 |
|--------|-------|
| napcat | 3241818457 |
| napcat2 | 1179262688 |

## 注意事项

- 只能获取 bot 所在群的历史消息
- 消息历史依赖于 QQ 服务器缓存，可能不完整
- 私聊历史暂不支持
