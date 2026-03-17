# Changelog

## [0.4.1] - 2026-03-17

### Changed

- Session ID 格式变更：使用 bot QQ 号替代实例名
  - 之前: `onebot:group:napcat:549149294`
  - 之后: `onebot:group:3241818457:549149294`
  - 好处：Session 与机器人 QQ 号绑定，实例名变更不影响会话连续性

### Added

- `onebot_history` Skill - 获取群历史消息
  - 支持 session_id 自动解析
  - 支持指定 instance + group_id
  - 鉴权通过 `get_login_info` API 自动获取 bot QQ 号

## [0.4.0] - 2026-03-16

### Added

- 分群 @ 提及策略 (`group_mention_policy`)
  - 可按群配置是否需要 @ 提及
  - 优先级：`group_mention_policy[group_id]` > `require_mention`

## [0.3.0] - 2026-03-15

### Added

- 输出选项配置 (`output_options`)
  - 为不同 Agent 分别控制输出内容
  - `show_reply`, `show_thinking`, `show_tool_calls`

## [0.2.0] - 2026-03-14

### Added

- 多实例支持
- 多 Agent 路由
- 图片、文件等富媒体支持

## [0.1.0] - 2026-03-13

### Added

- 基础 OneBot 11 协议实现
- WebSocket 连接
- 群聊/私聊消息收发
