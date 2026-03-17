# Changelog

## [0.4.2] - 2026-03-17

### Fixed

- **图片消息处理错误**
  - 问题：`ImageContent.image_url` 传入 `{"url": url}` 导致验证失败
  - 原因：`image_url` 字段期望字符串，而不是 dict
  - 解决：直接传入 `url` 字符串

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

### Fixed (CoPaw 集成问题)

- **custom_channels 路径错误**
  - 问题：代码放在 `~/.copaw/custom_channels/` 未生效
  - 原因：CoPaw 从 `/app/working/custom_channels/` 加载
  - 解决：代码放到正确路径

- **RunStatus 导入错误**
  - 问题：`ModuleNotFoundError: No module named 'agentscope_runtime.engine.schemas.run'`
  - 解决：正确导入路径 `from agentscope_runtime.engine.schemas.agent_schemas import RunStatus`

- **配置文件不一致**
  - 问题：`config.json` 中的配置与实际运行不符
  - 原因：Channel 配置存储在 `agent.json`，`config.json` 是遗留/备用配置
  - 解决：清理 `config.json` 中的过时 onebot 配置，使用 `agent.json` 管理

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
