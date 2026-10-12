# qqbot-adapter

plply 上的 QQ 适配器（v2，Go）：替 muse 承担所有与 QQ 的直接通信，muse 只做大脑。

- QQ 官方 WSS 网关收事件 → SQLite 持久化 → `ws://<tailscale-ip>:8765/events` 断线重放 + 实时推送
- `http://<tailscale-ip>:8766/v2/*` 透传 `api.bot.qq.com`（自动带 token），出向走持久 outbox + 重试
- 只服务 Tailscale 内网，无鉴权

设计：[docs/design.md](docs/design.md) · 协议：[docs/protocol.md](docs/protocol.md) · 部署：[deploy/](deploy/)

现状：v2 已实现并通过假桩 e2e（重放/实时推/outbox 重试）。线上 v1（Python，/root/qqbot-adapter）继续服役，按 [deploy/README.md](deploy/README.md) 导入历史后灰度切换。
