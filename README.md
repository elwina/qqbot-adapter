# qqbot-adapter

[![CI](https://github.com/elwina/qqbot-adapter/actions/workflows/ci.yml/badge.svg)](https://github.com/elwina/qqbot-adapter/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/elwina/qqbot-adapter)](https://goreportcard.com/report/github.com/elwina/qqbot-adapter)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small, single-binary gateway that owns **all** direct communication with QQ's
official bot platform on behalf of your application. Your backend never touches
QQ credentials, the WSS gateway, or token lifecycle — it just consumes a durable
event stream and calls a local HTTP proxy.

一个 Go 单二进制适配器，替你的应用承担与 QQ 官方机器人平台的**所有**直接通信。
后端不碰 QQ 凭证、WSS 长连和 token 生命周期——只消费一条可断线重放的事件流，
通过本地 HTTP 中转调用 QQ API。

```
QQ WSS (api.sgroup.qq.com) ──> qqgw   identify/resume/heartbeat/session_start_limit guard
                                  │   business dispatches -> spool; READY/RESUMED kept aside
                                  ▼
                              spool   SQLite: events(seq=id) + control + outbox + consumers
                              ▲  ▲                    ▲
        ws://:8765/events ────┘  │                    │ retention sweeper + consumer acks
        hello{last_seq,name?}    │                    │ -> replay -> ready -> live -> ping
        ANY /v2/* ───────────────┴──> relay --> api.bot.qq.com (durable outbox + retry)
        GET /healthz · GET /v1/events?since= · GET /v1/outbox/{id}
```

## Features / 特性

- **Durable inbound** — every QQ dispatch lands in SQLite before it's pushed, so
  consumers replay from `last_seq` after any disconnect. READY/RESUMED frames are
  audited separately and never pollute the business stream.
  入向事件先落库再推送，断线按 `last_seq` 重放；READY/RESUMED 单独存 control 表。
- **Durable outbound** — `ANY /v2/*` relays to `api.bot.qq.com` with the token
  injected. Network errors/5xx return `202 {outbox_id}` and are retried with
  exponential backoff (5s → 5min cap, marked `failed` after 24h).
  出向失败不落消息：5xx/网络错自动入队重试，可在 `/v1/outbox/{id}` 查状态。
- **Zero-dep single binary** — pure-Go SQLite (`modernc.org/sqlite`),
  `CGO_ENABLED=0`, logs to stdout for journald. RSS target <30 MB.
- **Gateway guardrails** — respects `session_start_limit`, long backoff on intent
  close codes (4013/4014/4914), resume on op7/op9.
- **Migration-friendly** — `import-jsonl` converts a legacy line-numbered
  `events.jsonl` keeping seq identical, so existing consumers need no reset.

## Quick start / 快速开始

```bash
# build (any platform; the binary is static)
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o adapter ./cmd/adapter

# configure
cp deploy/config.example.json config.json   # set listen_ip to your private IP
mkdir -p secrets state
printf '%s' 'YOUR_APP_ID'     > secrets/appid.txt
printf '%s' 'YOUR_APP_SECRET' > secrets/app_secret.txt
chmod 600 secrets/*

./adapter -config config.json
```

 systemd unit and full install/migration steps: [deploy/README.md](deploy/README.md).
 生产部署（systemd、专用账户、v1 jsonl 迁移、回滚）见 [deploy/README.md](deploy/README.md)。

## API / 接口

Event port `ws_port` (default 8765) — for consumers:

| Endpoint | Description |
|---|---|
| `WS /events` (any path upgrades) | send `{"last_seq":N,"name"?}` → replay → `{"type":"ready"}` → live events; `{"type":"ping"}` every 45s idle |
| `GET /v1/events?since=N&limit=M` | HTTP replay, same payload as WS |
| `GET /healthz` | `online/session_id/seq/subs/last_event_ts/…` |

Relay port `relay_port` (default 8766) — for API calls:

| Endpoint | Description |
|---|---|
| `ANY /v2/*` | proxy to `api.bot.qq.com` with `Authorization: QQBot <token>`; 2xx/4xx pass through, network errors/5xx → `202 {"outbox_id":N}` |
| `ANY /v2/*?async=1` | enqueue immediately, `202` |
| `GET /v1/outbox/{id}` | delivery status: `queued/sent/failed`, attempts, resp_body |
| `GET /healthz` | same health snapshot |

Wire details: [docs/protocol.md](docs/protocol.md) · subscribed event types:
[docs/events.md](docs/events.md) · design rationale: [docs/design.md](docs/design.md)（中文）

## Configuration / 配置

`config.json`（全部字段有默认值；`secrets/` 放 `appid.txt` + `app_secret.txt`，0600）：

| Field | Default | Description |
|---|---|---|
| `listen_ip` | `0.0.0.0` | IP to bind — set to your private/Tailscale IP |
| `ws_port` / `relay_port` | `8765` / `8766` | event service / API relay |
| `db_path` | `state/adapter.db` | SQLite path (relative to the config file) |
| `intents` | `50331648` | `1<<24 \| 1<<25` (group members + group/C2C messages) |
| `retention_days` | `30` | events retention |
| `control_retention_days` | `7` | READY/RESUMED audit retention |
| `outbox_max_age_hours` | `24` | queued → failed after this age |
| `outbox_keep_days` | `7` | terminal outbox rows retention |
| `replay_batch` | `500` | replay batch size |
| `secrets_dir` | `secrets` | where `appid.txt`/`app_secret.txt` live |

## Security / 安全

- Bind only to a private IP (Tailscale/LAN). **There is no authentication — do
  not expose to the public internet.**
- QQ credentials stay in `secrets/` (0600); tokens are never logged.
- 只绑内网/Tailscale IP。**无鉴权，严禁暴露公网**；凭证不离开本机、不进日志。

## Development / 开发

```bash
go build ./...                                        # build
go test -count=1 -timeout 90s ./internal/e2e/         # end-to-end with a fake QQ stub
```

Project layout and agent/dev conventions: [AGENTS.md](AGENTS.md).

## License

[MIT](LICENSE) © 2026 Elwina Vardal
