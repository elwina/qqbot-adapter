# qqbot-adapter

plply 上的 QQ 适配器（v2，Go 单二进制）：替 muse 承担**所有**与 QQ 的直接通信，muse 只做大脑。

- **入向**：QQ 官方 WSS 网关收事件 → SQLite 持久化 → `ws://<tailscale-ip>:8765/events` 断线重放 + 实时推送
- **出向**：`http://<tailscale-ip>:8766/v2/*` 透传 `api.bot.qq.com`（自动带 token），失败落持久 outbox + 自动重试
- 只服务 Tailscale 内网，无鉴权；QQ 凭证（AppID/AppSecret/access_token）只留在 plply

```
QQ WSS (api.sgroup.qq.com) ──> qqgw   identify/resume/心跳/session_start_limit 护栏
                                  │   业务 dispatch 入库；READY/RESUMED 单独记
                                  ▼
                              spool   SQLite：events(seq=id) + control + outbox + consumers
                              ▲  ▲                    ▲
        ws://:8765/events ────┘  │                    │ 定时 retention 清理 + 位点登记
        hello{last_seq,name?}    │                    │ → 重放 → ready → 实时推 → ping
        ANY /v2/* ───────────────┴──> relay ──> api.bot.qq.com（失败入 outbox 重试）
        GET /healthz · GET /v1/events?since= · GET /v1/outbox/{id}
```

## 接口速览

**事件端口 `8765`**

| 接口 | 说明 |
|---|---|
| `WS /events`（任意路径均可 Upgrade） | 发 `{"last_seq":N,"name"?}` → 重放 → `{"type":"ready"}` → 实时事件；45s 空闲 `{"type":"ping"}` |
| `GET /v1/events?since=N&limit=M` | HTTP 重放，与 WS 同载荷 |
| `GET /healthz` | `online/session_id/seq/subs/last_event_ts/…` |

**中转端口 `8766`**

| 接口 | 说明 |
|---|---|
| `ANY /v2/*` | 透传 QQ API；2xx/4xx 原样返回并记审计，网络错/5xx → `202 {"outbox_id":N}` |
| `ANY /v2/*?async=1` | 不同步尝试，直接 202 入队 |
| `GET /v1/outbox/{id}` | 投递状态：`queued/sent/failed`、attempts、resp_body |
| `GET /healthz` | 同上 |

事件帧与 v1 完全一致：`{"type":"event","seq":n,"event":{"recv_ts":浮点秒,"op":0,"t":"GROUP_MESSAGE_CREATE","s":4,"d":{…}}}`。
细节见 [docs/protocol.md](docs/protocol.md)，事件类型见 [docs/events.md](docs/events.md)。

## 关键特性

- **断线重放**：事件落库再推送，seq 单调递增（允许空洞）；消费端带 `last_seq` 重连即补齐
- **出向不丢消息**：QQ API 断连/5xx 时请求持久化进 outbox，5s→倍增→5min 封顶重试，24h 标记 `failed`
- **收发两侧全留痕**：入向 events + 出向 outbox（含响应体审计）
- **网关护栏**：`session_start_limit` 余量 <5 自动停 5 分钟；intent 类关闭码（4013/4014/4914）120s 长退避
- **单静态二进制**：`CGO_ENABLED=0`，无 venv 无依赖，RSS 目标 <30MB；日志走 stdout→journald
- **无缝迁移**：`import-jsonl` 按 v1 `events.jsonl` 行号保 seq，muse 的 `last_seq` 不需要重置

## 配置（config.json）

| 字段 | 默认 | 说明 |
|---|---|---|
| `listen_ip` | `0.0.0.0` | 部署时填 Tailscale IP（如 `100.85.44.30`） |
| `ws_port` / `relay_port` | `8765` / `8766` | 事件服务 / API 中转 |
| `db_path` | `state/adapter.db` | SQLite 路径（相对 config 所在目录） |
| `intents` | `50331648` | `1<<24 \| 1<<25`（群成员/入群申请 + 群聊/单聊消息） |
| `retention_days` | `30` | events 保留天数 |
| `control_retention_days` | `7` | READY/RESUMED 审计保留天数 |
| `outbox_max_age_hours` | `24` | 超过后 queued → failed |
| `outbox_keep_days` | `7` | 终态行保留天数 |
| `replay_batch` | `500` | 重放每批条数 |
| `secrets_dir` | `secrets` | `appid.txt` + `app_secret.txt`（0600） |

## 目录

```
cmd/adapter/main.go        组装：qqgw→spool→serve+relay+outbox worker
cmd/import-jsonl/main.go   events.jsonl → adapter.db（seq 连续续上）
internal/{config,spool,qqgw,serve,relay,outbox,token,health}/
internal/e2e/              假 QQ 桩全链路测试
docs/{design,protocol,events}.md
deploy/{plply-adapter.service,config.example.json,README.md}
```

## 构建与测试

```bash
go build ./...                                          # 本机
go test -count=1 -timeout 90s ./internal/e2e/           # e2e 全链路
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o adapter ./cmd/adapter
```

## 部署

见 [deploy/README.md](deploy/README.md)：编译产物 + config + secrets → `/root/qqbot-adapter-v2`，
停 v1 → 导入 events.jsonl → 同端口起 v2，muse 自然重连重放。回滚 = 切回 v1 systemd unit。

## 现状

v2 已实现并通过假桩 e2e（重放顺序/断线重连/outbox 重试）。线上 v1（Python，`/root/qqbot-adapter`）
继续服役，按部署手册导入历史后灰度切换。
