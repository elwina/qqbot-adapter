# qqbot-adapter v2 设计

日期：2026-10-12。目标：把 plply 上的 Python 适配器（777 行，无 git、events.jsonl 无界增长）重构成 Go 单二进制产品。

## 定位与边界

- **只做两件事**：QQ 事件的持久化转发；QQ API 的鉴权中转。muse（消费端，tailnet 节点）零改动——WS 协议与 relay 路径保持兼容。
- 只绑 Tailscale IP（100.85.44.30:8765/8766），无鉴权、不进公网；hello 协议预留扩展位。
- QQ 直连、access_token、AppSecret 全部收敛在 plply；muse 不持有 secret。

## 技术选型

| 决策 | 结论 | 理由 |
|---|---|---|
| 语言 | Go（CGO_ENABLED=0 静态二进制，Windows 交叉编译 linux/amd64） | 部署零依赖；并发模型贴合"1 入 N 出 + HTTP"；流量小，性能非主因 |
| SQLite | `modernc.org/sqlite`（纯 Go，免 cgo）+ WAL + busy_timeout | 单机事件日志的正确粒度；NATS/Redis Streams 杀鸡用牛刀 |
| WS | `nhooyr.io/websocket`（客户端+服务端同库） | 替代 Python 双库并存 |
| 鉴权 | 无（tailnet 内网） | 用户明确；协议留扩展位 |

## 架构

```
QQ WSS (api.sgroup.qq.com) ──> qqgw   identify/resume/heartbeat/session_start_limit 护栏
                                  │  业务 dispatch 入库；控制帧(READY/RESUMED)单独记
                                  ▼
                              spool   SQLite：events(seq=rowid) + outbox + consumers
                              ▲  ▲                    ▲
        ws://:8765/events ────┘  │                    │ 定时清理(retention) + 位点登记
        hello{last_seq,name?}    │                    │ → 重放 → ready → 实时推 → ping
        ANY /v2/* ───────────────┴──> relay ──> api.bot.qq.com（outbox 持久重试）
        GET /healthz · GET /v1/events?since= · GET /v1/outbox/{id}
```

## 数据模型

```sql
events(id INTEGER PRIMARY KEY AUTOINCREMENT,   -- 对外 seq
       recv_ts INTEGER NOT NULL,               -- unix ms
       op INTEGER NOT NULL, t TEXT NOT NULL, s INTEGER,
       payload TEXT NOT NULL);                 -- 原始 d JSON
-- 覆盖原 jsonl 语义；控制帧(READY/RESUMED)可短保留或单表

outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,
       created_ts INTEGER NOT NULL,
       method TEXT NOT NULL, path TEXT NOT NULL,
       body BLOB, content_type TEXT,
       status TEXT NOT NULL,                   -- queued | sent | failed
       resp_status INTEGER, resp_body BLOB,
       attempts INTEGER NOT NULL DEFAULT 0,
       next_retry_ts INTEGER,
       last_error TEXT);

consumers(name TEXT PRIMARY KEY,               -- hello 可选 name
          last_ack INTEGER, updated_ts INTEGER);
```

## 出向语义（v1 没有，本版核心新增）

需求来源：muse 发消息可能遇到 QQ API 断连/超时，需要**落盘重试**而不是丢失。

- `ANY /v2/*` 先同步尝试：成功 → 返回 QQ 响应 + `outbox.status=sent`（含 resp_body 审计）
- 网络错误 / 5xx → 入队 `status=queued`，返回 `202 {outbox_id}`；后台 worker 指数退避重试（5s→5min 封顶），超过 `outbox_max_age`（默认 24h）标 `failed`
- 即时可弃请求可用 `POST /v2/...?async=1` 直接 202 入队
- `GET /v1/outbox/{id}` 查状态；QQ 被动消息窗口短的实时回复靠 sync 路径，outbox 主要兜非实时通知

## 入向完整性（v1 已验证的事实）

- 已订阅 intent `1<<24 | 1<<25`（群成员/入群申请 + 群聊/单聊消息），8 天 12,681 行事件零丢损；频道系与交互回调未订阅（需要时在 QQ 开放平台开 + 配置加 intent）
- 补齐缺口：出向落 outbox 后，收发两侧都有持久记录

## 兼容协议（muse 零改动）

- client → `{"last_seq": N, "name"?: "muse"}`（name 为新增可选位点登记，老客户端不带也照常）
- server → `{"type":"event","seq":n,"event":{recv_ts,op,t,s,d}}` ×N → `{"type":"ready"}` → 实时推送；空闲 45s `{"type":"ping"}`
- 断线后按 sqlite `id > last_seq` 重放；seq 与行号解耦，可安全做保留窗口

## 配置

```json
{
  "listen_ip": "100.85.44.30",
  "ws_port": 8765,
  "relay_port": 8766,
  "db_path": "state/adapter.db",
  "intents": 50331648,
  "retention_days": 30,
  "outbox_max_age_hours": 24,
  "replay_batch": 500
}
```

secrets 维持 `secrets/appid.txt` + `app_secret.txt`（0600）；后续可换 1Password `op://` 引用。

## 目录

```
cmd/adapter/main.go        组装：qqgw→spool→serve+relay+healthz
internal/{qqgw,spool,serve,relay,outbox,token,health}/
cmd/import-jsonl/main.go   events.jsonl → sqlite（seq 连续续上）
docs/{design,protocol,events}.md
deploy/{plply-adapter.service,logrotate,README.md}
```

## 迁移与回滚

1. 本机假 QQ 桩（hello/identify/dispatch/heartbeat）+ 假 API 自测：重放顺序、resume、outbox 重试
2. `import-jsonl` 把 12,681 行灌库，seq 无缝续接
3. plply 上 `systemctl stop plply-adapter` → 换 Go 二进制 → 同端口启动 → muse 自然重连重放
4. 回滚：旧 venv 与文件保留一版本，systemd 切回原 ExecStart

## 验收

- events 表持续增长、`seq` 与 muse `last_seq` 重放一致；READY/RESUMED 不再污染业务重放
- 手动断 QQ 模拟 5xx：POST /v2 → 202，恢复后 outbox 自动 sent
- `/healthz` 返回 online/session/subs/last_event_ts；`?since=` HTTP 重放与 WS 一致
- RSS 目标 <30MB；单二进制无 venv
