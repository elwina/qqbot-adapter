# qqbot-adapter v2 协议

面向消费端（muse）与运维。所有端口只绑 Tailscale IP，无鉴权。

## 事件流 `ws://<ip>:8765/events`

兼容 v1：任何路径的 WS Upgrade 都接受（muse 固定连 `/events`）。

```
client -> {"last_seq": N, "name"?: "muse"}        # 连接后 30s 内发送
server -> {"type":"event","seq":n,"event":{...}}  # id>N 的历史重放，升序
server -> {"type":"ready"}                        # 重放结束
server -> {"type":"event","seq":n,"event":{...}}  # 实时推送
server -> {"type":"ping"}                         # 45s 无事件时
```

`event` 对象与 v1 jsonl 行同构：

```json
{"recv_ts": 1791091810.612, "op": 0, "t": "GROUP_MESSAGE_CREATE", "s": 4, "d": {...}}
```

- `recv_ts` 为浮点**秒**（v1 兼容；库内按毫秒存）
- READY/RESUMED 不进入事件流（v2 起存 control 表单独审计）
- seq 只保证单调递增，不保证连续（导入期控制帧占了号）
- 消费端积压超过 1024 条未读会被断开，按 `last_seq` 重连重放即可

`name` 为可选登记位：带上后服务端在 `consumers` 表记录 `last_ack`（最近送达 seq）。

## HTTP（8765 与 8766 均可）

| 端点 | 说明 |
|---|---|
| `GET /healthz` | `{online, ready, session_id, seq, hb_acks, dispatches, last_event_ts, subs, uptime_seconds, close_code, close_reason}` |
| `GET /v1/events?since=N&limit=M` | HTTP 重放：`{"events":[{"type":"event","seq":n,"event":{...}}], "last_seq":n}`；limit 默认 replay_batch，上限 5000 |

## API 中转 `http://<ip>:8766`

```
ANY /v2/*  ->  {qq_api_base}/v2/*   （自动加 Authorization: QQBot <token>）
```

- 同步成功（含 4xx）：透传 QQ 状态码与响应体，outbox 落一条 `sent` 审计
- 网络错误 / 5xx：入队 `status=queued`，返回 `202 {"outbox_id":N,"status":"queued"}`；
  后台 5s→倍增→5min 封顶重试，超过 `outbox_max_age_hours`（默认 24h）标 `failed`
- `POST /v2/...?async=1`：直接 202 入队，不同步尝试
- `GET /v1/outbox/{id}`：`{id, status, attempts, resp_status, resp_body, last_error, ...}`

QQ 被动消息窗口短的实时回复走 sync 路径；outbox 兜非实时通知。

## QQ 网关（出向，消费端不感知）

1. `GET {api}/gateway/bot`（带 token）→ `{url, session_start_limit}`；失败回退 `wss://api.sgroup.qq.com/websocket`
2. `remaining < 5` 时暂停 5 分钟（session_start_limit 护栏）
3. WS 连接后：op10 hello（取 heartbeat_interval）→ 有 session 发 op6 resume，否则 op2 identify
4. op1 心跳（d=最新 s）；op11 ack
5. op0 dispatch：`s` 更新 seq；READY 记 session_id；READY/RESUMED 入 control 表；其余业务事件入 events 并推给订阅端
6. op7 → 断开重连走 resume；op9 d=true → resume，d=false → 清 session/seq 等 1~5s 后重新 identify
7. 关闭码 4013/4014/4914（intent/鉴权类）→ 退避 120s；其余指数退避 2^n 封顶 60s + 抖动

token：`POST {api}/app/getAppAccessToken {appId, clientSecret}` → `{access_token, expires_in}`，
磁盘缓存 `state/token_cache.json`，提前 600s 刷新。

## SQLite 数据模型

```sql
events(id INTEGER PRIMARY KEY,          -- 对外 seq；导入时=jsonl 行号
       recv_ts INTEGER NOT NULL,        -- unix ms
       op INTEGER NOT NULL, t TEXT NOT NULL, s INTEGER,
       payload TEXT NOT NULL);          -- 原始 d JSON
control(id INTEGER PK, recv_ts, t, s, payload, orig_seq);  -- READY/RESUMED
outbox(id PK, created_ts, method, path, body, content_type,
       status, resp_status, resp_body, attempts, next_retry_ts, last_error);
consumers(name PK, last_ack, updated_ts);
```

retention：events 30 天、control 7 天、outbox 终态 7 天（均可配置）。
