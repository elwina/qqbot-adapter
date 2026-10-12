# 消费端接入指南 / Consumer Guide

新机器人（muse、grokbot、或任意后端）接入本适配器的全部工作：

> **EN**: A consumer only needs two things — (1) a WS client that remembers a
> monotonically increasing `last_seq`, and (2) an HTTP client that POSTs to
> `/v2/*`. No QQ credentials, no token logic, no gateway code on your side.

## 前提

- 与适配器在同一 Tailscale/内网，知道它的 IP 和两个端口（默认 8765 / 8766）
- **不需要** QQ AppID/AppSecret、不需要管 access_token——凭证全部在适配器侧

## 1. 收事件：`ws://<adapter-ip>:8765/events`

```
连接 → 发 hello → 收重放 → ready → 收实时事件 → 断线带 last_seq 重连
```

```jsonc
// 你连上后 30s 内发（name 可选，用于位点登记/多消费端区分）
{"last_seq": 12345, "name": "grokbot"}

// 服务端随后发，按顺序：
{"type":"event","seq":12346,"event":{"recv_ts":1791091810.612,"op":0,"t":"GROUP_MESSAGE_CREATE","s":4,"d":{...}}}
{"type":"ready"}                                  // 重放结束，之后是实时事件
{"type":"ping"}                                   // 45s 无事件时的保活，忽略即可
```

**消费端义务（唯一要做的状态管理）**：

- 持久化最近收到的 `seq`，重连时在 `last_seq` 带回 → 服务端补齐缺口
- 按 `seq` 或 `event.d.id`（QQ 消息 id）去重：重放与实时边界可能有一条重复窗口
- `recv_ts` 是浮点**秒**；`event.d` 是 QQ 原始载荷，字段以官方文档为准
- 积压保护：超过 1024 条未读会被服务端断开——正常消费速度远低于此，断了就带 `last_seq` 重连

## 2. 调 API：`http://<adapter-ip>:8766/v2/*`

把 `https://api.bot.qq.com` 换成适配器地址即可，**不要自己加 Authorization**：

```bash
# 被动回复（QQ 要求带触发消息的 msg_id，被动窗口短，走同步）
curl -X POST http://<ip>:8766/v2/groups/{group_openid}/messages \
     -H 'Content-Type: application/json' \
     -d '{"content":"收到","msg_id":"<event.d.id>","msg_type":0}'

# 非实时通知可加 ?async=1，直接入队（立即 202，后台重试）
curl -X POST 'http://<ip>:8766/v2/groups/{gid}/messages?async=1' -d '{...}'
```

**响应语义**：

| 返回 | 含义 | 你该做的 |
|---|---|---|
| QQ 原样响应（2xx/4xx） | 已送达 QQ | 按 QQ 语义处理（4xx 是你参数问题，重发无用） |
| `202 {"outbox_id":N}` | 网络错/5xx，已落盘排队 | 可丢弃；要确认就查 `/v1/outbox/N`（`GET`） |
| 500 `{"error":...}` | 适配器自身故障（如取 token 失败） | 告警，人工介入 |

## 3. 运维观测（可选）

- `GET :8765/healthz` → `online`（QQ 网关在线）、`last_event_ts`、`subs`（活跃订阅数）
- 你的 `name` 会出现在适配器 `consumers` 表：`last_ack` = 最近送达你的 seq——
  多消费端各自独立记位点，互不影响

## 最小 Python 客户端（可直接抄）

```python
import json, websockets.sync.client as wsc

last_seq = load_seq()          # 你自己的持久化
with wsc.connect("ws://IP:8765/events") as ws:
    ws.send(json.dumps({"last_seq": last_seq, "name": "mybot"}))
    for raw in ws:
        m = json.loads(raw)
        if m["type"] != "event":
            continue           # ready/ping 跳过
        ev = m["event"]
        if m["seq"] <= last_seq:
            continue           # 重放-实时边界去重
        handle(ev["t"], ev["d"])  # 你的业务
        last_seq = m["seq"]; save_seq(last_seq)
```

断线→重连，带上 `last_seq`，完。HTTP 调 API 用任何 HTTP client 发 `POST :8766/v2/...` 即可。
