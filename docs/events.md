# 事件类型

适配器按 `intents` 订阅 QQ 事件，原样透传给消费端。**本服务不解释 `d` 载荷语义**——
字段定义以 QQ 开放平台文档为准，这里只列已订阅范围和实测样例。

## 已订阅 intent

`intents = 50331648 = (1<<24) | (1<<25)`

| 位 | 范围 | 典型事件 t |
|---|---|---|
| `1<<24` | 群成员增减 / 入群申请 | `GROUP_ADD_ROBOT`、`GROUP_DEL_ROBOT`、`GROUP_JOIN_REQUEST` 等 |
| `1<<25` | 群聊消息 / 单聊消息 | `GROUP_MESSAGE_CREATE`（群消息，含 @）、`C2C_MESSAGE_CREATE`（单聊）、`FRIEND_ADD`/`FRIEND_DEL` 等 |

频道系 intent（GUILDS/GUILD_MESSAGES 等）与 INTERACTION 回调**未订阅**。需要时：
QQ 开放平台开对应 intent 开关 → 改 `config.json` 的 `intents` → 重启。

## 线上实测样例（v1 jsonl，12k+ 行）

- `GROUP_MESSAGE_CREATE`：`d` 含 `author{member_openid,username,member_role}`、
  `content`、`group_openid`、`id`、`mentions[]`、`message_scene`、`timestamp`
- `C2C_MESSAGE_CREATE`：单聊消息，结构同上（`group_openid` 换成对方 openid 语义）
- 控制帧 `READY`/`RESUMED`：v1 混在事件流里；v2 起单独存 `control` 表，**不下发给消费端**

## 存储形态

```sql
events(id=seq, recv_ts=unix_ms, op=0, t, s, payload=原始d的JSON)
```

- `s` 是 QQ 网关自己的 dispatch 序号（resume 用），`id` 才是消费端用的 seq
- `payload` 存原始 `d` JSON 文本，不重序列化，字段增删随 QQ 服务端演进
