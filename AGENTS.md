# qqbot-adapter 开发约定

Go + SQLite（modernc，纯 Go 免 cgo）的 QQ 机器人适配器：QQ 事件的持久化
转发 + QQ API 的鉴权中转。设计与协议先读 `docs/design.md` 和 `docs/protocol.md`。

## 构建 / 测试

```bash
go build ./...                                    # 本机
go test -count=1 -timeout 90s ./internal/e2e/     # e2e：假 QQ 桩全链路
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o adapter ./cmd/adapter   # 部署二进制
```

依赖拉取走 `GOPROXY=https://goproxy.cn,direct`（proxy.golang.org 在本网慢）。

## 结构

- `cmd/adapter` 主程序：qqgw(入向) + spool(SQLite) + serve(8765) + relay(8766) + outbox worker
- `cmd/import-jsonl` v1 events.jsonl → adapter.db，行号=seq 保留
- `internal/{config,spool,qqgw,serve,relay,outbox,token,health}`
- e2e 测试在 `internal/e2e/`（internal 包限制，测试只能放模块内）

## 协议红线

- WS 事件帧 `{"type":"event","seq":n,"event":{recv_ts:浮点秒,op,t,s,d}}` 必须与 v1 完全一致
- READY/RESUMED 不入业务流（存 control 表）；seq 允许有空洞不保证连续
- 出向：`/v2/*` 5xx/网络错 → 202 落 outbox 重试；4xx/2xx 透传并记 sent
- 凭证只读 `secrets/*.txt`（0600），token/secret 绝不写日志（v1 有 _mask 过滤器，v2 不打印请求头即可）

## 部署形态

单二进制 + config.json + secrets/（0600）。部署步骤见 `deploy/README.md`；
只绑内网/Tailscale IP，无鉴权，勿暴露公网。
