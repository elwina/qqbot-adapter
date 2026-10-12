# 部署到 plply

v2 为单静态二进制（CGO_ENABLED=0），部署目标 `/root/qqbot-adapter-v2`，与 v1 `/root/qqbot-adapter` 并存直到切换完成。

## 构建（Windows 本机交叉编译）

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o adapter ./cmd/adapter
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o import-jsonl ./cmd/import-jsonl
```

## 首次部署

```bash
# plply 上
mkdir -p /root/qqbot-adapter-v2/{state,secrets}
# 上传 adapter、import-jsonl、config.json（由 deploy/config.example.json 复制）
# 密钥沿用 v1 的 secrets/appid.txt + app_secret.txt（0600）

cp deploy/plply-adapter.service /etc/systemd/system/plply-adapter-v2.service
systemctl daemon-reload
```

日志走 stdout → journald：`journalctl -u plply-adapter-v2 -f`。无文件日志，不需要 logrotate。

## 迁移（灰度切换）

```bash
# 1. 复制 v1 事件日志（导入期间避免 v1 继续写）
systemctl stop plply-adapter
cp /root/qqbot-adapter/state/events.jsonl /root/qqbot-adapter-v2/state/

# 2. 导入：seq = jsonl 行号，muse 的 last_seq 无缝续上
cd /root/qqbot-adapter-v2
./import-jsonl -config config.json -file state/events.jsonl

# 3. 换服务：同端口 8765/8766
systemctl enable --now plply-adapter-v2
# muse 会自然重连，按 last_seq 重放

# 4. 验收
journalctl -u plply-adapter-v2 -f          # 看到 "identify 被接受（READY）"
curl http://127.0.0.1:8765/healthz          # online=true
curl -X POST http://127.0.0.1:8766/v2/users/@me -d '{}' -H 'Content-Type: application/json'
```

## 回滚

v1 文件原样保留：`systemctl stop plply-adapter-v2 && systemctl start plply-adapter`。
注意此时 v1 的 events.jsonl 与 v2 的 adapter.db 会产生 seq 分叉——回滚前 muse 消费到的
v2 seq（sqlite id）在 v1 行号体系里同样单调，重放语义兼容；若 v2 期间有新事件，可把缺口
期间的 adapter.db 导回 jsonl 再启 v1（或接受 muse last_seq 之后的重复推送，按 seq 去重）。

## 端口

- `8765`：事件 WS（任意路径均可 Upgrade）+ `GET /healthz` + `GET /v1/events?since=`
- `8766`：`ANY /v2/*` 中转 + `GET /v1/outbox/{id}` + `GET /healthz`
- 只绑 `listen_ip`（Tailscale IP），无鉴权，不进公网
