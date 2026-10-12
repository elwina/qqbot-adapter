# Deployment / 部署

The adapter is a single static binary (`CGO_ENABLED=0`) — no runtime, no venv.
Logs go to stdout → journald, so no logrotate is needed.

适配器是单静态二进制（`CGO_ENABLED=0`），无运行时依赖。日志走 stdout→journald，无需 logrotate。

## Build / 构建

```bash
# on the target host, or cross-compile anywhere
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o adapter ./cmd/adapter
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o import-jsonl ./cmd/import-jsonl
```

## Install / 安装

```bash
# suggested layout
sudo mkdir -p /opt/qqbot-adapter/{state,secrets}
sudo cp adapter import-jsonl /opt/qqbot-adapter/
sudo cp deploy/config.example.json /opt/qqbot-adapter/config.json

# credentials (0600), from the QQ open platform console
sudoedit /opt/qqbot-adapter/secrets/appid.txt      # your AppID
sudoedit /opt/qqbot-adapter/secrets/app_secret.txt # your AppSecret
sudo chmod 600 /opt/qqbot-adapter/secrets/*

# edit config.json — set listen_ip to the private/Tailscale IP you want to bind
sudoedit /opt/qqbot-adapter/config.json

# dedicated service user
sudo useradd -r -s /usr/sbin/nologin qqbot || true
sudo chown -R qqbot:qqbot /opt/qqbot-adapter

sudo cp deploy/qqbot-adapter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qqbot-adapter
```

中文说明：`secrets/` 放 AppID/AppSecret（0600 权限），`config.json` 的 `listen_ip`
改成你要绑定的内网/Tailscale IP，`qqbot` 是专用运行账户。

## Verify / 验收

```bash
journalctl -u qqbot-adapter -f            # "identify 被接受（READY）"
curl http://<listen-ip>:8765/healthz       # online=true, subs, last_event_ts
curl -X POST http://<listen-ip>:8766/v2/users/@me \
     -H 'Content-Type: application/json' -d '{}'   # QQ 返回的 JSON
```

## Migrating from a v1 jsonl adapter / 从 v1 迁移

If you run the Python adapter that appends events to `state/events.jsonl`,
`import-jsonl` preserves line numbers as seq so existing consumers' `last_seq`
continues seamlessly (READY/RESUMED lines move to the `control` table):

```bash
systemctl stop <v1-service>
cp /path/to/state/events.jsonl /opt/qqbot-adapter/state/
cd /opt/qqbot-adapter && ./import-jsonl -config config.json -file state/events.jsonl
systemctl enable --now qqbot-adapter     # same ports; consumers reconnect & replay
```

Rollback: keep the v1 files; `systemctl stop qqbot-adapter && systemctl start <v1-service>`.

## Ports / 端口

| Port | Purpose |
|---|---|
| `ws_port` (8765) | event WS (any path upgrades) + `GET /healthz` + `GET /v1/events?since=` |
| `relay_port` (8766) | `ANY /v2/*` relay + `GET /v1/outbox/{id}` + `GET /healthz` |

Bind both to a private IP (Tailscale or LAN). There is **no authentication** —
do not expose to the public internet.
