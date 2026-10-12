# Contributing

Issues and PRs are welcome — English or 中文 both fine.

## Ground rules / 红线

- **Never commit secrets** — `secrets/` is gitignored; keep it that way.
- The WS event frame is a frozen contract:
  `{"type":"event","seq":n,"event":{"recv_ts":秒浮点,"op","t","s","d"}}`.
  Changing its shape breaks every existing consumer.
- READY/RESUMED stay out of the business stream (`control` table).
- Outbound semantics: 2xx/4xx pass through, network errors/5xx → `202` + outbox.

## Dev loop

```bash
go build ./... && go vet ./...
go test -count=1 -timeout 90s ./internal/e2e/   # fake-QQ end-to-end
gofmt -w .
```

CI runs the same checks plus a `CGO_ENABLED=0` static build.

## Where things live

- `internal/qqgw` — QQ WSS client (identify/resume/heartbeat/backoff)
- `internal/spool` — SQLite: events / control / outbox / consumers + pub-sub
- `internal/serve` — consumer WS + `/healthz` + `/v1/events` (port `ws_port`)
- `internal/relay` — `/v2/*` proxy + outbox status (port `relay_port`)
- `internal/outbox` — retry worker
- `internal/token` — access_token cache
- `cmd/import-jsonl` — legacy jsonl → sqlite migration
