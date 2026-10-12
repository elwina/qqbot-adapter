# Changelog

All notable changes to this project will be documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- v2 Go rewrite: QQ WSS gateway (`identify`/`resume`/heartbeat, `session_start_limit`
  guard, intent close-code backoff) → SQLite spool → WS replay + HTTP relay.
- `ws://:8765/events`: `hello{last_seq,name?}` → batched replay → `ready` → live push,
  `ping` every 45s idle; slow consumers are dropped to force clean replays.
- `ANY /v2/*` relay with durable outbox: network errors/5xx → `202 {outbox_id}`,
  exponential retry (5s→5min, `failed` after 24h); `?async=1` to enqueue directly.
- `GET /healthz`, `GET /v1/events?since=`, `GET /v1/outbox/{id}`.
- `import-jsonl`: migrates a legacy line-numbered `events.jsonl`, preserving seq;
  READY/RESUMED lines go to the `control` audit table.
- Single static binary (`CGO_ENABLED=0`), token disk cache, config file with
  defaults for every field, `-version` flag.
- End-to-end test suite against a fake QQ gateway + API stub.
