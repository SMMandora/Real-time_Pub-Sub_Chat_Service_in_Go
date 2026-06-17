# Real-time Chat Service

Horizontally scalable real-time chat, built in slices. This is **slice 1**: a
single in-memory WebSocket gateway.

## Run

```bash
go run ./cmd/gateway
```

Open http://localhost:8080/ in two browser tabs, join the same room, and chat.

Environment variables:

- `ADDR` — listen address (default `:8080`)
- `WEB_DIR` — directory served at `/` (default `web`)

## Endpoints

- `GET /ws` — WebSocket. JSON frames: `{"type":"join","room":"general"}`,
  `{"type":"send","room":"general","text":"hi"}`, `{"type":"leave","room":"general"}`.
  Server sends `message`, `system`, and `error` frames.
- `GET /healthz` — liveness (always 200).
- `GET /readyz` — readiness (503 while shutting down).

## Test

```bash
go test ./... -race
```

## Roadmap

Slice 1 (this) → Redis fan-out → Postgres history → presence/typing →
rate limiting/auth → observability → K8s + load test.
