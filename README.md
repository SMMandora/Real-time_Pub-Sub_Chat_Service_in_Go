# Real-time Chat Service

Horizontally scalable real-time chat, built in slices. This is **slice 1**: a
single in-memory WebSocket gateway.

## Run

Slice 2 requires Redis. Start it with Docker:

```bash
docker compose up -d        # starts Redis 7 on localhost:6379
go run ./cmd/gateway
```

Open http://localhost:8080/ in two browser tabs, join the same room, and chat.
Run a second gateway on another port (`ADDR=:8081 go run ./cmd/gateway`) and the
two replicas fan out to each other through Redis.

Environment variables:

- `ADDR` — listen address (default `:8080`)
- `WEB_DIR` — directory served at `/` (default `web`)
- `REDIS_ADDR` — Redis address (default `localhost:6379`)

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

Slice 1 (done) → Redis fan-out (done) → Postgres history → presence/typing →
rate limiting/auth → observability → K8s + load test.
