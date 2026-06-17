# Real-time Chat Service

Horizontally scalable real-time chat, built in slices. Through **slice 3a**:
multiple stateless WebSocket gateway replicas fan messages out via Redis
pub/sub, and a worker persists every message to Postgres.

## Run

Slice 2+ needs Redis; slice 3a adds Postgres. Start both with Docker:

```bash
docker compose up -d        # Redis 7 on :6379, Postgres 16 on :5432
go run ./cmd/gateway        # WebSocket gateway on :8080
go run ./cmd/worker         # persistence worker on :8090
```

Open http://localhost:8080/ in two browser tabs, join the same room, and chat.
Messages fan out in real time and the worker persists them to Postgres.

Environment variables:

- `ADDR` — gateway listen address (default `:8080`)
- `WEB_DIR` — directory served at `/` (default `web`)
- `REDIS_ADDR` — Redis address (default `localhost:6379`)
- `DATABASE_URL` — Postgres DSN (default `postgres://chat:chat@localhost:5432/chat?sslmode=disable`)
- `WORKER_ADDR` — worker health address (default `:8090`)

## Endpoints

- `GET /ws` — WebSocket. JSON frames: `{"type":"join","room":"general"}`,
  `{"type":"send","room":"general","text":"hi"}`, `{"type":"leave","room":"general"}`.
  Server sends `message`, `system`, and `error` frames.
- `GET /api/rooms/{room}/messages?limit=&before=` — paginated history JSON
  (`{"messages":[{"id","from","text","ts"}]}`); `limit` default 100 (max 200),
  `before` returns messages with id < before. Joining a room also replays the
  last 100 messages (or messages after the `id` sent on the JOIN frame).
- `GET /healthz` — liveness (always 200).
- `GET /readyz` — readiness (503 while shutting down or when Redis or Postgres is unreachable).

## Test

```bash
go test ./... -race
```

## Roadmap

Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history + replay (done) → presence/typing → rate limiting/auth → observability → K8s + load test.
