# Slice 3a Design — Message Persistence (write path)

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 3a of 7)

## Context

Slices 1-2 deliver multi-gateway real-time chat over Redis pub/sub, with no
durable storage — messages exist only in flight. Slice 3a adds the **write
path**: every message gets a stable per-room ID and is persisted to Postgres by
a dedicated worker. Slice 3b (next cycle) adds the **read path** (history API +
replay on join + reconnect-since).

### Roadmap

1. Slice 1 — In-memory gateway *(done)*
2. Slice 2 — Redis pub/sub fan-out *(done)*
3. **Slice 3a — Message persistence (write path)** *(this spec)* → 3b history (read path)
4. Presence + typing indicators
5. Rate limiting + username auth
6. Observability
7. K8s deploy + load test + public demo

## Goals

- Assign every chat message a per-room, client-visible, ordered ID at send time.
- Persist messages durably to Postgres via a separate worker process.
- Keep the gateway change small and additive; the slice-2 fan-out is unchanged.

## Non-goals (deferred)

- History REST API, last-100 replay on join, reconnect-since-`last_seen_id`
  (slice 3b).
- Room CRUD / private (invite-token) rooms.
- Multi-worker horizontal scaling (would need Redis Streams + consumer groups).
- Typing/presence, rate limiting, auth, metrics/traces, K8s.

## Decisions

- **Postgres client:** `github.com/jackc/pgx/v5` (pool).
- **Message IDs:** per-room `INCR seq:{room}` in Redis, assigned at publish time
  in `Hub.Publish`. IDs are strictly ordered within a room (1, 2, 3, …); the PK
  is `(room_id, id)`. Per-room sequence is exactly what slice 3b's
  `last_seen_id` cursor needs.
- **Consume model:** a single worker process `PSUBSCRIBE room:*` (the channels
  slice 2 already publishes to). This matches the original pub/sub + async
  worker architecture. **Limitation (accepted for v1):** fire-and-forget —
  messages published while the worker is down are not persisted, and multiple
  worker replicas would double-write, so exactly one worker runs.
- **DB-write-failure policy:** on `InsertBatch` failure, retry the batch once;
  if it still fails, log a warning and drop the batch (consistent with the
  fire-and-forget model). No unbounded buffering.
- **Worker topology:** its own `cmd/worker` process, independent of the gateway
  (they share only Redis and Postgres).
- **Tests:** in-memory fake `MessageStore` for batching/worker logic (no Docker)
  plus a testcontainers-go Postgres integration test that `t.Skip`s when the
  Docker daemon is unavailable.

## Architecture

```
SEND ─→ Hub.Publish: id = INCR seq:{room}; stamp frame.id ─→ PUBLISH room:{id}
                                          │
              (slice-2 fan-out unchanged) ├─→ gateway subscribers → clients
                                          └─→ worker PSUBSCRIBE room:* → batch → Postgres
```

The sender sees its message (with `id`) via the slice-2 round-trip. The worker
is a parallel subscriber on the same `room:*` channels; it never affects
real-time delivery.

## Gateway changes (additive)

- **`internal/gateway/protocol.go`** — add `ID int64 \`json:"id,omitempty"\`` to
  `Frame`. `messageFrame` is unchanged (ID is stamped later).
- **`internal/gateway/bus.go`** — add `NextID(ctx context.Context, room string)
  (int64, error)` to the `Bus` interface. `RedisBus` implements it as
  `INCR seq:{room}`; `fakeBus` uses an in-memory per-room counter.
- **`internal/gateway/hub.go`** — `Publish` calls `bus.NextID(ctx, roomID)`,
  sets `f.ID`, then marshals and publishes. On `NextID` error, return the error
  (the client already turns a `Publish` error into an `error` frame).

## New package `internal/persistence`

- **`message.go`**
  ```go
  type Message struct {
      RoomID    string
      ID        int64
      Sender    string
      Body      string
      CreatedMS int64
  }

  type MessageStore interface {
      Migrate(ctx context.Context) error
      InsertBatch(ctx context.Context, msgs []Message) error
  }
  ```
- **`batcher.go`** — a `Batcher` with a configurable max size (default 100) and
  flush interval (default 50ms). It receives `Message`s on an input channel and
  flushes the accumulated slice to `store.InsertBatch` when the size threshold
  is reached or the interval ticks (whichever first). On `Close`/shutdown it
  flushes any remainder. Applies the retry-once-then-drop-with-log policy.
- **`worker.go`** — owns a go-redis client and `PSUBSCRIBE room:*`. For each
  received payload it unmarshals a minimal inbound struct (json tags
  `type,room,id,from,text,ts`), skips anything where `type != "message"`, builds
  a `Message`, and sends it to the batcher's input channel.
- **`store_pg.go`** — `pgxStore` backed by a `*pgxpool.Pool`. `Migrate` runs the
  `CREATE TABLE IF NOT EXISTS`. `InsertBatch` opens one transaction per batch and
  queues per-row `INSERT … ON CONFLICT (room_id, id) DO NOTHING` (idempotent),
  then commits.

The worker decodes its own inbound struct rather than importing
`gateway.Frame`, keeping the packages decoupled. The struct's json tags must
match the message frame the gateway publishes (`type`, `room`, `id`, `from`,
`text`, `ts`); the worker test publishes a literal message JSON to assert this
contract.

## Schema

```sql
CREATE TABLE IF NOT EXISTS messages (
  room_id    TEXT   NOT NULL,
  id         BIGINT NOT NULL,
  sender     TEXT   NOT NULL,
  body       TEXT   NOT NULL,
  created_ms BIGINT NOT NULL,
  PRIMARY KEY (room_id, id)
);
```

## New `cmd/worker/main.go`

- Reads `REDIS_ADDR` (default `localhost:6379`) and `DATABASE_URL`
  (default `postgres://chat:chat@localhost:5432/chat?sslmode=disable`).
- Connects the pgx pool, runs `Migrate`, builds the store + batcher + worker.
- Serves `/healthz` (always 200) and `/readyz` (503 until the pool pings OK) on
  a configurable `WORKER_ADDR` (default `:8090`).
- On SIGINT/SIGTERM: stop consuming, flush the batcher, close the pool, exit.

## Error handling

- Malformed JSON or `type != "message"` payloads → skipped (debug log), worker
  continues.
- `InsertBatch` failure → retry once; if still failing, log a warning and drop
  the batch.
- Worker offline → messages during the gap are not persisted (accepted v1
  limitation).
- `NextID` (Redis `INCR`) failure on send → `Publish` returns the error → client
  receives an `error` frame (existing slice-2 behavior).

## Testing

- **`batcher_test.go`** (fake store, no Docker): exactly 100 messages flush as
  one 100-row batch; fewer than 100 followed by an interval tick flush; `Close`
  flushes the remainder; a store error on first attempt triggers one retry.
- **`worker_test.go`** (miniredis, no Docker): publishing a literal message JSON
  to `room:x` results in the worker forwarding a `Message{RoomID:"x", ID:…,
  Sender:…, Body:…}` to a fake store; a non-`message` payload is ignored.
- **`store_pg_test.go`** (testcontainers Postgres, `t.Skip` if Docker absent):
  `Migrate` then `InsertBatch` round-trips rows; rows are ordered by `id`;
  inserting a duplicate `(room_id, id)` is a no-op (dedup).
- **Gateway** `bus_test.go` / `hub_test.go`: `NextID` returns increasing
  per-room values; `Hub.Publish` stamps a non-zero, increasing `id` on the
  published frame (assert via the loopback `fakeBus`).

## docker-compose + README

- Add a `postgres:16` service (db `chat`, user `chat`, password `chat`, port
  `5432`) to `docker-compose.yml`.
- README: document `DATABASE_URL`, `WORKER_ADDR`, and running the worker
  (`go run ./cmd/worker`) alongside Redis and the gateway.

## Acceptance criteria

- `Hub.Publish` stamps a strictly increasing per-room `id` on each message
  (verified via fakeBus).
- The worker, subscribed to `room:*` on a live (mini)redis, turns a published
  message into a `Message` delivered to the store (verified with a fake store).
- The batcher flushes on the 100-message and 50ms boundaries and on shutdown
  (verified with a fake store).
- Against a real Postgres (testcontainers, when Docker is available),
  `InsertBatch` persists rows idempotently and history is retrievable ordered by
  `id`.
- `go test ./... -race` passes (the Postgres integration test skips cleanly when
  Docker is unavailable).
- `docker compose up -d` starts Redis and Postgres; `go run ./cmd/worker`
  connects, migrates, and persists messages sent through the gateway.
