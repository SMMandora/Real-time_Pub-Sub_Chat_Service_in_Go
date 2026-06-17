# Slice 3b Design — Message History (read path)

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 3b of 7)

## Context

Slice 3a persists every message to Postgres with a per-room ID. Slice 3b adds
the **read path**: a joining client gets recent history replayed, a reconnecting
client gets only what it missed (`last_seen_id`), and a REST endpoint serves
paginated history. The gateway gains a read-only Postgres connection.

### Roadmap

1. Slice 1 — In-memory gateway *(done)*
2. Slice 2 — Redis pub/sub fan-out *(done)*
3. Slice 3a — Message persistence (write path) *(done)*
4. **Slice 3b — Message history (read path)** *(this spec)*
5. Presence + typing indicators
6. Rate limiting + username auth
7. Observability; K8s deploy + load test + public demo

## Goals

- Replay the last 100 messages to a client when it joins a room.
- On reconnect, replay only messages newer than the client's `last_seen_id`.
- Serve paginated room history over a REST endpoint.

## Non-goals (deferred)

- Room CRUD / private (invite-token) rooms.
- Typing/presence (slice 4), rate limiting/auth (slice 5), observability, K8s.
- React frontend (later); the demo page changes only if needed to show replay.

## Decisions

- **Reconnect cursor transport:** reuse the existing `Frame.id` field on a JOIN
  frame. `{"type":"join","room":"x","id":42}` means "replay messages after id
  42"; an absent/zero `id` means "send the last 100". No new Frame field.
- **Gateway↔Postgres:** the gateway connects a read-only pgx pool, pings at
  startup, and exits non-zero if Postgres is unreachable. `/readyz` pings Redis
  **and** Postgres. At runtime, a failed history query is logged and degrades
  gracefully (replay skipped; REST returns 503) — real-time chat is unaffected.
- **REST endpoint location:** on the gateway (it already has the chi router and
  will hold the read store). No separate API service in this slice.
- **Replay cap:** 100 messages for both the no-cursor and since-cursor paths.
  Clients backfill larger gaps via the REST API.
- **Package decoupling:** the `gateway` package defines its own `history`
  interface and `StoredMessage` type; an adapter in `cmd/gateway/main.go`
  bridges it to `persistence.PgxStore`'s read methods. `persistence` stays
  dependency-free; `gateway` never imports `persistence`.

## Architecture

```
JOIN room (id = cursor) ─→ gateway replays matching history to the joining client only
GET /api/rooms/{room}/messages ─→ gateway returns a JSON page from Postgres
```

The gateway holds one `history` value (read store), used by both the per-client
replay path and the REST handler.

## Components

### `internal/persistence/store_pg.go` (add read methods)

```go
// RecentMessages returns the newest `limit` messages for a room, ordered
// ascending by id.
func (s *PgxStore) RecentMessages(ctx context.Context, room string, limit int) ([]Message, error)

// MessagesSince returns messages with id > sinceID for a room, ordered
// ascending by id, capped at `limit`.
func (s *PgxStore) MessagesSince(ctx context.Context, room string, sinceID int64, limit int) ([]Message, error)
```

```go
// MessagesBefore returns the newest `limit` messages with id < beforeID for a
// room, ordered ascending by id. Powers REST `before` pagination.
func (s *PgxStore) MessagesBefore(ctx context.Context, room string, beforeID int64, limit int) ([]Message, error)
```

Queries:
- `RecentMessages`: `SELECT … WHERE room_id=$1 ORDER BY id DESC LIMIT $2`, then reverse to ascending.
- `MessagesSince`: `SELECT … WHERE room_id=$1 AND id > $2 ORDER BY id ASC LIMIT $3`.
- `MessagesBefore`: `SELECT … WHERE room_id=$1 AND id < $2 ORDER BY id DESC LIMIT $3`, then reverse to ascending.

### `internal/gateway/history.go` (new)

```go
type StoredMessage struct {
	ID   int64
	From string
	Text string
	TS   int64
}

type history interface {
	Recent(ctx context.Context, room string, limit int) ([]StoredMessage, error)
	Since(ctx context.Context, room string, sinceID int64, limit int) ([]StoredMessage, error)
	Before(ctx context.Context, room string, beforeID int64, limit int) ([]StoredMessage, error)
}
```

### `internal/gateway/client.go` (modify)

- `Client` gains a `history` reference and a stored handler `ctx`.
- `newClient` signature grows to accept the context and the history value.
- On JOIN (in `handleFrame`), after `hub.Join`, spawn a goroutine that:
  - if `f.ID > 0`: `history.Since(ctx, room, f.ID, 100)`, else `history.Recent(ctx, room, 100)`;
  - enqueues each `StoredMessage` to **this client only** as a `message` frame
    (`Frame{Type: TypeMessage, Room: room, ID: m.ID, From: m.From, Text: m.Text, TS: m.TS}`);
  - on query error, logs and returns (no enqueue).
- Replay is asynchronous so `readPump` is not blocked; the client deduplicates
  by `id` (replayed and live messages may overlap harmlessly).

### `internal/gateway/server.go` (modify)

- `NewServer(hub *Hub, bus pinger, hist history, log *slog.Logger, webDir string)`.
- `handleWS` passes the history (and ctx) into `newClient`.
- New route `GET /api/rooms/{room}/messages?limit=&before=`:
  - `limit` default 100, clamped to [1, 200]; `before` optional.
  - With `before`: `history.Before(room, before, limit)`; else
    `history.Recent(room, limit)`. Returns
    `{"messages":[{"id":…,"from":…,"text":…,"ts":…}, …]}` ascending by id.
  - On store error → 503.
- `/readyz` returns 503 if draining, `bus.Ping` fails, or the history/pg ping
  fails. (A `pinger` is already injected for Redis; add a Postgres pinger.)

### `cmd/gateway/main.go` (modify)

- Read `DATABASE_URL` (default `postgres://chat:chat@localhost:5432/chat?sslmode=disable`).
- Connect a pgx pool; ping at startup; exit non-zero on failure.
- Build a `histAdapter` wrapping `*persistence.PgxStore` that maps
  `persistence.Message` → `gateway.StoredMessage`, satisfying `gateway.history`.
- Pass the adapter to `NewServer`; close the pool on shutdown.

## Data flow (replay on join)

1. Client sends `{"type":"join","room":"x","id":42}`.
2. `hub.Join` subscribes/adds the member; live messages begin flowing.
3. Replay goroutine: `history.Since(ctx, "x", 42, 100)` → enqueue each as a
   `message` frame to the joining client.
4. Client renders, deduping by `id`. No cursor → `history.Recent(ctx, "x", 100)`.

## Error handling

- Postgres unreachable at startup → log + exit non-zero.
- History query failure at runtime → logged; replay skipped (client still
  receives live messages); REST endpoint returns 503.
- `/readyz` → 503 when draining, Redis down, or Postgres down.
- Join-replay spawns one goroutine per join; unbounded join spam is a known v1
  exposure addressed by rate limiting in slice 5.

## Testing

- **`persistence` (testcontainers Postgres, `t.Skip` without Docker):**
  `RecentMessages` returns the newest N ascending; `MessagesSince` returns only
  `id > cursor`, ordered, capped; `before`-paginated read returns `id < before`.
- **`gateway` replay (fake history):** JOIN with no cursor calls `Recent` and
  enqueues those frames to the client; JOIN with `id=N` calls `Since(…, N, …)`;
  a query error yields no enqueue and no crash.
- **`gateway` REST (httptest + fake history):** returns the JSON page with the
  expected messages; `limit` clamped to [1,200]; `before` parsed; store error →
  503.
- All concurrency tests run under `-race`.

## Acceptance criteria

- A client joining a room with no cursor receives up to the last 100 persisted
  messages as `message` frames (verified with a fake history store).
- A client joining with `id=N` receives only messages with `id > N` (capped at
  100).
- `GET /api/rooms/{room}/messages` returns a JSON page; `limit`/`before` honored
  and clamped; store error → 503.
- `/readyz` returns 503 when Postgres is unreachable.
- Against real Postgres (testcontainers, when Docker is available), the read
  methods return correctly ordered, filtered, and capped results.
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
