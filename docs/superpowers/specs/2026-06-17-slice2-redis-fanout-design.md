# Slice 2 Design — Redis Pub/Sub Fan-out (multi-gateway)

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 2 of 7)

## Context

Slice 1 delivered a single in-memory WebSocket gateway: a frozen JSON protocol,
a goroutine-per-room hub with local fan-out, health endpoints, graceful
shutdown, and a demo client. Slice 2 makes fan-out work across multiple gateway
replicas by routing messages through Redis pub/sub, so a message sent to a
client on one gateway reaches clients connected to any gateway.

### Roadmap

1. Slice 1 — In-memory gateway *(done)*
2. **Slice 2 — Redis pub/sub fan-out** *(this spec)*
3. Postgres persistence worker + history API
4. Presence + typing indicators (cross-gateway)
5. Rate limiting + username auth
6. Observability (Prometheus / Grafana / OTel / Jaeger)
7. K8s deploy + load test + public demo

## Goals

- A message sent by any client fans out to every subscribed client across all
  gateway replicas, with identical per-room ordering everywhere.
- Keep the slice-1 wire protocol unchanged; clients are unaffected.
- Introduce a clean `Bus` seam so the transport (Redis) is swappable and
  testable without a network.

## Non-goals (deferred)

- Cross-gateway presence / `system` join-leave notices (slice 4) — these stay
  local in slice 2.
- Message IDs, history, replay (slice 3).
- Typing indicators, rate limiting, auth, metrics/traces, K8s (later slices).
- Containerizing the gateway itself (a Redis service is added to compose; the
  gateway Dockerfile is a later slice).

## Decisions

- **Redis client:** `github.com/redis/go-redis/v9` (de-facto Go client).
- **Redis is required.** The gateway connects to `REDIS_ADDR` (default
  `localhost:6379`). There is no in-memory fallback; single-gateway traffic also
  goes through Redis.
- **Fan-out model: round-trip through Redis.** On SEND the gateway PUBLISHes the
  message frame and does NOT fan out locally; the sender's own message returns
  via the subscription. This gives all clients identical ordering at the cost of
  one Redis round-trip.
- **Subscription architecture: single multiplexed subscriber per gateway.** One
  Redis PubSub connection; channels are subscribed on first local join of a room
  and unsubscribed on reap of the last local member. One receive goroutine
  dispatches incoming messages to local rooms by channel name.
- **Tests use miniredis** (`github.com/alicebob/miniredis/v2`), in-process, no
  Docker. Real-Redis testcontainers are introduced in the observability/load
  slice.

## Architecture

```
Client SEND ─→ Hub.Publish ─→ Redis PUBLISH room:{id}
                                      │
Redis ──(one PubSub conn)──→ Bus dispatch goroutine
                                      │
                              Hub.deliverLocal(room, frame)
                                      │
                              Room.broadcast ─→ local members' writePumps
```

`system` join/leave notices remain generated inside `Room.run` and fanned out to
local members only — they are not published to Redis (cross-gateway presence is
slice 4). Chat `message` frames are the only frames that traverse Redis.

## Components

### `internal/gateway/bus.go` (new)

```go
type Bus interface {
    Publish(ctx context.Context, channel string, payload []byte) error
    Subscribe(ctx context.Context, channel string) error
    Unsubscribe(ctx context.Context, channel string) error
    SetHandler(func(channel string, payload []byte))
    Ping(ctx context.Context) error
    Close() error
}
```

- `roomChannel(id string) string` returns `"room:" + id`.
- `RedisBus` implements `Bus` over a single go-redis `*redis.Client` and one
  `*redis.PubSub`. A receive goroutine ranges over `pubsub.Channel()` and calls
  the handler with each message's channel and payload. `SetHandler` must be
  called before the receive goroutine is started (during construction).
- go-redis auto-reconnects and its PubSub re-subscribes to tracked channels on
  reconnect; the implementation relies on this for runtime resilience.

### `hub.go` (modified)

- `NewHub(bus Bus) *Hub` — stores the bus and registers the dispatch handler
  (decode payload → `deliverLocal`).
- `Join`: on first creation of a room, call `bus.Subscribe(ctx, roomChannel(id))`
  before starting the room goroutine, still under the hub lock.
- `Leave`: on reap of the last member, call `bus.Unsubscribe(ctx, roomChannel(id))`
  under the hub lock.
- `Publish(roomID string, f Frame) error` — marshal the frame and
  `bus.Publish(ctx, roomChannel(roomID), payload)`. Called by clients on SEND.
- `deliverLocal(roomID string, f Frame)` — the former `Broadcast` body: look up
  the room and `select { case r.broadcast <- f: case <-r.done: }`. Called only by
  the bus handler.

### `client.go` (modified)

- `roomRegistry` replaces `Broadcast(roomID string, f Frame)` with
  `Publish(roomID string, f Frame) error`.
- `handleFrame` SEND case calls `c.hub.Publish(...)` instead of `Broadcast`. If
  publish returns an error, enqueue an `error` frame to the sender.

### `server.go` (modified)

- `NewServer(hub *Hub, bus Bus, log *slog.Logger, webDir string)` — server holds
  a pinger (the bus).
- `/readyz` returns 503 when draining OR when `bus.Ping(ctx)` returns an error;
  otherwise 200. (`/healthz` is unchanged — liveness, always 200.)

### `cmd/gateway/main.go` (modified)

- Read `REDIS_ADDR` (default `localhost:6379`).
- Construct `RedisBus`; `Ping` at startup — on failure log and exit non-zero.
- Pass the bus to `NewHub` and `NewServer`.
- On shutdown: `SetDraining(true)` → `hub.CloseAll(...)` → `httpServer.Shutdown`
  → `bus.Close()`.

### `docker-compose.yml` (new)

A single `redis` service on `redis:7` exposing `6379:6379`, so local dev can run
`docker compose up -d` for Redis and `go run ./cmd/gateway` against it.

## Data flow (SEND, two gateways)

1. Client C1 on gateway A joins room `general`. Gateway A subscribes
   `room:general` (first local member).
2. Client C2 on gateway B joins room `general`. Gateway B subscribes
   `room:general`.
3. C1 sends `{"type":"send","room":"general","text":"hi"}`. Gateway A publishes
   the `message` frame to `room:general` — no local fan-out.
4. Both gateways' subscribers receive the message; each calls `deliverLocal`,
   fanning out to its local members. C1 (via A) and C2 (via B) both receive it.

## Error handling

- Redis unreachable at startup → log + non-zero exit.
- Redis failing at runtime → `/readyz` flips to 503; go-redis reconnects and
  re-subscribes automatically.
- Message for a reaped room → `deliverLocal` finds no room, no-op.
- `Publish` error → sender gets an `error` frame.
- `Subscribe`/`Unsubscribe` run under the hub lock (a brief Redis call). This is
  an accepted slice-2 tradeoff; if it becomes a contention problem it can move
  out of the lock in a later slice.

## Testing (miniredis, in-process)

- **`bus_test.go`** — `RedisBus` against miniredis: a published payload reaches
  the handler on a subscribed channel; after `Unsubscribe` it does not.
- **Cross-gateway fan-out test** — two `Hub` instances sharing one miniredis
  endpoint. A member on hub B joins room X (synchronize on B's local `system`
  join ack), a member on hub A publishes, and B's member receives the message.
  This is the headline test proving multi-gateway fan-out.
- **`hub_test.go`** — updated with a `fakeBus` recording publish/subscribe/
  unsubscribe calls: subscribe-on-first-join, unsubscribe-on-reap, publish-on-
  send, no double-subscribe on a second local joiner of the same room.
- **`client_test.go`** — updated so the fake registry implements `Publish`;
  SEND-before-join still produces an `error` frame and does not publish.

All concurrency tests run under `-race`.

## Acceptance criteria

- With two in-process hubs sharing miniredis, a publish on one hub is delivered
  to a subscribed member on the other (cross-gateway fan-out test passes).
- `go test ./... -race` passes, including the updated hub/client tests and the
  new bus tests.
- The gateway connects to Redis on startup (pings), and `/readyz` returns 503
  when Redis is unreachable and 200 when healthy.
- A single client sending a message still receives its own message (round-trip),
  preserving slice-1 demo behavior when Redis is running.
- `docker compose up -d` starts Redis; `go run ./cmd/gateway` connects to it and
  the two-tab demo still works.
