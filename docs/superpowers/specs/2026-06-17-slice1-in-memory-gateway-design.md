# Slice 1 Design — In-Memory WebSocket Gateway

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 1 of 7)

## Context

The full project is a horizontally scalable real-time chat service decomposed
into seven independently-built slices. This spec covers **slice 1 only**: a
single, stateless Go WebSocket gateway with an in-memory room hub, the real
JSON wire protocol, health endpoints, graceful shutdown, structured logging,
and a throwaway static HTML demo client.

### Roadmap (each slice = its own spec → plan → build cycle)

1. **Slice 1 — In-memory gateway** *(this spec)*
2. Redis pub/sub fan-out (multi-replica)
3. Postgres persistence worker + history API
4. Presence + typing indicators
5. Rate limiting + username auth
6. Observability (Prometheus / Grafana / OTel / Jaeger)
7. K8s deploy + load test + public demo

## Goals

- A runnable single-replica gateway: open two browser tabs and chat live across
  multiple rooms.
- Freeze a real JSON wire protocol that slices 2+ reuse unchanged (the
  in-memory hub gets swapped for Redis behind the same frames).
- Build in cross-cutting concerns now that are painful to retrofit: health
  endpoints, graceful shutdown, structured logging, unit tests.

## Non-goals (deferred to named slices)

Redis fan-out (2), Postgres/history (3), presence/typing (4), rate
limiting/usernames (5), metrics/traces (6), Docker/K8s/load test/React app (7).
No auth — `from` is a generated per-connection id until slice 5.

## Stack

- Go 1.22+ (standard library)
- `nhooyr.io/websocket` for WebSocket handling
- `chi` for HTTP routing
- No Redis, Postgres, or external services in this slice.

## Architecture

A single stateless Go service (becomes one replica in slice 2). One `chi`
router exposes:

| Route       | Purpose                                                      |
|-------------|-------------------------------------------------------------|
| `GET /ws`   | WebSocket upgrade (`nhooyr.io/websocket`)                   |
| `GET /healthz` | Always 200 while the process is alive (liveness)         |
| `GET /readyz`  | 200 normally; 503 once graceful shutdown begins          |
| `GET /`     | Serves the static demo page from `web/`                     |

### Concurrency model

**Goroutine-per-room with channels** (chosen over shared-map+mutex and actor
libraries). Each room runs its own goroutine that solely owns its client set;
join, leave, and broadcast are messages delivered over channels, so there are
no mutexes on the hot path and no lock contention. Each connection has two
goroutines: a `readPump` and a `writePump`. This pattern maps cleanly onto
Redis pub/sub in slice 2 — the room goroutine becomes the Redis subscriber.

## Components

Each component lives in `internal/gateway/` and is independently testable.

- **`protocol.go`** — typed JSON frame structs plus (un)marshal helpers. The
  wire format is frozen here.
  - Client → server: `join`, `leave`, `send`
  - Server → client: `message`, `system` (join/leave notices), `error`
- **`hub.go`** — owns `map[roomID]*Room`. Lazily creates a room on first join;
  deletes a room when its last client leaves. Acts as a registry — rooms do the
  actual fan-out work.
- **`room.go`** — one goroutine per room owning its client set; serializes
  join/leave/broadcast over channels. No locks.
- **`client.go`** — wraps a `*websocket.Conn`. `readPump` parses inbound frames
  and dispatches to the hub; `writePump` drains a **bounded** send channel. If
  the send buffer fills (slow client), the client is dropped and its connection
  closed, protecting the room goroutine from stalling.
- **`server.go`** — chi router, WebSocket handler, health handlers, static file
  serving.
- **`cmd/gateway/main.go`** — wires config (port via env), slog setup, starts
  the server, handles OS signals for shutdown.

## Wire protocol (frozen)

All frames are JSON objects with a `type` field.

**Client → server**

```json
{ "type": "join",  "room": "general" }
{ "type": "leave", "room": "general" }
{ "type": "send",  "room": "general", "text": "hello" }
```

**Server → client**

```json
{ "type": "message", "room": "general", "from": "ab12cd", "text": "hello", "ts": 1718600000000 }
{ "type": "system",  "room": "general", "event": "join", "from": "ab12cd" }
{ "type": "error",   "message": "not joined to room \"general\"" }
```

- `from` is a short random per-connection id (real usernames arrive in slice 5).
- `ts` is Unix milliseconds, set by the server at broadcast time.

## Data flow

1. Client connects to `/ws`; the gateway creates a `Client` and starts its
   read/write pumps.
2. `readPump` reads `join{room}` → hub adds the client to the `Room` (creating
   it if absent), which emits a `system` join notice to the room.
3. Client sends `send{room,text}` → the `Room` broadcasts a `message` frame to
   every member's send channel.
4. Each member's `writePump` writes the frame out.
5. Disconnect or `leave` removes the client; an empty room is reaped and a
   `system` leave notice is emitted to remaining members.

## Error handling

- Malformed JSON or unknown frame type → `error` frame; connection stays open.
- `send` or `leave` for a room the client never joined → `error` frame.
- Slow/stuck client (full send buffer) → drop the client and close with a
  `1001 going away` close frame.
- A read size limit is set on the connection to bound memory per message.

## Graceful shutdown

On SIGINT/SIGTERM:

1. `/readyz` flips to 503 (load balancers stop routing new traffic).
2. The HTTP listener stops accepting new connections.
3. The hub tells every room to close its clients with a `1001 going away`
   close frame.
4. Wait up to a bounded timeout for drains to finish, then exit.

## Testing

Table-driven unit tests on hub/room logic, exercised through an interface so no
real sockets are required:

- join adds a member to the room
- broadcast reaches every member
- leave removes a member
- last-leave reaps the room from the hub
- `send`/`leave` to an unjoined room produces an `error`
- slow-client (full buffer) is dropped

Plus one `httptest` + real-WebSocket happy-path test that exercises the wire
format end to end (connect → join → send → receive `message`).

## Project layout

```
go.mod                      # module github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go
cmd/gateway/main.go
internal/gateway/
  protocol.go
  hub.go
  room.go
  client.go
  server.go
  hub_test.go
  room_test.go
  server_test.go
web/index.html              # vanilla-JS demo client (no build step)
README.md
```

The module path matches the project's git remote
(`github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go`).

## Acceptance criteria

- `go test ./...` passes, covering the cases listed under Testing.
- `go run ./cmd/gateway` starts the service; opening two browser tabs at the
  served page, joining the same room, and sending a message shows the message
  appear in both tabs.
- `/healthz` returns 200; `/readyz` returns 200 before shutdown and 503 during
  shutdown.
- Sending SIGINT closes client connections with a `1001` close frame and the
  process exits within the shutdown timeout.
