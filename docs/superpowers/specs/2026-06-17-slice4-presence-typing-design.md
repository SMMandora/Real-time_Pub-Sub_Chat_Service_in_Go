# Slice 4 Design — Presence + Typing Indicators

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 4 of 7)

## Context

Slices 1-3b deliver multi-gateway real-time chat with persistence and history.
Slice 4 adds **presence** (who is currently in a room, across gateways) and
**typing indicators** (ephemeral). Both ride a new `presence:{room}` side
channel, parallel to the persisted `room:{room}` channel.

### Roadmap

1. Slice 1 — In-memory gateway *(done)*
2. Slice 2 — Redis pub/sub fan-out *(done)*
3. Slice 3a/3b — Persistence + history *(done)*
4. **Slice 4 — Presence + typing** *(this spec)*
5. Rate limiting + username auth
6. Observability
7. K8s deploy + load test + public demo

## Goals

- Show the live set of members in a room, consistent across gateway replicas.
- Show transient "X is typing" indicators.
- Persist nothing for either feature (ephemeral, separate from the message path).

## Non-goals (deferred)

- Usernames / auth (slice 5; presence member identity becomes the username then).
- Rate limiting (slice 5), observability (slice 6), K8s (slice 7).
- Read receipts, away/idle status, per-user multi-device dedup.

## Decisions

- **Heartbeats: gateway-driven.** Each connection runs a heartbeat goroutine
  that periodically `ZADD`s the Redis presence sorted set (score = now ms) for
  every room it is joined to; on disconnect it `ZREM`s. No client heartbeat
  protocol. A crashed gateway's members expire via a TTL.
- **Presence delivery: snapshot on change.** On join/leave the gateway updates
  the sorted set, reads the current live member list, and publishes the full
  snapshot to `presence:{room}`; every gateway pushes it to its local room
  members, who replace their list. Heartbeats only refresh scores (no publish).
- **Member identity:** the existing 8-hex client id. Slice 5 swaps this to the
  authenticated username.
- **Typing channel:** the same `presence:{room}` side channel carries typing
  frames. The persistence worker only `PSUBSCRIBE room:*`, so nothing on
  `presence:*` is ever persisted.
- **Constants:** heartbeat interval 10s; presence TTL 30s (a snapshot includes
  members with score ≥ now − 30s).

## Architecture

```
join/leave ─→ ZADD/ZREM presence:{room} ─→ read live snapshot ─→ PUBLISH presence:{room} (snapshot frame)
typing     ─→ PUBLISH presence:{room} (typing frame, from = clientID)
                                   │
   gateways subscribed to presence:{room} ─→ deliverLocal ─→ local room members render
heartbeat (per connection, every 10s) ─→ ZADD presence:{room} (refresh score, no publish)
```

The Redis sorted set `presence:{room}` (key) holds the cross-gateway member set
(score = last-heartbeat ms). The pub/sub channel `presence:{room}` carries
snapshot and typing frames. (Redis keys and pub/sub channels are separate
namespaces, so the shared name is fine.)

## Protocol additions (additive)

- `Frame` gains `Members []string \`json:"members,omitempty"\``.
- New constants: `TypePresence = "presence"`, `TypeTyping = "typing"`.
- Presence frame: `{"type":"presence","room":"x","members":["ab12","cd34"]}`.
- Typing frame: `{"type":"typing","room":"x","from":"ab12"}`.
- Inbound: client sends `{"type":"typing","room":"x"}`. No client heartbeat.

## Components

### `internal/gateway/presence.go` (new)

```go
type PresenceStore interface {
	Add(ctx context.Context, room, member string, scoreMs int64) error
	Remove(ctx context.Context, room, member string) error
	Snapshot(ctx context.Context, room string, minScoreMs int64) ([]string, error)
}
```

`RedisPresenceStore` implements it over a `*redis.Client`:
- `Add`: `ZADD presence:{room} score member`.
- `Remove`: `ZREM presence:{room} member`.
- `Snapshot`: `ZRANGEBYSCORE presence:{room} minScoreMs +inf` (live members only).

A `presenceKey(room)` helper returns `presence:{room}`. Tests use a
`fakePresenceStore` (in-memory map).

### `internal/gateway/bus.go` (modify)

- Generalize `roomFromChannel` to strip either the `room:` or `presence:`
  prefix (so the dispatcher maps both channel families to a room id).
- Add `presenceChannel(id string) string` returning `"presence:" + id`.
- The `Bus` interface is unchanged (presence publishes via the existing
  `Publish`).

### `internal/gateway/hub.go` (modify)

- `Hub` holds a `PresenceStore`.
- `Join`: on first room creation, subscribe **both** `roomChannel(id)` and
  `presenceChannel(id)`. `Leave` (reap) unsubscribes both.
- `onBusMessage`: unchanged logic — decode, derive room via the generalized
  `roomFromChannel`, `deliverLocal(room, frame)`. The frame's `type` tells the
  client whether it is a message, system, presence, or typing frame.
- New `PublishPresence(room string, f Frame) error`: marshal and
  `bus.Publish(presenceChannel(room), payload)`.
- New `PresenceSnapshot(room string) ([]string, error)`: `store.Snapshot(room,
  now − TTL)` — used to build snapshot frames.

### `internal/gateway/client.go` (modify)

- `Client` gains a `PresenceStore` reference (the hub also exposes
  `PublishPresence` / `PresenceSnapshot`, which the client calls).
- On JOIN (new room): `store.Add(room, c.id, now)`, then publish a presence
  snapshot (read snapshot → `PublishPresence(room, presenceFrame(room,
  members))`). This is in addition to the existing history replay.
- On LEAVE and in `leaveAll`: `store.Remove(room, c.id)`, then publish a
  snapshot.
- A **heartbeat goroutine** started in `handleWS`: every 10s, for each joined
  room, `store.Add(room, c.id, now)` (refresh score, no publish). Stops when the
  connection context is cancelled.
- New `handleFrame` case `TypeTyping`: if joined to the room, publish
  `typingFrame(room, c.id)` to `presence:{room}`.

### `cmd/gateway/main.go` (modify)

- Construct a `RedisPresenceStore` (from `REDIS_ADDR`), pass it to the hub.
- Close it on shutdown.

### `web/index.html` (modify)

- Render a presence list, replaced whenever a `presence` frame arrives.
- Render a transient "X is typing…" line that clears ~3s after the last typing
  frame.
- Send a throttled `typing` frame as the user types in the message box.

## Data flow (join)

1. Client A on gateway G sends `join x`.
2. Hub adds A locally (subscribes `room:x` + `presence:x` on first room use).
3. A's client: `store.Add("x", A, now)` → reads snapshot `["A", …]` →
   `hub.PublishPresence("x", presenceFrame("x", members))`.
4. All gateways subscribed to `presence:x` deliver the snapshot to their local
   room-x members; everyone (including A) updates their presence list.
5. A's heartbeat goroutine refreshes A's score every 10s.

## Error handling

- A Redis presence op failure is logged; it does not drop the connection. A
  snapshot publish is skipped if the snapshot read fails.
- Presence and typing frames are never persisted (separate channel; the worker
  ignores `presence:*`).
- **Known v1 limitation:** a crashed gateway's members are excluded from new
  snapshots (score filter) but no event re-publishes until the next join/leave
  in that room, so clients may briefly show a stale member. A periodic
  re-broadcast is deferred.

## Testing

- **`presence_test.go` (miniredis):** `Add` then `Snapshot` returns the member
  within the score window; a member with a stale (old) score is excluded;
  `Remove` drops a member.
- **`hub`/`client` (fakeBus loopback + `fakePresenceStore`):** join calls
  `Add` and publishes a `presence` snapshot frame; leave calls `Remove` and
  publishes a snapshot; a `typing` inbound frame publishes a `typing` frame to
  `presence:{room}`; the heartbeat goroutine calls `Add` (test with a short
  interval).
- **bus routing:** a frame published to `presence:{room}` is delivered to that
  room's local members (generalized `roomFromChannel`).
- All concurrency tests run under `-race`.

## Acceptance criteria

- Joining a room adds the client to `presence:{room}` and broadcasts a snapshot
  including it; leaving/disconnecting removes it and broadcasts a snapshot
  without it (verified with fakes).
- A `Snapshot` excludes members whose score is older than the TTL window
  (verified against miniredis).
- A `typing` inbound frame results in a `typing` frame published to
  `presence:{room}` carrying the sender's id (verified with the loopback bus).
- The heartbeat goroutine refreshes presence scores on its interval.
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
- Two browser tabs joining the same room each show the other in the presence
  list and see "typing…" while the other types.
