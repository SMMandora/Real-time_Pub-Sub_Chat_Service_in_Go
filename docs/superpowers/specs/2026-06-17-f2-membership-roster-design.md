# Design — F2: Room Membership + Away/Offline Roster

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Frontend program (slice F2 of F1–F5)

## Context

The members panel in the reference UI groups members into Online / Away /
Offline. Presence only knows *connected* users, so Offline/Away members need a
persistent membership record with a last-seen timestamp.

## Goals

- Persist room membership with a `last_seen_ms` per (room, user).
- `GET /api/rooms/{id}/members` returns each member with a status derived from
  the live presence set + `last_seen_ms`.

## Status model (honest, derivable)

- **online** — username currently in `presence:{id}` (fresh heartbeat).
- **away** — not present, but `last_seen_ms` within the away window (5 min).
- **offline** — `last_seen_ms` older than the away window.

## Schema

`Migrate` additionally runs:
```sql
CREATE TABLE IF NOT EXISTS room_members (
  room_id      TEXT   NOT NULL,
  username     TEXT   NOT NULL,
  last_seen_ms BIGINT NOT NULL,
  PRIMARY KEY (room_id, username)
);
```

## Components

### `internal/persistence/rooms.go`

- `MemberRecord{Username string; LastSeenMs int64}`.
- `TouchMember(ctx, room, username string, lastSeenMs int64) error` — upsert
  (`INSERT … ON CONFLICT (room_id, username) DO UPDATE SET last_seen_ms = EXCLUDED.last_seen_ms`).
- `ListMembers(ctx, room string) ([]MemberRecord, error)` — ordered by username.

### `internal/gateway/rooms.go`

- `MemberRecord{Username string; LastSeenMs int64}`.
- `MemberStore` interface: `Touch(ctx, room, username string, lastSeenMs int64) error`,
  `List(ctx, room string) ([]MemberRecord, error)`.
- `awayWindowMs int64 = 300000` (5 min).

### `internal/gateway/server.go` — constructor refactor + roster

- **Refactor `NewServer` to take a `ServerConfig` struct** (it would otherwise
  reach 9 positional params): `ServerConfig{Hub, Bus, History, Presence,
  Limiter, Rooms, Members, Log, WebDir}`. The server builds its `clientConfig`
  (now including `members`) from it.
- New route `GET /api/rooms/{id}/members`:
  - `online := set(presence.Snapshot(ctx, id, now − presenceTTLms))`.
  - `members := membersStore.List(ctx, id)`.
  - status per member: `online` if in the online set; else `away` if
    `now − last_seen_ms ≤ awayWindowMs`; else `offline`.
  - respond `{"members":[{"username","status"}]}` ordered: online, away, offline,
    then by username. (No timestamps leaked beyond status.)

### `internal/gateway/client.go`

- `clientConfig` gains `members MemberStore`; `Client` stores it.
- On JOIN (after the gate passes, in the `if !already` block): `members.Touch(room, username, now)`.
- In the heartbeat goroutine: `members.Touch(room, username, now)` for each joined room (alongside the existing presence refresh).
- On LEAVE and in `leaveAll`: `members.Touch(room, username, now)` (records the leave time as last-seen).
- Touch failures are logged and ignored (best-effort; never block chat).

### `cmd/gateway/main.go`

- `memberAdapter` wrapping `*persistence.PgxStore` (Touch/List, mapping
  `persistence.MemberRecord` ↔ `gateway.MemberRecord`); wire into `ServerConfig`.

## Error handling

- A Touch error → logged, ignored (membership is best-effort).
- A roster lookup error (members list or presence) → 503 for the endpoint.

## Testing

- **persistence (testcontainers, skips w/o Docker):** Touch inserts then updates
  last_seen on conflict; List returns members ordered.
- **gateway roster (httptest + fakes):** seed a fake member store + fake presence
  store; assert status derivation — a present member is `online`, a member with
  recent last-seen not present is `away`, an old one is `offline`; ordering
  online→away→offline.
- **gateway client (fakes):** JOIN calls `members.Touch`; a fake member store
  records it. (The existing join tests get a no-op fake member store via the
  helper.)
- All tests run with `CGO_ENABLED=0 go test ./...` (the race detector's C
  compiler is currently unavailable on this machine).

## Acceptance criteria

- `GET /api/rooms/{id}/members` returns members with correct online/away/offline
  status from the presence set + last-seen, ordered online→away→offline.
- JOIN / heartbeat / leave update `last_seen_ms`.
- `NewServer(ServerConfig{…})` compiles and all call sites are updated.
- `CGO_ENABLED=0 go test ./...` passes (PG integration tests skip without Docker).
