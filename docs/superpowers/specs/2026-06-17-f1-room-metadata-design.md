# Design — F1: Room Metadata + Online Count

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Frontend program (slice F1 of F1–F5)

## Context

The reference UI shows room directory cards with a name, description, and a live
"● N online" count, and a creation modal with name + description. The rooms table
currently holds only id/visibility/invite_token. F1 adds the metadata and the
online count so the directory/modal show real data.

## Goals

- Rooms carry a `name` and `description`.
- `GET /api/rooms` (and `GET /api/rooms/{id}`) return those plus a live `online`
  count per room.

## Non-goals

- Membership/roster (F2), metrics proxy (F3), the React app (F4/F5).

## Schema

`Migrate` additionally runs (idempotent, upgrades existing tables):
```sql
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS name        TEXT NOT NULL DEFAULT '';
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
```

## Components

### `internal/persistence/rooms.go`

- `RoomRecord` gains `Name string`, `Description string`.
- `CreateRoom` inserts name/description; `GetRoom`/`ListRooms` select them.

### `internal/gateway/rooms.go`

- `gateway.RoomRecord` gains `Name`, `Description`.

### `internal/gateway/server.go`

- `roomView` gains `Name`, `Description`, and `Online int` (json: `name`,
  `description`, `online`).
- `handleCreateRoom` reads `name`/`description` from the body (both optional;
  default empty) and passes them to `Create`.
- `handleListRooms` / `handleGetRoom`: for each room, compute
  `online = len(presence.Snapshot(ctx, room.ID, now − presenceTTLms))` using the
  presence store the server already holds (via `clientCfg.presence`). Build the
  `roomView` with name/description/visibility/online. Tokens still never
  serialized.

### `cmd/gateway/main.go`

- `roomAdapter` maps the new `Name`/`Description` fields both directions.

### `web/index.html`

- The throwaway demo's create flow is unaffected (it doesn't create rooms); no
  change required. (The React app in F4 uses these fields.)

## Data flow

`POST /api/rooms {id, name?, description?, visibility}` → stored. `GET /api/rooms`
→ `{rooms:[{id, name, description, visibility, online}]}`; `online` is the count
of members with a fresh heartbeat in `presence:{id}`.

## Error handling

- Same as today: dup id → 409, bad visibility → 400. Name/description are free
  text (empty allowed); no new validation.
- A presence snapshot error while listing → that room's `online` is reported as
  0 (best-effort; the list still returns).

## Testing

- **persistence (testcontainers, skips w/o Docker):** create with
  name/description; get/list return them.
- **gateway REST (httptest + fakes):** the server needs the presence store, so
  reuse the existing fakes; `GET /api/rooms` includes `name`/`description` and an
  `online` count derived from a fake presence store seeded with members; create
  accepts name/description; tokens still absent from list/get.
- All concurrency tests under `-race`.

## Acceptance criteria

- `POST /api/rooms` stores name/description; `GET /api/rooms` and
  `GET /api/rooms/{id}` return them plus a per-room `online` count; tokens never
  exposed.
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
