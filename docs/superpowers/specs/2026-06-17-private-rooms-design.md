# Design — Private Rooms + Room CRUD

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (deferred feature)

## Context

Rooms are currently implicit: created on first JOIN, no metadata, and anyone can
join any room string. This adds **public + private (invite-token) rooms** and a
**room CRUD REST API** — the last unbuilt feature from the original spec's
"Required features".

## Goals

- Register rooms with a visibility (`public`/`private`); private rooms require an
  invite token to join.
- CRUD rooms over REST.
- Keep ad-hoc public chat working (a JOIN to an unregistered room stays open).

## Non-goals

- Room ownership / membership lists, token rotation, per-room rate limits, auth
  roles. Anyone may create/list/get/delete rooms; invite tokens are the only
  gate, and only for joining private rooms.

## Decisions

- **Model: public-open + private-registered.** No DB record → public/implicit
  (join allowed). A registered `private` room requires its invite token; a
  registered `public` room joins freely.
- **Room id: client-chosen** on create; duplicate → 409. Private rooms get a
  server-generated invite token.
- **Token transport:** a new optional `Frame.Token` field on the JOIN frame.
- **Fail-closed** on a room-lookup DB error during JOIN: reject that join rather
  than risk treating a private room as joinable.

## Architecture

A `rooms` table in Postgres holds registered rooms. The gateway reads it to gate
JOINs and exposes CRUD on `/api/rooms`. The pattern mirrors history: the gateway
owns the `RoomStore` interface + `RoomRecord` type; an adapter in `main` bridges
to `persistence.PgxStore`. The persistence worker is unaffected.

```
POST/GET/DELETE /api/rooms ──→ gateway REST ──→ rooms table
JOIN {room, token} ──→ gateway looks up room ──→ private? require token : allow
```

## Schema

```sql
CREATE TABLE IF NOT EXISTS rooms (
  id           TEXT PRIMARY KEY,
  visibility   TEXT NOT NULL,          -- 'public' | 'private'
  invite_token TEXT,                   -- set only for private rooms
  created_ms   BIGINT NOT NULL
);
```

## Components

### `internal/persistence/store_pg.go` (modify)

- `Migrate` also runs the `rooms` `CREATE TABLE IF NOT EXISTS`.
- `RoomRecord{ID, Visibility, InviteToken string; CreatedMS int64}`.
- `CreateRoom(ctx, RoomRecord) error` — `INSERT`; on PK conflict return the
  exported sentinel `ErrRoomExists`.
- `GetRoom(ctx, id) (RoomRecord, bool, error)` — found flag false when absent.
- `ListRooms(ctx) ([]RoomRecord, error)` — all rooms ordered by id.
- `DeleteRoom(ctx, id) error`.

### `internal/gateway/rooms.go` (new)

```go
type RoomRecord struct {
	ID          string
	Visibility  string
	InviteToken string
}

var ErrRoomExists = errors.New("room already exists")

type RoomStore interface {
	Lookup(ctx context.Context, id string) (RoomRecord, bool, error)
	Create(ctx context.Context, r RoomRecord) error // ErrRoomExists on dup
	List(ctx context.Context) ([]RoomRecord, error)
	Delete(ctx context.Context, id string) error
}

func newInviteToken() string // 16 random bytes hex
```

### `internal/gateway/protocol.go` (modify)

Add `Token string \`json:"token,omitempty"\`` to `Frame`.

### `internal/gateway/client.go` (modify)

- `clientConfig` gains `rooms RoomStore`; `Client` stores it.
- JOIN gate, synchronous, before joining (in `handleFrame` `TypeJoin`, after the
  empty-room check, before adding to `joined`/`hub.Join`):
  - `rec, found, err := c.rooms.Lookup(ctx, f.Room)` (2s timeout context).
  - `err != nil` → `enqueue(errorFrame("room unavailable"))`, return (fail-closed).
  - `found && rec.Visibility == "private" && rec.InviteToken != f.Token` →
    `enqueue(errorFrame("invalid invite token"))`, return.
  - otherwise proceed with the existing join (set joined, `hub.Join`, replay,
    presence).

### `internal/gateway/server.go` (modify)

- `Server` holds the `RoomStore` (added to `NewServer`).
- Routes:
  - `POST /api/rooms` — body `{"id":...,"visibility":"public|private"}`. Validate
    id non-empty and visibility ∈ {public,private} (else 400). Private →
    generate an invite token. `Create`; `ErrRoomExists` → 409. Respond 201 with
    `{"id","visibility","invite_token"}` (token omitted/empty for public).
  - `GET /api/rooms` — list registered rooms as `[{"id","visibility"}]` — **never
    include invite tokens**.
  - `GET /api/rooms/{id}` — `{"id","visibility"}` or 404 (no token).
  - `DELETE /api/rooms/{id}` — 204; deleting an absent room is also 204.
- The existing `GET /api/rooms/{room}/messages` (history) stays; chi routes the
  distinct paths.

### `cmd/gateway/main.go` (modify)

`roomAdapter` wrapping `*persistence.PgxStore`, mapping `persistence.RoomRecord`
↔ `gateway.RoomRecord` and `persistence` `ErrRoomExists` ↔ `gateway.ErrRoomExists`.

### `web/index.html` (modify)

Add an invite-token input sent in the JOIN frame's `token` field (creating rooms
is done via the REST API, documented in the README).

## Error handling

- JOIN to a private room with a missing/wrong token → `error` frame, not joined.
- Room-lookup DB error on JOIN → fail-closed (reject that join). Already-joined
  members are unaffected.
- `POST` duplicate id → 409; invalid visibility / empty id → 400; `GET {id}` not
  found → 404.

## Testing

- **persistence (testcontainers, `t.Skip` without Docker):** create then get;
  list returns it; duplicate id → `ErrRoomExists`; delete removes it; get-absent
  → not found.
- **gateway JOIN gate (fake `RoomStore`):** unregistered/public → joins
  (`hub.Join` called); private + correct token → joins; private + wrong/empty
  token → `error` frame and **no** `hub.Join`; lookup error → rejected, no join.
- **gateway REST (httptest + fake store):** POST private returns a non-empty
  token; POST public returns no token; POST duplicate → 409; POST bad visibility
  → 400; GET list omits tokens; GET `{id}` 404 when absent; DELETE → 204.
- All concurrency tests run under `-race`.

## Acceptance criteria

- A room created `private` requires its returned token to JOIN; with the right
  token the JOIN succeeds, with a wrong/empty token it is rejected with an
  `error` frame and the client is not added to the room.
- A JOIN to an unregistered room still works (public-implicit).
- `GET /api/rooms` and `GET /api/rooms/{id}` never expose invite tokens.
- Creating a duplicate id returns 409; bad visibility returns 400.
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
