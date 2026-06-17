# Slice 5 Design — Username Auth + Rate Limiting

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 5 of 7)

## Context

Slices 1-4 deliver multi-gateway chat with persistence, history, presence, and
typing, where identity is an anonymous 8-hex connection id. Slice 5 adds **simple
username auth** (the identity users actually see) and **rate limiting** (30
msg/min per user via a Redis token bucket).

### Roadmap

1-4. In-memory → Redis fan-out → persistence/history → presence/typing *(done)*
5. **Slice 5 — Username auth + rate limiting** *(this spec)*
6. Observability (Prometheus / Grafana / OTel / Jaeger)
7. K8s deploy + load test + public demo

## Goals

- Identify users by a chosen username, shown in messages, presence, and typing.
- Enforce 30 messages/minute per user with a Redis token bucket.

## Non-goals (explicit)

- Passwords, OAuth, JWT, sessions (the project's stated auth non-goal).
- Per-room or per-IP limits; disconnect-on-abuse; observability; K8s.

## Decisions

- **Username transport:** WebSocket query param `/ws?username=alice`. Validated
  against `^[A-Za-z0-9_-]{1,32}$`; a missing or invalid username is rejected with
  HTTP 400 before the WebSocket upgrade.
- **Identity:** the username is the public identity in `message.from`, presence
  members, and typing `from`. The internal 8-hex **connection id remains**
  `member.ID()` for hub/room bookkeeping, so multiple connections sharing a
  username are distinct members.
- **Rate limit:** 30 msg/min per username via a Redis **token bucket** (capacity
  30, refill 0.5 tokens/s) implemented as one atomic Lua script. An over-limit
  SEND yields an `error` frame and is dropped (not published/persisted); the
  connection stays open.
- **Limiter Redis error → fail-open:** log and allow the send, so a Redis blip
  does not block all chat (availability over strict enforcement).
- **Constructor grouping:** `newClient` would otherwise reach 8 parameters;
  group the stable deps into a `clientConfig` struct.

## Architecture

```
connect /ws?username=alice ─→ validate ─→ Client{id: 8hex (internal), username: "alice"}
SEND ─→ limiter.Allow(username)? ──no──→ error frame (dropped, not published)
                                 └─yes─→ hub.Publish(messageFrame(room, username, ...))
```

## Components

### `internal/gateway/ratelimit.go` (new)

```go
type RateLimiter interface {
	Allow(ctx context.Context, user string) (bool, error)
}
```

`RedisRateLimiter` holds a `*redis.Client`, a `*redis.Script` (token-bucket
Lua), `capacity`, and `refillPerSec`. `Allow` runs the script against
`ratelimit:{user}` with `now = time.Now().UnixMilli()` and cost 1, returning the
script's `1`/`0`. Constants: `rateLimitCapacity = 30`, `rateLimitRefillPerSec =
0.5` (30 per 60s). A `fakeRateLimiter` (settable allow/err) is used by tests.

Token-bucket Lua (`KEYS[1] = ratelimit:{user}`, `ARGV = capacity, refillPerSec,
nowMs, cost`):
- read `tokens`, `ts` (default to full capacity at `now` if absent);
- `tokens = min(capacity, tokens + (now-ts)/1000 * refillPerSec)`; `ts = now`;
- if `tokens >= cost`: `tokens -= cost`, allowed = 1, else allowed = 0;
- `HSET` tokens+ts, `PEXPIRE` 120000ms, return allowed.

### `internal/gateway/server.go` (modify)

- `validUsername(s string) bool` — `^[A-Za-z0-9_-]{1,32}$` (compiled once).
- `handleWS` reads `r.URL.Query().Get("username")`; if `!validUsername`, respond
  `http.Error(w, "invalid username", 400)` and return (no upgrade). Otherwise
  pass the username into `newClient`.
- `NewServer` gains a `RateLimiter`; it builds and stores a `clientConfig`.

### `internal/gateway/client.go` (modify)

```go
type clientConfig struct {
	hub      roomRegistry
	history  history
	presence PresenceStore
	limiter  RateLimiter
	log      *slog.Logger
}

func newClient(ctx context.Context, username string, cfg clientConfig, cancel context.CancelFunc) *Client
```

- `Client` gains `username string` and `limiter RateLimiter` (plus the existing
  fields, now populated from `cfg`).
- `member.ID()` still returns the internal connection id.
- SEND: after the joined check, call `limiter.Allow(ctx, c.username)`. On a
  `false` result → `error` frame "rate limit exceeded" and return without
  publishing. On limiter error → log and proceed (fail-open). Then
  `hub.Publish(messageFrame(room, c.username, text, now))`.
- `typing` frames and presence add/snapshot use `c.username` (not `c.id`).

### `cmd/gateway/main.go` (modify)

- Construct `RedisRateLimiter` from `REDIS_ADDR` with capacity 30, refill 0.5;
  pass it to `NewServer`.

### `web/index.html` (modify)

- Prompt for a username on load; connect to `/ws?username=<encoded>`. Rate-limit
  and other server errors already render via the existing `error`-frame branch.

## Error handling

- Missing/invalid username → HTTP 400, no WebSocket.
- Over-limit SEND → `error` frame, not published.
- Limiter Redis error → fail-open (log + allow).
- The rate check runs synchronously in the SEND path (one fast `EVAL`), bounded
  by a short timeout.

## Testing

- **`ratelimit_test.go` (miniredis):** a burst up to capacity is allowed and the
  next request is blocked; a second user has an independent bucket. (miniredis
  executes the Lua via its embedded interpreter.)
- **`client` (fakes):** SEND when `fakeRateLimiter` allows → `hub.Publish` called
  with `from = username`; when it blocks → `error` frame and no publish; an
  errored limiter still publishes (fail-open); `typing`/presence carry the
  username.
- **`server` (httptest):** `validUsername` table test; `/ws?username=bad!` → 400.
- All concurrency tests run under `-race`.

## Acceptance criteria

- A connection with a valid `?username=` uses that name as `from` in messages and
  as the presence/typing identity; an invalid/missing username yields HTTP 400.
- The Nth+1 message within the bucket window is rejected with an `error` frame and
  not published; tokens refill over time (verified against miniredis with a small
  capacity).
- A different user is rate-limited independently.
- A limiter error does not block sending (fail-open).
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
