# Slice 5 — Username Auth + Rate Limiting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Identify users by a validated username (the public identity in messages, presence, typing) and rate-limit each user to 30 messages/minute via a Redis token bucket.

**Architecture:** A new `RateLimiter` (Redis token-bucket Lua script) gates the SEND path. Usernames arrive as a `/ws?username=` query param, validated and rejected with 400 if invalid; the username replaces the connection id as `from`/presence/typing identity (the 8-hex id stays for internal bookkeeping). The growing client constructor is grouped into a `clientConfig`.

**Tech Stack:** Go 1.22+, existing go-redis (Lua via `redis.Script`), chi, miniredis (tests).

**Commit convention:** Commit locally on `main`. Do NOT push. No Claude/Anthropic attribution. Use `git -c commit.gpgsign=false commit`.

**-race needs a C compiler on this machine.** Prefix race-test Bash commands with:
```
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
```

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `internal/gateway/ratelimit.go` | **new** | `RateLimiter`, `RedisRateLimiter` token-bucket Lua |
| `internal/gateway/ratelimit_test.go` | **new** | limiter tests (miniredis) |
| `internal/gateway/ratelimit_fake_test.go` | **new** | `fakeRateLimiter` |
| `internal/gateway/client.go` | modify | `clientConfig`; username + limiter; SEND rate check; username identity |
| `internal/gateway/client_test.go` | modify | new ctor/helpers; identity assertions; rate tests |
| `internal/gateway/server.go` | modify | `validUsername`; `?username` in handleWS; `NewServer` + `clientCfg` |
| `internal/gateway/server_test.go` | modify | ctor arity; username on e2e dials; validUsername/400 tests |
| `cmd/gateway/main.go` | modify | construct `RedisRateLimiter` |
| `web/index.html` | modify | prompt username, connect with `?username=` |
| `README.md` | modify | auth + rate-limit docs |

---

### Task 1: Rate limiter (additive)

**Files:**
- Create: `internal/gateway/ratelimit.go`, `internal/gateway/ratelimit_test.go`

- [ ] **Step 1: Write `ratelimit_test.go`**

Create `internal/gateway/ratelimit_test.go`:
```go
package gateway

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisRateLimiterBurstThenBlock(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	// Small capacity and negligible refill make the burst deterministic.
	rl := NewRedisRateLimiter(mr.Addr(), 5, 0.0001)
	defer rl.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ok, err := rl.Allow(ctx, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	ok, err := rl.Allow(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("6th request should be blocked")
	}

	// A different user has an independent bucket.
	ok, err = rl.Allow(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("bob's first request should be allowed")
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run TestRedisRateLimiter 2>&1 | head -10
```
Expected: FAIL to compile — `undefined: NewRedisRateLimiter`.

- [ ] **Step 3: Implement `ratelimit.go`**

Create `internal/gateway/ratelimit.go`:
```go
package gateway

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter decides whether a user may send another message now.
type RateLimiter interface {
	Allow(ctx context.Context, user string) (bool, error)
}

// tokenBucketScript atomically refills based on elapsed time and consumes one
// token. KEYS[1] = bucket key; ARGV = capacity, refillPerSec, nowMs, cost.
var tokenBucketScript = redis.NewScript(`
local cap = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = cap
  ts = now
end
local elapsed = (now - ts) / 1000.0
tokens = math.min(cap, tokens + elapsed * refill)
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 120000)
return allowed
`)

// RedisRateLimiter implements a per-user token bucket in Redis.
type RedisRateLimiter struct {
	rdb          *redis.Client
	capacity     int
	refillPerSec float64
}

func NewRedisRateLimiter(addr string, capacity int, refillPerSec float64) *RedisRateLimiter {
	return &RedisRateLimiter{
		rdb:          redis.NewClient(&redis.Options{Addr: addr}),
		capacity:     capacity,
		refillPerSec: refillPerSec,
	}
}

func rateLimitKey(user string) string { return "ratelimit:" + user }

func (r *RedisRateLimiter) Allow(ctx context.Context, user string) (bool, error) {
	res, err := tokenBucketScript.Run(ctx, r.rdb, []string{rateLimitKey(user)},
		r.capacity, r.refillPerSec, time.Now().UnixMilli(), 1).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (r *RedisRateLimiter) Close() error { return r.rdb.Close() }
```

- [ ] **Step 4: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/gateway/ -run TestRedisRateLimiter -race -v
```
Expected: PASS. (miniredis runs the Lua via its embedded interpreter; `redis.Script.Run` falls back from EVALSHA to EVAL.) If miniredis cannot execute the script, STOP and report the exact error rather than rewriting the algorithm.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/ratelimit.go internal/gateway/ratelimit_test.go
git -c commit.gpgsign=false commit -m "Add Redis token-bucket rate limiter"
```

---

### Task 2: Wire auth + rate limiting into the gateway

Signature changes to `newClient`/`NewServer` ripple to tests and `main.go`; one commit. Tests first (red), then production.

**Files:**
- Create: `internal/gateway/ratelimit_fake_test.go`
- Modify: `internal/gateway/client_test.go`, `internal/gateway/server_test.go`
- Modify: `internal/gateway/client.go`, `internal/gateway/server.go`, `cmd/gateway/main.go`

- [ ] **Step 1: Create the fake limiter**

Create `internal/gateway/ratelimit_fake_test.go`:
```go
package gateway

import (
	"context"
	"sync"
)

// fakeRateLimiter is mutex-guarded because a server's shared instance is used
// concurrently by multiple connection goroutines in the WebSocket e2e tests.
type fakeRateLimiter struct {
	mu    sync.Mutex
	allow bool
	err   error
	calls int
}

func (f *fakeRateLimiter) Allow(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.allow, f.err
}
```

- [ ] **Step 2: Update `client_test.go`**

Replace `newTestClient` (the function at lines ~45-47) with the `clientConfig` version plus two helpers:
```go
func newTestClient(reg roomRegistry, hist history, cancel context.CancelFunc) *Client {
	cfg := clientConfig{
		hub:      reg,
		history:  hist,
		presence: newFakePresenceStore(),
		limiter:  &fakeRateLimiter{allow: true},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(context.Background(), "tester", cfg, cancel)
}

func newPresenceClient(ctx context.Context, reg roomRegistry, ps PresenceStore, cancel context.CancelFunc) *Client {
	cfg := clientConfig{
		hub:      reg,
		history:  &fakeHistory{},
		presence: ps,
		limiter:  &fakeRateLimiter{allow: true},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(ctx, "tester", cfg, cancel)
}

func newRateClient(reg roomRegistry, limiter RateLimiter) *Client {
	cfg := clientConfig{
		hub:      reg,
		history:  &fakeHistory{},
		presence: newFakePresenceStore(),
		limiter:  limiter,
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(context.Background(), "alice", cfg, func() {})
}
```

In `TestHandleJoinThenSend`, change the identity assertion `got.From != c.ID()` to `got.From != c.username`.

In `TestJoinAddsPresenceAndBroadcastsSnapshot`: replace its direct `newClient(...)` construction with `c := newPresenceClient(context.Background(), reg, ps, func() {})`, and change `reg.presence[0].Members[0] != c.ID()` to `!= c.username`.

In `TestLeaveRemovesPresenceAndBroadcasts`: replace its direct `newClient(...)` construction with `c := newPresenceClient(context.Background(), reg, ps, func() {})`.

In `TestTypingPublishesTypingFrame`: change `typing.From != c.ID()` to `typing.From != c.username` (and the `c.ID()` in the `t.Fatalf` format arg to `c.username`).

In `TestHeartbeatRefreshesPresence`: replace its direct `newClient(ctx, reg, ..., cancel)` construction with `c := newPresenceClient(ctx, reg, ps, cancel)` (keep the following `c.hbInterval = 10 * time.Millisecond` line).

Append the rate-limit tests:
```go
func TestSendBlockedByRateLimit(t *testing.T) {
	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: false})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 0 {
		t.Fatalf("rate-limited send must not publish, got %+v", reg.published)
	}
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected one rate-limit error frame, got %+v", out)
	}
}

func TestSendAllowedByRateLimit(t *testing.T) {
	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: true})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 1 || reg.published[0].From != "alice" {
		t.Fatalf("expected one published message from alice, got %+v", reg.published)
	}
}

func TestSendFailsOpenOnLimiterError(t *testing.T) {
	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: false, err: errors.New("redis down")})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 1 {
		t.Fatalf("a limiter error should fail open and publish, got %+v", reg.published)
	}
}
```

- [ ] **Step 3: Update `server_test.go`**

Add `"strings"` to the import block.

`NewServer` gains a `RateLimiter` argument before the logger. Update `newTestServer`:
```go
func newTestServer() *Server {
	bus := newFakeBus()
	hub := NewHub(bus)
	return NewServer(hub, bus, &fakeHistory{}, newFakePresenceStore(), &fakeRateLimiter{allow: true}, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}
```
In `TestReadyzFailsWhenRedisDown`, `TestReadyzFailsWhenPostgresDown`, `TestHistoryEndpointReturnsMessages`, `TestHistoryEndpointBeforeParam`, and `TestHistoryEndpointStoreErrorReturns503`, insert `&fakeRateLimiter{allow: true}` before the logger argument in each `NewServer(...)` call.

In `TestEndToEndFanout`, the two dials must carry a valid username. Change:
```go
	a, actx := dialWS(t, wsURL)
	...
	b, bctx := dialWS(t, wsURL)
```
to:
```go
	a, actx := dialWS(t, wsURL+"?username=alice")
	...
	b, bctx := dialWS(t, wsURL+"?username=bob")
```
In `TestMalformedJSONReturnsError`, change `c, ctx := dialWS(t, wsURL)` to `c, ctx := dialWS(t, wsURL+"?username=carol")`.

Append these tests:
```go
func TestValidUsername(t *testing.T) {
	cases := map[string]bool{
		"alice":                 true,
		"a_b-1":                 true,
		"ABC123":                true,
		strings.Repeat("a", 32): true,
		"":                      false,
		"bad!":                  false,
		"has space":             false,
		strings.Repeat("a", 33): false,
	}
	for in, want := range cases {
		if got := validUsername(in); got != want {
			t.Errorf("validUsername(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestWSRejectsInvalidUsername(t *testing.T) {
	srv := newTestServer()
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws?username=bad!", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid username, got %d", rec.Code)
	}
}
```

- [ ] **Step 4: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run "TestSend|TestValidUsername|TestWSRejects" 2>&1 | head -20
```
Expected: FAIL to compile — `not enough arguments in call to newClient`/`NewServer`, `c.username undefined`, `undefined: validUsername`, `clientConfig` undefined.

- [ ] **Step 5: Replace `client.go`**

Replace the entire contents of `internal/gateway/client.go` with:
```go
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// replayLimit caps how many history messages are replayed to a joining client.
const replayLimit = 100

// roomRegistry is the subset of the hub a client depends on, so handleFrame can
// be tested with a fake.
type roomRegistry interface {
	Join(roomID string, m member)
	Leave(roomID string, m member)
	Publish(roomID string, f Frame) error
	PublishPresence(roomID string, f Frame) error
}

// clientConfig groups the stable per-connection dependencies so newClient does
// not grow an unwieldy parameter list.
type clientConfig struct {
	hub      roomRegistry
	history  history
	presence PresenceStore
	limiter  RateLimiter
	log      *slog.Logger
}

// Client is one WebSocket connection. enqueue feeds the bounded send channel
// that writePump drains; overflow drops the client by cancelling its context.
type Client struct {
	id          string
	username    string
	ctx         context.Context
	hub         roomRegistry
	history     history
	presence    PresenceStore
	limiter     RateLimiter
	hbInterval  time.Duration
	log         *slog.Logger
	send        chan Frame
	cancel      context.CancelFunc
	once        sync.Once
	closeReason string

	mu     sync.Mutex
	joined map[string]bool
}

func newID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

func newClient(ctx context.Context, username string, cfg clientConfig, cancel context.CancelFunc) *Client {
	return &Client{
		id:         newID(),
		username:   username,
		ctx:        ctx,
		hub:        cfg.hub,
		history:    cfg.history,
		presence:   cfg.presence,
		limiter:    cfg.limiter,
		hbInterval: heartbeatInterval,
		log:        cfg.log,
		send:       make(chan Frame, 16),
		cancel:     cancel,
		joined:     make(map[string]bool),
	}
}

func (c *Client) ID() string { return c.id }

func (c *Client) enqueue(f Frame) {
	select {
	case c.send <- f:
	default:
		c.close("slow consumer")
	}
}

func (c *Client) close(reason string) {
	c.once.Do(func() {
		c.closeReason = reason
		c.cancel()
	})
}

func (c *Client) handleFrame(f Frame) {
	switch f.Type {
	case TypeJoin:
		if f.Room == "" {
			c.enqueue(errorFrame("join requires a room"))
			return
		}
		c.mu.Lock()
		already := c.joined[f.Room]
		c.joined[f.Room] = true
		c.mu.Unlock()
		if !already {
			c.hub.Join(f.Room, c)
			// Replay history to this client only. Async so readPump is not
			// blocked on the DB; the client deduplicates by id, so replayed
			// and live messages may overlap harmlessly.
			go c.replay(f.Room, f.ID)
			c.addPresence(f.Room)
		}
	case TypeLeave:
		c.mu.Lock()
		was := c.joined[f.Room]
		delete(c.joined, f.Room)
		c.mu.Unlock()
		if was {
			c.hub.Leave(f.Room, c)
			c.removePresence(f.Room)
		}
	case TypeSend:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if !joined {
			c.enqueue(errorFrame(fmt.Sprintf("not joined to room %q", f.Room)))
			return
		}
		if !c.allowSend() {
			c.enqueue(errorFrame("rate limit exceeded"))
			return
		}
		if err := c.hub.Publish(f.Room, messageFrame(f.Room, c.username, f.Text, nowMillis())); err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
	case TypeTyping:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if joined {
			_ = c.hub.PublishPresence(f.Room, typingFrame(f.Room, c.username))
		}
	default:
		c.enqueue(errorFrame("unknown frame type"))
	}
}

// allowSend consults the rate limiter. On a limiter error it fails open
// (allows) so a Redis blip does not block all chat.
func (c *Client) allowSend() bool {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	allowed, err := c.limiter.Allow(ctx, c.username)
	if err != nil {
		c.log.Warn("rate limit check failed", "user", c.username, "err", err)
		return true
	}
	return allowed
}

// replay fetches recent history (or messages after sinceID) and enqueues them
// to this client as message frames. Best-effort: on error it logs and returns.
func (c *Client) replay(room string, sinceID int64) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	var msgs []StoredMessage
	var err error
	if sinceID > 0 {
		msgs, err = c.history.Since(ctx, room, sinceID, replayLimit)
	} else {
		msgs, err = c.history.Recent(ctx, room, replayLimit)
	}
	if err != nil {
		c.log.Warn("history replay failed", "room", room, "err", err)
		return
	}
	for _, m := range msgs {
		c.enqueue(Frame{Type: TypeMessage, Room: room, ID: m.ID, From: m.From, Text: m.Text, TS: m.TS})
	}
}

// addPresence records this client in the room's presence set and broadcasts the
// updated snapshot. Synchronous: join/leave are infrequent and the Redis calls
// are small. Uses a background context so it also works during teardown.
func (c *Client) addPresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.presence.Add(ctx, room, c.username, nowMillis()); err != nil {
		c.log.Warn("presence add failed", "room", room, "err", err)
		return
	}
	c.publishSnapshot(ctx, room)
}

func (c *Client) removePresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.presence.Remove(ctx, room, c.username)
	c.publishSnapshot(ctx, room)
}

func (c *Client) publishSnapshot(ctx context.Context, room string) {
	members, err := c.presence.Snapshot(ctx, room, nowMillis()-presenceTTLms)
	if err != nil {
		c.log.Warn("presence snapshot failed", "room", room, "err", err)
		return
	}
	_ = c.hub.PublishPresence(room, presenceFrame(room, members))
}

// heartbeat periodically refreshes this client's presence score in every joined
// room. It does not publish (membership is unchanged); it just keeps scores
// fresh so the client is not pruned by the TTL filter.
func (c *Client) heartbeat() {
	ticker := time.NewTicker(c.hbInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			rooms := make([]string, 0, len(c.joined))
			for room := range c.joined {
				rooms = append(rooms, room)
			}
			c.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			for _, room := range rooms {
				_ = c.presence.Add(ctx, room, c.username, nowMillis())
			}
			cancel()
		}
	}
}

func (c *Client) leaveAll() {
	c.mu.Lock()
	rooms := make([]string, 0, len(c.joined))
	for room := range c.joined {
		rooms = append(rooms, room)
	}
	c.joined = make(map[string]bool)
	c.mu.Unlock()
	for _, room := range rooms {
		c.hub.Leave(room, c)
		c.removePresence(room)
	}
}

func (c *Client) readPump(ctx context.Context, conn *websocket.Conn) {
	conn.SetReadLimit(65536)
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		f, err := decodeFrame(data)
		if err != nil {
			c.enqueue(errorFrame("invalid JSON"))
			continue
		}
		c.handleFrame(f)
	}
}

func (c *Client) writePump(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case f := <-c.send:
			data, err := f.encode()
			if err != nil {
				continue
			}
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = conn.Write(wctx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
```

- [ ] **Step 6: Replace `server.go`**

Replace the entire contents of `internal/gateway/server.go` with:
```go
package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"nhooyr.io/websocket"
)

// pinger reports backing-store health; the Redis bus implements it.
type pinger interface {
	Ping(ctx context.Context) error
}

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

func validUsername(s string) bool { return usernamePattern.MatchString(s) }

type Server struct {
	hub       *Hub
	bus       pinger
	hist      history
	log       *slog.Logger
	webDir    string
	clientCfg clientConfig
	draining  atomic.Bool
}

func NewServer(hub *Hub, bus pinger, hist history, presence PresenceStore, limiter RateLimiter, log *slog.Logger, webDir string) *Server {
	return &Server{
		hub:    hub,
		bus:    bus,
		hist:   hist,
		log:    log,
		webDir: webDir,
		clientCfg: clientConfig{
			hub:      hub,
			history:  hist,
			presence: presence,
			limiter:  limiter,
			log:      log,
		},
	}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/ws", s.handleWS)
	r.Get("/api/rooms/{room}/messages", s.handleHistory)
	r.Handle("/*", http.FileServer(http.Dir(s.webDir)))
	return r
}

func (s *Server) SetDraining(v bool) { s.draining.Store(v) }

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.bus.Ping(ctx); err != nil {
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := s.hist.Ping(ctx); err != nil {
		http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func parseLimit(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 100
	}
	if n > 200 {
		return 200
	}
	return n
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	room := chi.URLParam(r, "room")
	limit := parseLimit(r.URL.Query().Get("limit"))

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var (
		msgs []StoredMessage
		err  error
	)
	if before := r.URL.Query().Get("before"); before != "" {
		beforeID, perr := strconv.ParseInt(before, 10, 64)
		if perr != nil {
			http.Error(w, "invalid before", http.StatusBadRequest)
			return
		}
		msgs, err = s.hist.Before(ctx, room, beforeID, limit)
	} else {
		msgs, err = s.hist.Recent(ctx, room, limit)
	}
	if err != nil {
		s.log.Warn("history query failed", "room", room, "err", err)
		http.Error(w, "history unavailable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Messages []StoredMessage `json:"messages"`
	}{Messages: msgs})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if !validUsername(username) {
		http.Error(w, "invalid username", http.StatusBadRequest)
		return
	}

	// InsecureSkipVerify allows the local demo page to connect during dev.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		s.log.Warn("ws accept failed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	client := newClient(ctx, username, s.clientCfg, cancel)
	s.hub.Register(client)
	s.log.Info("client connected", "id", client.ID(), "user", username)
	go client.heartbeat()

	defer func() {
		client.leaveAll()
		s.hub.Unregister(client)
		reason := client.closeReason
		if reason == "" {
			reason = "bye"
		}
		_ = conn.Close(websocket.StatusGoingAway, reason)
		s.log.Info("client disconnected", "id", client.ID())
	}()

	go client.writePump(ctx, conn)
	// readPump runs in the handler goroutine (not backgrounded) so that
	// http.Server.Shutdown blocks on this handler until the pump exits.
	// That is what drains in-flight clients during graceful shutdown.
	client.readPump(ctx, conn)
}
```

- [ ] **Step 7: Update `cmd/gateway/main.go`**

In `cmd/gateway/main.go`, after the presence store is constructed (the `presence := gateway.NewRedisPresenceStore(redisAddr)` / `defer presence.Close()` lines), add the limiter:
```go
	limiter := gateway.NewRedisRateLimiter(redisAddr, 30, 0.5)
	defer limiter.Close()
```
Change the server construction to include it:
```go
	srv := gateway.NewServer(hub, bus, hist, presence, limiter, log, webDir)
```

- [ ] **Step 8: Tidy, build, vet, race (repeated)**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go mod tidy
go build ./... && go vet ./...
go test ./... -race -count=3
```
Expected: build+vet clean; all tests `ok` across 3 runs (PG integration tests skip), no race warnings — including the new rate-limit, validUsername, and 400 tests, and the updated e2e tests.

- [ ] **Step 9: Commit**

```bash
git add internal/gateway/ cmd/gateway/main.go go.mod go.sum
git -c commit.gpgsign=false commit -m "Add username auth and per-user rate limiting"
```

---

### Task 3: Demo username + README + final verification

**Files:**
- Modify: `web/index.html`, `README.md`

- [ ] **Step 1: Prompt for a username and send it on connect**

In `web/index.html`, change the line `let room = null;` to add a username derived from a prompt right after it:
```js
    let room = null;
    const username = (prompt("Choose a username (letters, digits, _ or -):", "guest") || "guest")
      .replace(/[^A-Za-z0-9_-]/g, "").slice(0, 32) || "guest";
```
In `connect()`, change:
```js
      ws = new WebSocket(`ws://${location.host}/ws`);
```
to:
```js
      ws = new WebSocket(`ws://${location.host}/ws?username=${encodeURIComponent(username)}`);
```
(The client-side sanitize matches the server's `^[A-Za-z0-9_-]{1,32}$`, so a valid name is always sent. Rate-limit errors already render via the existing `error`-frame branch.)

- [ ] **Step 2: Update the README**

In `README.md`, replace the `GET /ws` bullet block:
```markdown
- `GET /ws` — WebSocket. Client frames: `join` (optional `id` = last-seen
  cursor), `send`, `leave`, `typing`. Server frames: `message`, `system`,
  `error`, `presence` (member list), `typing`. Presence and typing ride a
  `presence:{room}` side channel and are never persisted.
```
with:
```markdown
- `GET /ws?username=<name>` — WebSocket. `username` is required and must match
  `^[A-Za-z0-9_-]{1,32}$` (else HTTP 400); it is the identity in `message.from`,
  presence, and typing. Client frames: `join` (optional `id` = last-seen cursor),
  `send`, `leave`, `typing`. Server frames: `message`, `system`, `error`,
  `presence` (member list), `typing`. Each user is limited to 30 messages/minute
  (Redis token bucket); over-limit sends get an `error` frame. Presence and
  typing ride a `presence:{room}` side channel and are never persisted.
```
Update the `## Roadmap` line to:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history + replay (done) → presence + typing (done) → auth + rate limiting (done) → observability → K8s + load test.
```

- [ ] **Step 3: Final verification**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./... && go test ./... -race -count=2
```
Expected: builds clean, vet clean, all tests `ok` (PG tests skip), no race warnings.

- [ ] **Step 4: Confirm tree clean**

Run:
```bash
git status --porcelain
```
Expected: empty (ignore/delete any local `*.exe`).

- [ ] **Step 5: Commit**

```bash
git add web/index.html README.md
git -c commit.gpgsign=false commit -m "Demo username prompt and document auth + rate limiting"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `RateLimiter`/`RedisRateLimiter` token bucket 30/0.5 (Task 1), SEND rate check with fail-open (Task 2 client `allowSend`), username query param + `validUsername` + 400 (Task 2 server), username identity in `from`/presence/typing (Task 2 client), `clientConfig` grouping (Task 2), `RedisRateLimiter` wired in main (Task 2), demo username prompt (Task 3). Out-of-scope (passwords/OAuth/JWT) absent.
- **Signature consistency:** `RateLimiter{Allow}`; `newClient(ctx, username, cfg, cancel)`; `NewServer(hub, bus, hist, presence, limiter, log, webDir)`; `clientConfig{hub, history, presence, limiter, log}`. `fakeRateLimiter` satisfies `RateLimiter`; `RedisRateLimiter` too.
- **Identity:** `member.ID()` still returns the 8-hex connection id (room/hub bookkeeping unchanged); username is only the public identity. Multi-tab same-username presence caveat is documented in the spec.
- **fakeRateLimiter is mutex-guarded** because the server's shared instance is hit concurrently by multiple connection goroutines in the WebSocket e2e tests.
- **e2e dials now require `?username=`** or the upgrade is rejected with 400.
