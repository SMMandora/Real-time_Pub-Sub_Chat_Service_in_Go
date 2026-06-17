# Slice 4 — Presence + Typing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show cross-gateway room presence (who's in a room) and ephemeral typing indicators, over a `presence:{room}` side channel that is never persisted.

**Architecture:** Presence lives in a Redis sorted set `presence:{room}` (score = last-heartbeat ms). Each connection heartbeats its joined rooms every 10s; join/leave update the set and publish a full snapshot to the `presence:{room}` pub/sub channel; gateways deliver snapshots and typing frames to local room members. The persistence worker only subscribes `room:*`, so presence/typing are never stored.

**Tech Stack:** Go 1.22+, existing go-redis, chi, miniredis (tests).

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
| `internal/gateway/protocol.go` | modify | `Frame.Members`; `TypePresence`/`TypeTyping`; `presenceFrame`/`typingFrame` |
| `internal/gateway/bus.go` | modify | `presenceChannel`; generalized `roomFromChannel` |
| `internal/gateway/presence.go` | **new** | `PresenceStore` interface, `RedisPresenceStore` |
| `internal/gateway/presence_test.go` | **new** | `RedisPresenceStore` tests (miniredis) |
| `internal/gateway/hub.go` | modify | subscribe `presence:{id}`; `PublishPresence` |
| `internal/gateway/client.go` | modify | presence store; heartbeat; add/remove on join/leave; typing |
| `internal/gateway/presence_fake_test.go` | **new** | `fakePresenceStore` |
| `internal/gateway/client_test.go` | modify | `fakeRegistry.PublishPresence`; new ctor; presence/typing/heartbeat tests |
| `internal/gateway/server.go` | modify | `NewServer` gains presence; pass to clients |
| `internal/gateway/server_test.go` | modify | constructor arity |
| `cmd/gateway/main.go` | modify | construct `RedisPresenceStore`, pass to server |
| `web/index.html` | modify | presence list + typing indicator |
| `README.md` | modify | presence/typing docs |

Presence ops on **join/leave run synchronously** in the frame handler (a few fast Redis calls; joins/leaves are infrequent). Only the **heartbeat is a goroutine**. Replay (slice 3b) remains async.

---

### Task 1: Presence store + side-channel protocol (additive)

**Files:**
- Create: `internal/gateway/presence.go`, `internal/gateway/presence_test.go`
- Modify: `internal/gateway/protocol.go`, `internal/gateway/bus.go`

- [ ] **Step 1: Write `presence_test.go`**

Create `internal/gateway/presence_test.go`:
```go
package gateway

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisPresenceAddSnapshotRemove(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	ps := NewRedisPresenceStore(mr.Addr())
	defer ps.Close()
	ctx := context.Background()

	if err := ps.Add(ctx, "x", "a", 1000); err != nil {
		t.Fatal(err)
	}
	if err := ps.Add(ctx, "x", "b", 2000); err != nil {
		t.Fatal(err)
	}

	got, err := ps.Snapshot(ctx, "x", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 members, got %v", got)
	}

	// minScore filter excludes the stale member (score 1000 < 1500).
	got, err = ps.Snapshot(ctx, "x", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected [b], got %v", got)
	}

	if err := ps.Remove(ctx, "x", "a"); err != nil {
		t.Fatal(err)
	}
	if err := ps.Remove(ctx, "x", "b"); err != nil {
		t.Fatal(err)
	}
	got, _ = ps.Snapshot(ctx, "x", 0)
	if len(got) != 0 {
		t.Fatalf("expected empty after removing both, got %v", got)
	}
}
```

> **Note:** adding `Members []string` makes `Frame` non-comparable, so the existing `protocol_test.go` (`TestFrameConstructors`, `TestFrameRoundTrip`) must switch their `!=` Frame comparisons to `reflect.DeepEqual` (add the `reflect` import) and be included in this task's commit.

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run TestRedisPresence 2>&1 | head -10
```
Expected: FAIL to compile — `undefined: NewRedisPresenceStore`.

- [ ] **Step 3: Add protocol additions**

In `internal/gateway/protocol.go`, add `Members []string` to the `Frame` struct (after the `Message` field):
```go
	Members []string `json:"members,omitempty"`
```
Add to the `const (...)` type block:
```go
	TypePresence = "presence"
	TypeTyping   = "typing"
```
Add these constructors (next to `messageFrame`):
```go
func presenceFrame(room string, members []string) Frame {
	return Frame{Type: TypePresence, Room: room, Members: members}
}

func typingFrame(room, from string) Frame {
	return Frame{Type: TypeTyping, Room: room, From: from}
}
```

- [ ] **Step 4: Add bus side-channel helpers**

In `internal/gateway/bus.go`, add a presence channel prefix to the `const` block:
```go
	presenceChannelPrefix = "presence:"
```
Add the channel helper (next to `roomChannel`):
```go
func presenceChannel(id string) string { return presenceChannelPrefix + id }
```
Replace `roomFromChannel` so it strips whichever prefix applies:
```go
func roomFromChannel(channel string) string {
	if strings.HasPrefix(channel, presenceChannelPrefix) {
		return strings.TrimPrefix(channel, presenceChannelPrefix)
	}
	return strings.TrimPrefix(channel, roomChannelPrefix)
}
```

- [ ] **Step 5: Create `presence.go`**

Create `internal/gateway/presence.go`:
```go
package gateway

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// heartbeatInterval is how often a connection refreshes its presence score.
	heartbeatInterval = 10 * time.Second
	// presenceTTLms is the window (ms) within which a member counts as present.
	presenceTTLms int64 = 30000
)

// PresenceStore tracks per-room membership with last-heartbeat scores.
type PresenceStore interface {
	Add(ctx context.Context, room, member string, scoreMs int64) error
	Remove(ctx context.Context, room, member string) error
	Snapshot(ctx context.Context, room string, minScoreMs int64) ([]string, error)
}

func presenceKey(room string) string { return "presence:" + room }

// RedisPresenceStore implements PresenceStore over a Redis sorted set per room.
type RedisPresenceStore struct {
	rdb *redis.Client
}

func NewRedisPresenceStore(addr string) *RedisPresenceStore {
	return &RedisPresenceStore{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (p *RedisPresenceStore) Add(ctx context.Context, room, member string, scoreMs int64) error {
	return p.rdb.ZAdd(ctx, presenceKey(room), redis.Z{Score: float64(scoreMs), Member: member}).Err()
}

func (p *RedisPresenceStore) Remove(ctx context.Context, room, member string) error {
	return p.rdb.ZRem(ctx, presenceKey(room), member).Err()
}

func (p *RedisPresenceStore) Snapshot(ctx context.Context, room string, minScoreMs int64) ([]string, error) {
	return p.rdb.ZRangeByScore(ctx, presenceKey(room), &redis.ZRangeBy{
		Min: strconv.FormatInt(minScoreMs, 10),
		Max: "+inf",
	}).Result()
}

func (p *RedisPresenceStore) Close() error { return p.rdb.Close() }
```

- [ ] **Step 6: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/gateway/ -race
```
Expected: build+vet clean; `ok`, no race warnings (existing tests + the new presence test pass).

- [ ] **Step 7: Commit**

```bash
git add internal/gateway/presence.go internal/gateway/presence_test.go internal/gateway/protocol.go internal/gateway/bus.go
git -c commit.gpgsign=false commit -m "Add presence store and side-channel protocol"
```

---

### Task 2: Wire presence + typing into the gateway

Signature changes to `newClient` and `NewServer` ripple to `main.go` and tests, so this lands in **one commit**. Tests first (red), then production.

**Files:**
- Create: `internal/gateway/presence_fake_test.go`
- Modify: `internal/gateway/client_test.go`, `internal/gateway/server_test.go`
- Modify: `internal/gateway/hub.go`, `internal/gateway/client.go`, `internal/gateway/server.go`, `cmd/gateway/main.go`

- [ ] **Step 1: Create the fake presence store**

Create `internal/gateway/presence_fake_test.go`:
```go
package gateway

import (
	"context"
	"sort"
	"sync"
)

type fakePresenceStore struct {
	mu       sync.Mutex
	members  map[string]map[string]int64 // room -> member -> score
	addCalls int
	err      error
}

func newFakePresenceStore() *fakePresenceStore {
	return &fakePresenceStore{members: make(map[string]map[string]int64)}
}

func (s *fakePresenceStore) Add(_ context.Context, room, member string, score int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addCalls++
	if s.err != nil {
		return s.err
	}
	if s.members[room] == nil {
		s.members[room] = make(map[string]int64)
	}
	s.members[room][member] = score
	return nil
}

func (s *fakePresenceStore) Remove(_ context.Context, room, member string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.members[room] != nil {
		delete(s.members[room], member)
	}
	return nil
}

func (s *fakePresenceStore) Snapshot(_ context.Context, room string, minScore int64) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	var out []string
	for m, sc := range s.members[room] {
		if sc >= minScore {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *fakePresenceStore) addCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addCalls
}
```

- [ ] **Step 2: Update `client_test.go`**

Add `PublishPresence` to `fakeRegistry` (and a `presence []Frame` field). Change the struct to add the field:
```go
	presence   []Frame
```
and add the method (next to the other fakeRegistry methods):
```go
func (f *fakeRegistry) PublishPresence(room string, fr Frame) error {
	f.presence = append(f.presence, fr)
	return nil
}
```

Update the `newTestClient` helper to supply a presence store:
```go
func newTestClient(reg roomRegistry, hist history, cancel context.CancelFunc) *Client {
	return newClient(context.Background(), reg, hist, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), cancel)
}
```

Append these tests:
```go
func TestJoinAddsPresenceAndBroadcastsSnapshot(t *testing.T) {
	reg := &fakeRegistry{}
	ps := newFakePresenceStore()
	c := newClient(context.Background(), reg, &fakeHistory{}, ps, slog.New(slog.NewTextHandler(io.Discard, nil)), func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	if ps.addCallCount() == 0 {
		t.Fatal("expected presence Add on join")
	}
	if len(reg.presence) != 1 || reg.presence[0].Type != TypePresence || reg.presence[0].Room != "x" {
		t.Fatalf("expected one presence snapshot frame, got %+v", reg.presence)
	}
	if len(reg.presence[0].Members) != 1 || reg.presence[0].Members[0] != c.ID() {
		t.Fatalf("expected snapshot to contain joiner, got %+v", reg.presence[0].Members)
	}
}

func TestLeaveRemovesPresenceAndBroadcasts(t *testing.T) {
	reg := &fakeRegistry{}
	ps := newFakePresenceStore()
	c := newClient(context.Background(), reg, &fakeHistory{}, ps, slog.New(slog.NewTextHandler(io.Discard, nil)), func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeLeave, Room: "x"})

	// After leave, the latest snapshot must not contain the member.
	last := reg.presence[len(reg.presence)-1]
	if len(last.Members) != 0 {
		t.Fatalf("expected empty snapshot after leave, got %+v", last.Members)
	}
}

func TestTypingPublishesTypingFrame(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeTyping, Room: "x"})

	var typing *Frame
	for i := range reg.presence {
		if reg.presence[i].Type == TypeTyping {
			typing = &reg.presence[i]
		}
	}
	if typing == nil || typing.Room != "x" || typing.From != c.ID() {
		t.Fatalf("expected a typing frame from %s in room x, got %+v", c.ID(), reg.presence)
	}
}

func TestTypingRequiresJoin(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})

	c.handleFrame(Frame{Type: TypeTyping, Room: "x"})

	for _, f := range reg.presence {
		if f.Type == TypeTyping {
			t.Fatal("typing before join should not publish")
		}
	}
}

func TestHeartbeatRefreshesPresence(t *testing.T) {
	reg := &fakeRegistry{}
	ps := newFakePresenceStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newClient(ctx, reg, &fakeHistory{}, ps, slog.New(slog.NewTextHandler(io.Discard, nil)), cancel)
	c.hbInterval = 10 * time.Millisecond

	c.mu.Lock()
	c.joined["x"] = true
	c.mu.Unlock()

	go c.heartbeat()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ps.addCallCount() >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected heartbeat to call Add repeatedly, got %d", ps.addCallCount())
}
```

(Note: `TestHeartbeatRefreshesPresence` writes `c.joined` under `c.mu` because the heartbeat goroutine reads it under the same lock.)

- [ ] **Step 3: Update `server_test.go` constructor arity**

In `server_test.go`, every `NewServer(...)` call gains a presence-store argument before the logger. Update `newTestServer`:
```go
func newTestServer() *Server {
	bus := newFakeBus()
	hub := NewHub(bus)
	return NewServer(hub, bus, &fakeHistory{}, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}
```
And in `TestReadyzFailsWhenRedisDown`, `TestReadyzFailsWhenPostgresDown`, `TestHistoryEndpointReturnsMessages`, `TestHistoryEndpointBeforeParam`, `TestHistoryEndpointStoreErrorReturns503`, change each `NewServer(NewHub(bus), bus, hist, slog.New(...), "web")` to insert `newFakePresenceStore()` before the logger:
```go
	srv := NewServer(NewHub(bus), bus, hist, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
```
(`hist` is whatever that test already constructs.)

- [ ] **Step 4: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run "TestJoinAddsPresence|TestTyping|TestHeartbeat" 2>&1 | head -20
```
Expected: FAIL to compile — `not enough arguments in call to newClient`, `c.hbInterval undefined`, `c.heartbeat undefined`, `*fakeRegistry does not implement roomRegistry (missing method PublishPresence)`.

- [ ] **Step 5: Update `hub.go`**

In `internal/gateway/hub.go`, in `Join`, after the existing `room:` subscribe, also subscribe the presence channel:
```go
		_ = h.bus.Subscribe(context.Background(), roomChannel(roomID))
		_ = h.bus.Subscribe(context.Background(), presenceChannel(roomID))
```
In `Leave`, after the existing `room:` unsubscribe, also unsubscribe presence:
```go
		_ = h.bus.Unsubscribe(context.Background(), roomChannel(roomID))
		_ = h.bus.Unsubscribe(context.Background(), presenceChannel(roomID))
```
Add a `PublishPresence` method (after `Publish`):
```go
// PublishPresence broadcasts a presence or typing frame on the room's side
// channel. Unlike Publish it assigns no id and is never persisted.
func (h *Hub) PublishPresence(roomID string, f Frame) error {
	payload, err := f.encode()
	if err != nil {
		return err
	}
	return h.bus.Publish(context.Background(), presenceChannel(roomID), payload)
}
```

- [ ] **Step 6: Update `client.go`**

In `internal/gateway/client.go`:

Add `PublishPresence` to the `roomRegistry` interface:
```go
type roomRegistry interface {
	Join(roomID string, m member)
	Leave(roomID string, m member)
	Publish(roomID string, f Frame) error
	PublishPresence(roomID string, f Frame) error
}
```

Add `presence` and `hbInterval` fields to the `Client` struct (after `history`):
```go
	presence   PresenceStore
	hbInterval time.Duration
```

Update `newClient` to accept and set them:
```go
func newClient(ctx context.Context, hub roomRegistry, hist history, presence PresenceStore, log *slog.Logger, cancel context.CancelFunc) *Client {
	return &Client{
		id:         newID(),
		ctx:        ctx,
		hub:        hub,
		history:    hist,
		presence:   presence,
		hbInterval: heartbeatInterval,
		log:        log,
		send:       make(chan Frame, 16),
		cancel:     cancel,
		joined:     make(map[string]bool),
	}
}
```

In `handleFrame`, the `TypeJoin` case (inside `if !already`) gains a synchronous presence add after the async replay:
```go
		if !already {
			c.hub.Join(f.Room, c)
			go c.replay(f.Room, f.ID)
			c.addPresence(f.Room)
		}
```
The `TypeLeave` case (inside `if was`) gains a presence remove:
```go
		if was {
			c.hub.Leave(f.Room, c)
			c.removePresence(f.Room)
		}
```
Add a new `TypeTyping` case (before `default`):
```go
	case TypeTyping:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if joined {
			_ = c.hub.PublishPresence(f.Room, typingFrame(f.Room, c.id))
		}
```

In `leaveAll`, remove presence for each room after leaving:
```go
	for _, room := range rooms {
		c.hub.Leave(room, c)
		c.removePresence(room)
	}
```

Add these methods (after `replay`):
```go
// addPresence records this client in the room's presence set and broadcasts the
// updated snapshot. Synchronous: join/leave are infrequent and the Redis calls
// are small. Uses a background context so it also works during teardown.
func (c *Client) addPresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.presence.Add(ctx, room, c.id, nowMillis()); err != nil {
		c.log.Warn("presence add failed", "room", room, "err", err)
		return
	}
	c.publishSnapshot(ctx, room)
}

func (c *Client) removePresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.presence.Remove(ctx, room, c.id)
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
				_ = c.presence.Add(ctx, room, c.id, nowMillis())
			}
			cancel()
		}
	}
}
```

- [ ] **Step 7: Update `server.go`**

In `internal/gateway/server.go`, add a `presence` field to `Server`:
```go
	presence PresenceStore
```
Update `NewServer`:
```go
func NewServer(hub *Hub, bus pinger, hist history, presence PresenceStore, log *slog.Logger, webDir string) *Server {
	return &Server{hub: hub, bus: bus, hist: hist, presence: presence, log: log, webDir: webDir}
}
```
In `handleWS`, start the heartbeat and pass the presence store into `newClient`:
```go
	client := newClient(ctx, s.hub, s.hist, s.presence, s.log, cancel)
	s.hub.Register(client)
	s.log.Info("client connected", "id", client.ID())
	go client.heartbeat()
```

- [ ] **Step 8: Update `cmd/gateway/main.go`**

In `cmd/gateway/main.go`, after the Redis bus is constructed and pinged, build the presence store and pass it to `NewServer`. Add after the bus ping block (and before `pool, err := pgxpool.New...` or wherever the server is built — place the presence construction just before `srv := gateway.NewServer(...)`):
```go
	presence := gateway.NewRedisPresenceStore(redisAddr)
	defer presence.Close()
```
Change the server construction to include it:
```go
	srv := gateway.NewServer(hub, bus, hist, presence, log, webDir)
```

- [ ] **Step 9: Tidy, build, vet, race (repeated)**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go mod tidy
go build ./... && go vet ./...
go test ./... -race -count=3
```
Expected: build+vet clean; all tests `ok` across 3 runs (PG integration tests skip), no race warnings — including the new presence/typing/heartbeat tests.

- [ ] **Step 10: Commit**

```bash
git add internal/gateway/ cmd/gateway/main.go go.mod go.sum
git -c commit.gpgsign=false commit -m "Wire presence heartbeats and typing into the gateway"
```

---

### Task 3: Demo UI + README + final verification

**Files:**
- Modify: `web/index.html`, `README.md`

- [ ] **Step 1: Add presence + typing to the demo**

In `web/index.html`:

(a) Add two elements after the `<div id="log"></div>` line:
```html
  <div id="presence" class="sys"></div>
  <div id="typing" class="sys"></div>
```

(b) In the `<script>`, just after `const seenIds = new Set();`, add:
```js
    let typingTimer = null;
```

(c) In the `ws.onmessage` handler, add two branches to the if/else chain — after the existing `else if (f.type === "error") ...` line, append:
```js
        else if (f.type === "presence") {
          document.getElementById("presence").textContent =
            "present: " + (f.members || []).join(", ");
        }
        else if (f.type === "typing") {
          const el = document.getElementById("typing");
          el.textContent = f.from + " is typing…";
          clearTimeout(typingTimer);
          typingTimer = setTimeout(() => { el.textContent = ""; }, 3000);
        }
```

(d) Make the message input send a throttled typing frame. Find the message input element (`id="text"`) usage and add a listener near the bottom of the script (before the closing `</script>`):
```js
    let lastTyping = 0;
    document.getElementById("text").addEventListener("input", () => {
      const now = Date.now();
      if (room && ws && ws.readyState === WebSocket.OPEN && now - lastTyping > 1500) {
        lastTyping = now;
        ws.send(JSON.stringify({ type: "typing", room }));
      }
    });
```

After editing, read the file back and verify the JS is well-formed (matched braces; the if/else chain ends cleanly).

- [ ] **Step 2: Update the README**

In `README.md`, under `## Endpoints`, extend the `GET /ws` frame description to mention the new frames. Replace the `GET /ws` bullet block:
```markdown
- `GET /ws` — WebSocket. JSON frames: `{"type":"join","room":"general"}`,
  `{"type":"send","room":"general","text":"hi"}`, `{"type":"leave","room":"general"}`.
  Server sends `message`, `system`, and `error` frames.
```
with:
```markdown
- `GET /ws` — WebSocket. Client frames: `join` (optional `id` = last-seen
  cursor), `send`, `leave`, `typing`. Server frames: `message`, `system`,
  `error`, `presence` (member list), `typing`. Presence and typing ride a
  `presence:{room}` side channel and are never persisted.
```
Update the `## Roadmap` line to:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history + replay (done) → presence + typing (done) → rate limiting/auth → observability → K8s + load test.
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
git -c commit.gpgsign=false commit -m "Add presence list and typing indicator to demo, document frames"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `PresenceStore`/`RedisPresenceStore` sorted set (Task 1), `presence:{room}` side channel + generalized routing (Task 1 bus + Task 2 hub), `Frame.Members` + presence/typing frames (Task 1), gateway-driven heartbeat goroutine 10s / TTL 30s (Task 2 client), snapshot-on-change publish (Task 2 client `publishSnapshot`), typing publish on the side channel (Task 2 client), demo presence list + typing indicator (Task 3). Out-of-scope (usernames/auth, rate limiting) absent — member identity is the client id.
- **Signature consistency:** `PresenceStore{Add,Remove,Snapshot}`; `newClient(ctx, hub, hist, presence, log, cancel)`; `NewServer(hub, bus, hist, presence, log, webDir)`; `roomRegistry` gains `PublishPresence`; `Hub.PublishPresence`. `NewHub` is unchanged (presence publishing uses the existing bus). `fakePresenceStore` satisfies `PresenceStore`; `fakeRegistry` now satisfies `roomRegistry` (incl. `PublishPresence`).
- **Sync vs async:** join/leave presence ops are synchronous in the frame handler (small, infrequent); only the heartbeat is a goroutine, and it touches the presence store (which is mutex-safe in the fake). `fakeRegistry` is therefore only touched synchronously and needs no mutex.
- **Teardown ctx:** `addPresence`/`removePresence`/`heartbeat` use `context.Background()` (not the connection ctx) for the Redis calls, so presence removal still runs while the connection is being torn down.
- **Known limitation (documented in spec):** a crashed gateway's members fall out of snapshots via the score filter but are not actively re-broadcast until the next join/leave in that room.
