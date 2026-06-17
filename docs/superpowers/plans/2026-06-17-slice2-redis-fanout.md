# Slice 2 — Redis Pub/Sub Fan-out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route chat messages through Redis pub/sub so fan-out works across multiple gateway replicas, behind a clean `Bus` seam, without changing the slice-1 wire protocol.

**Architecture:** A new `Bus` interface (Redis implementation `RedisBus`) carries messages between gateways. On SEND the hub PUBLISHes to `room:{id}` instead of fanning out locally; a single per-gateway multiplexed subscriber receives messages for subscribed rooms and delivers them to local members via `deliverLocal`. Rooms subscribe on first local join and unsubscribe on reap.

**Tech Stack:** Go 1.22+, `github.com/redis/go-redis/v9`, `github.com/alicebob/miniredis/v2` (tests), existing `nhooyr.io/websocket` + `chi`.

**Commit convention:** Commit locally on `main`. Do NOT push. Do NOT add any Claude/Anthropic attribution. Use `git -c commit.gpgsign=false commit`.

**-race needs a C compiler on this machine.** Prefix race-test Bash commands with:
```
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
```

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `internal/gateway/bus.go` | **new** | `Bus` interface, `RedisBus`, `roomChannel`/`controlChannel` |
| `internal/gateway/bus_test.go` | **new** | `RedisBus` tests against miniredis |
| `internal/gateway/bus_fake_test.go` | **new** | `fakeBus` loopback test double |
| `internal/gateway/hub.go` | modify | `NewHub(bus)`, subscribe/unsubscribe, `Publish`, `deliverLocal`, bus handler |
| `internal/gateway/client.go` | modify | `roomRegistry.Publish`, SEND calls `Publish` |
| `internal/gateway/server.go` | modify | `NewServer(hub, bus, …)`, `/readyz` pings bus |
| `cmd/gateway/main.go` | modify | build `RedisBus`, ping at startup, `Close` on shutdown |
| `internal/gateway/hub_test.go` | modify | use `fakeBus`, assert subscribe/publish/reap |
| `internal/gateway/client_test.go` | modify | fake registry `Publish`, publish-error test |
| `internal/gateway/server_test.go` | modify | construct with a bus, redis-down readyz test |
| `internal/gateway/multigateway_test.go` | **new** | cross-gateway fan-out via miniredis |
| `docker-compose.yml` | **new** | Redis 7 service |
| `README.md` | modify | Redis prerequisite + docker compose |

---

### Task 1: Add dependencies

**Files:** `go.mod`, `go.sum`

- [ ] **Step 1: Add the Redis client and miniredis**

Run:
```bash
go get github.com/redis/go-redis/v9@v9.7.0
go get github.com/alicebob/miniredis/v2@v2.33.0
```
Expected: both added to `go.mod`. (Patch versions may differ slightly; that is fine as long as the build and tests pass.)

- [ ] **Step 2: Verify the module still builds**

Run:
```bash
go build ./...
```
Expected: clean, exit 0. (Nothing imports the new deps yet; they will appear as `// indirect` until later tasks — do NOT run `go mod tidy` yet, it would drop them.)

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git -c commit.gpgsign=false commit -m "Add go-redis and miniredis dependencies"
```

---

### Task 2: Redis bus (`bus.go`)

**Files:**
- Create: `internal/gateway/bus.go`
- Test: `internal/gateway/bus_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/gateway/bus_test.go`:
```go
package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisBusPublishReachesSubscribedHandler(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()

	got := make(chan string, 1)
	bus.SetHandler(func(channel string, payload []byte) {
		got <- channel + "|" + string(payload)
	})
	if err := bus.Subscribe(context.Background(), "room:x"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), "room:x", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case s := <-got:
		if s != "room:x|hello" {
			t.Fatalf("got %q, want room:x|hello", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within 2s")
	}
}

func TestRedisBusUnsubscribeStopsDelivery(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()

	got := make(chan string, 1)
	bus.SetHandler(func(channel string, payload []byte) { got <- string(payload) })

	if err := bus.Subscribe(context.Background(), "room:x"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Unsubscribe(context.Background(), "room:x"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), "room:x", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case s := <-got:
		t.Fatalf("expected no delivery after unsubscribe, got %q", s)
	case <-time.After(300 * time.Millisecond):
		// success: nothing delivered
	}
}

func TestRedisBusPing(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()

	if err := bus.Ping(context.Background()); err != nil {
		t.Fatalf("ping against live miniredis failed: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
go test ./internal/gateway/ -run TestRedisBus -v
```
Expected: FAIL — `undefined: NewRedisBus`.

- [ ] **Step 3: Implement `bus.go`**

Create `internal/gateway/bus.go`:
```go
package gateway

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	roomChannelPrefix = "room:"
	// controlChannel keeps the pub/sub connection active even when no rooms
	// are subscribed yet, avoiding empty-subscription edge cases.
	controlChannel = "gateway:control"
)

func roomChannel(id string) string { return roomChannelPrefix + id }

func roomFromChannel(channel string) string {
	return strings.TrimPrefix(channel, roomChannelPrefix)
}

// Bus is the cross-gateway message transport. A payload published to a channel
// is delivered to every gateway subscribed to that channel.
type Bus interface {
	Publish(ctx context.Context, channel string, payload []byte) error
	Subscribe(ctx context.Context, channel string) error
	Unsubscribe(ctx context.Context, channel string) error
	SetHandler(func(channel string, payload []byte))
	Ping(ctx context.Context) error
	Close() error
}

// RedisBus implements Bus over a single Redis pub/sub connection. One receive
// goroutine ranges over incoming messages and invokes the handler.
type RedisBus struct {
	rdb    *redis.Client
	pubsub *redis.PubSub
}

// NewRedisBus connects to Redis at addr and subscribes to an internal control
// channel so the pub/sub connection is always active.
func NewRedisBus(addr string) *RedisBus {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	pubsub := rdb.Subscribe(context.Background(), controlChannel)
	return &RedisBus{rdb: rdb, pubsub: pubsub}
}

func (b *RedisBus) Publish(ctx context.Context, channel string, payload []byte) error {
	return b.rdb.Publish(ctx, channel, payload).Err()
}

func (b *RedisBus) Subscribe(ctx context.Context, channel string) error {
	return b.pubsub.Subscribe(ctx, channel)
}

func (b *RedisBus) Unsubscribe(ctx context.Context, channel string) error {
	return b.pubsub.Unsubscribe(ctx, channel)
}

// SetHandler registers the delivery callback and starts the receive goroutine.
// Call exactly once before messages are expected. The goroutine exits when the
// pub/sub connection is closed by Close.
func (b *RedisBus) SetHandler(handler func(channel string, payload []byte)) {
	go func() {
		for msg := range b.pubsub.Channel() {
			if msg.Channel == controlChannel {
				continue
			}
			handler(msg.Channel, []byte(msg.Payload))
		}
	}()
}

func (b *RedisBus) Ping(ctx context.Context) error {
	return b.rdb.Ping(ctx).Err()
}

func (b *RedisBus) Close() error {
	_ = b.pubsub.Close()
	return b.rdb.Close()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
go test ./internal/gateway/ -run TestRedisBus -v
```
Expected: PASS (3 tests). If `TestRedisBusPublishReachesSubscribedHandler` is flaky/times out due to go-redis `Channel()` interaction with miniredis, that surfaces here — the control-channel subscription in `NewRedisBus` is the mitigation; do not proceed until green.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/bus.go internal/gateway/bus_test.go
git -c commit.gpgsign=false commit -m "Add Redis bus with pub/sub fan-out"
```

---

### Task 3: Wire the gateway through the bus

This task changes interdependent signatures (`NewHub`, `NewServer`, `roomRegistry`) across `hub.go`, `client.go`, `server.go`, and `main.go`, plus their tests. The package will not compile midway, so all changes land in **one commit**. Write the tests first (red), then the production code (green).

**Files:**
- Create: `internal/gateway/bus_fake_test.go`, `internal/gateway/multigateway_test.go`
- Modify: `internal/gateway/hub_test.go`, `internal/gateway/client_test.go`, `internal/gateway/server_test.go`
- Modify: `internal/gateway/hub.go`, `internal/gateway/client.go`, `internal/gateway/server.go`, `cmd/gateway/main.go`

- [ ] **Step 1: Create the loopback fake bus**

Create `internal/gateway/bus_fake_test.go`:
```go
package gateway

import (
	"context"
	"sync"
)

// fakeBus is an in-memory loopback Bus for tests: a Publish to a subscribed
// channel is delivered synchronously to the handler, simulating this gateway's
// own subscription receiving the message. It records calls for assertions.
type fakeBus struct {
	mu         sync.Mutex
	handler    func(channel string, payload []byte)
	subscribed map[string]bool
	published  []busMsg
	subCount   int
	unsubCount int
	pingErr    error
}

type busMsg struct {
	channel string
	payload []byte
}

func newFakeBus() *fakeBus {
	return &fakeBus{subscribed: make(map[string]bool)}
}

func (b *fakeBus) Publish(_ context.Context, channel string, payload []byte) error {
	b.mu.Lock()
	b.published = append(b.published, busMsg{channel: channel, payload: payload})
	h := b.handler
	deliver := b.subscribed[channel]
	b.mu.Unlock()
	if deliver && h != nil {
		h(channel, payload)
	}
	return nil
}

func (b *fakeBus) Subscribe(_ context.Context, channel string) error {
	b.mu.Lock()
	b.subscribed[channel] = true
	b.subCount++
	b.mu.Unlock()
	return nil
}

func (b *fakeBus) Unsubscribe(_ context.Context, channel string) error {
	b.mu.Lock()
	delete(b.subscribed, channel)
	b.unsubCount++
	b.mu.Unlock()
	return nil
}

func (b *fakeBus) SetHandler(h func(channel string, payload []byte)) {
	b.mu.Lock()
	b.handler = h
	b.mu.Unlock()
}

func (b *fakeBus) Ping(_ context.Context) error { return b.pingErr }
func (b *fakeBus) Close() error                 { return nil }

func (b *fakeBus) isSubscribed(channel string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subscribed[channel]
}

func (b *fakeBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.published)
}

func (b *fakeBus) subscribeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subCount
}
```

- [ ] **Step 2: Replace `hub_test.go`**

Replace the entire contents of `internal/gateway/hub_test.go` with:
```go
package gateway

import "testing"

func TestHubLazyCreateAndReapAndSubscribe(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}

	h.Join("general", a)
	if h.roomCount() != 1 {
		t.Fatalf("expected 1 room, got %d", h.roomCount())
	}
	if !bus.isSubscribed(roomChannel("general")) {
		t.Fatalf("expected subscription to %q after join", roomChannel("general"))
	}

	h.Leave("general", a)
	if h.roomCount() != 0 {
		t.Fatalf("expected room reaped, got %d", h.roomCount())
	}
	if bus.isSubscribed(roomChannel("general")) {
		t.Fatalf("expected unsubscribe from %q after reap", roomChannel("general"))
	}
}

func TestHubSubscribesOncePerRoom(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("general", &fakeMember{id: "a"})
	h.Join("general", &fakeMember{id: "b"})
	if bus.subscribeCount() != 1 {
		t.Fatalf("expected 1 subscribe for two joiners of same room, got %d", bus.subscribeCount())
	}
}

func TestHubPublishGoesToBus(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("general", &fakeMember{id: "a"})

	if err := h.Publish("general", messageFrame("general", "a", "hi", 1)); err != nil {
		t.Fatal(err)
	}
	if bus.publishCount() != 1 {
		t.Fatalf("expected 1 publish, got %d", bus.publishCount())
	}
}

func TestHubRoundTripReachesMembers(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Join("general", a)
	h.Join("general", b)

	// Publish loops back through the subscribed bus to local members.
	if err := h.Publish("general", messageFrame("general", "a", "hi", 1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return hasText(a.frames(), "hi") && hasText(b.frames(), "hi") })
}

func TestHubLeaveUnknownRoomIsNoop(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}
	h.Leave("ghost", a)
	if h.roomCount() != 0 {
		t.Fatalf("expected 0 rooms, got %d", h.roomCount())
	}
}

func TestHubCloseAllClosesRegisteredClients(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Register(a)
	h.Register(b)

	h.CloseAll("bye")

	if a.closed != "bye" || b.closed != "bye" {
		t.Fatalf("expected both closed with reason, got a=%q b=%q", a.closed, b.closed)
	}
}
```

- [ ] **Step 3: Update `client_test.go`**

In `internal/gateway/client_test.go`, change the import block from `import (\n\t"testing"\n)` to:
```go
import (
	"errors"
	"testing"
)
```

Replace the `fakeRegistry` type and its methods (the block from `type fakeRegistry struct {` through the `Broadcast` method) with:
```go
// fakeRegistry records hub calls so handleFrame can be tested in isolation.
type fakeRegistry struct {
	joined     []string
	left       []string
	published  []Frame
	publishErr error
}

func (f *fakeRegistry) Join(roomID string, m member)  { f.joined = append(f.joined, roomID) }
func (f *fakeRegistry) Leave(roomID string, m member) { f.left = append(f.left, roomID) }
func (f *fakeRegistry) Publish(roomID string, fr Frame) error {
	f.published = append(f.published, fr)
	return f.publishErr
}
```

In `TestHandleSendRequiresJoin`, change `reg.broadcast` to `reg.published` (the assertion `if len(reg.broadcast) != 0` becomes `if len(reg.published) != 0`).

In `TestHandleJoinThenSend`, change both `reg.broadcast` references to `reg.published` (`if len(reg.broadcast) != 1` → `if len(reg.published) != 1`; `got := reg.broadcast[0]` → `got := reg.published[0]`).

Append this new test:
```go
func TestHandleSendPublishErrorReturnsErrorFrame(t *testing.T) {
	reg := &fakeRegistry{publishErr: errors.New("boom")}
	c := newClient(reg, func() {})
	c.handleFrame(Frame{Type: TypeJoin, Room: "general"})
	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected error frame on publish failure, got %+v", out)
	}
}
```

- [ ] **Step 4: Update `server_test.go`**

In `internal/gateway/server_test.go`, add `"errors"` to the import block (alongside the existing imports).

Replace the `newTestServer` function with:
```go
func newTestServer() *Server {
	bus := newFakeBus()
	hub := NewHub(bus)
	return NewServer(hub, bus, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}
```

In `TestEndToEndFanout`, replace the line:
```go
	srv := NewServer(NewHub(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
```
with:
```go
	srv := newTestServer()
```

In `TestMalformedJSONReturnsError`, replace the same `srv := NewServer(NewHub(), …)` line with:
```go
	srv := newTestServer()
```

Append this new test:
```go
func TestReadyzFailsWhenRedisDown(t *testing.T) {
	bus := newFakeBus()
	bus.pingErr = errors.New("redis down")
	hub := NewHub(bus)
	srv := NewServer(hub, bus, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with redis down = %d, want 503", rec.Code)
	}
}
```

- [ ] **Step 5: Create the cross-gateway test**

Create `internal/gateway/multigateway_test.go`:
```go
package gateway

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// TestCrossGatewayFanout is the headline test: a publish on one gateway's hub
// reaches a member connected to a different gateway's hub, via Redis.
func TestCrossGatewayFanout(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	busA := NewRedisBus(mr.Addr())
	defer busA.Close()
	busB := NewRedisBus(mr.Addr())
	defer busB.Close()

	hubA := NewHub(busA)
	hubB := NewHub(busB)

	b := &fakeMember{id: "b"}
	hubB.Join("x", b) // gateway B subscribes room:x
	a := &fakeMember{id: "a"}
	hubA.Join("x", a) // gateway A subscribes room:x

	if err := hubA.Publish("x", messageFrame("x", "a", "hello", 1)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return hasText(b.frames(), "hello") })
}
```

- [ ] **Step 6: Run tests to verify they fail (compile error = red)**

Run:
```bash
go test ./internal/gateway/ -run TestHub 2>&1 | head -20
```
Expected: FAIL to compile — `not enough arguments in call to NewHub`, `h.Publish undefined`, etc. This is the red state; production code is next.

- [ ] **Step 7: Replace `hub.go`**

Replace the entire contents of `internal/gateway/hub.go` with:
```go
package gateway

import (
	"context"
	"sync"
)

// Hub is a registry of rooms and connected clients, fronting a Bus for
// cross-gateway fan-out. The mutex guards the maps; it is NOT held during
// fan-out (that lives in each Room's goroutine).
type Hub struct {
	mu      sync.Mutex
	rooms   map[string]*Room
	clients map[string]member
	bus     Bus
}

// NewHub wires the hub to a Bus and registers the bus delivery handler.
func NewHub(bus Bus) *Hub {
	h := &Hub{
		rooms:   make(map[string]*Room),
		clients: make(map[string]member),
		bus:     bus,
	}
	bus.SetHandler(h.onBusMessage)
	return h
}

// onBusMessage is invoked by the bus for every message on a subscribed channel.
// It decodes the frame and delivers it to local members of the room.
func (h *Hub) onBusMessage(channel string, payload []byte) {
	f, err := decodeFrame(payload)
	if err != nil {
		return
	}
	h.deliverLocal(roomFromChannel(channel), f)
}

// Register/Unregister track every connected client so CloseAll can reach
// clients that are connected but not in any room.
func (h *Hub) Register(m member) {
	h.mu.Lock()
	h.clients[m.ID()] = m
	h.mu.Unlock()
}

func (h *Hub) Unregister(m member) {
	h.mu.Lock()
	delete(h.clients, m.ID())
	h.mu.Unlock()
}

// Join adds a member to a room, creating the room (and subscribing the bus to
// its channel) on first use. The lock is held across the channel send so Join
// and Leave fully serialize, preventing a reap from racing an in-flight join.
func (h *Hub) Join(roomID string, m member) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[roomID]
	if !ok {
		r = newRoom(roomID)
		h.rooms[roomID] = r
		go r.run()
		_ = h.bus.Subscribe(context.Background(), roomChannel(roomID))
	}
	r.join <- m
}

// Leave removes a member and reaps the room (unsubscribing the bus) if it became
// empty. The reply from the room arrives while the lock is held, so the reap
// decision is atomic with respect to Join.
func (h *Hub) Leave(roomID string, m member) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[roomID]
	if !ok {
		return
	}
	reply := make(chan bool, 1)
	r.leave <- leaveReq{m: m, empty: reply}
	if <-reply {
		close(r.done)
		delete(h.rooms, roomID)
		_ = h.bus.Unsubscribe(context.Background(), roomChannel(roomID))
	}
}

// Publish sends a frame to all gateways (including this one) by publishing it to
// the room's bus channel. The message returns via the subscription and is then
// delivered locally, so the sender sees its own message too.
func (h *Hub) Publish(roomID string, f Frame) error {
	payload, err := f.encode()
	if err != nil {
		return err
	}
	return h.bus.Publish(context.Background(), roomChannel(roomID), payload)
}

// deliverLocal fans a frame out to local members of a room. It is called only by
// the bus handler. If the room was reaped concurrently, the select on r.done
// abandons the send instead of blocking forever.
func (h *Hub) deliverLocal(roomID string, f Frame) {
	h.mu.Lock()
	r, ok := h.rooms[roomID]
	h.mu.Unlock()
	if !ok {
		return
	}
	select {
	case r.broadcast <- f:
	case <-r.done:
	}
}

func (h *Hub) CloseAll(reason string) {
	h.mu.Lock()
	members := make([]member, 0, len(h.clients))
	for _, m := range h.clients {
		members = append(members, m)
	}
	h.mu.Unlock()
	for _, m := range members {
		m.close(reason)
	}
}

// roomCount is a test helper for asserting lazy creation and reaping.
func (h *Hub) roomCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms)
}
```

- [ ] **Step 8: Edit `client.go`**

In `internal/gateway/client.go`, replace the `roomRegistry` interface:
```go
type roomRegistry interface {
	Join(roomID string, m member)
	Leave(roomID string, m member)
	Broadcast(roomID string, f Frame)
}
```
with:
```go
type roomRegistry interface {
	Join(roomID string, m member)
	Leave(roomID string, m member)
	Publish(roomID string, f Frame) error
}
```

In the `TypeSend` case of `handleFrame`, replace:
```go
		c.hub.Broadcast(f.Room, messageFrame(f.Room, c.id, f.Text, nowMillis()))
```
with:
```go
		if err := c.hub.Publish(f.Room, messageFrame(f.Room, c.id, f.Text, nowMillis())); err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
```

- [ ] **Step 9: Replace `server.go`**

Replace the entire contents of `internal/gateway/server.go` with:
```go
package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"nhooyr.io/websocket"
)

// pinger reports backing-store health; the Redis bus implements it.
type pinger interface {
	Ping(ctx context.Context) error
}

type Server struct {
	hub      *Hub
	bus      pinger
	log      *slog.Logger
	webDir   string
	draining atomic.Bool
}

func NewServer(hub *Hub, bus pinger, log *slog.Logger, webDir string) *Server {
	return &Server{hub: hub, bus: bus, log: log, webDir: webDir}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/ws", s.handleWS)
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	// InsecureSkipVerify allows the local demo page to connect during dev.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		s.log.Warn("ws accept failed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	client := newClient(s.hub, cancel)
	s.hub.Register(client)
	s.log.Info("client connected", "id", client.ID())

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

- [ ] **Step 10: Replace `cmd/gateway/main.go`**

Replace the entire contents of `cmd/gateway/main.go` with:
```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/gateway"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	webDir := os.Getenv("WEB_DIR")
	if webDir == "" {
		webDir = "web"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	bus := gateway.NewRedisBus(redisAddr)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := bus.Ping(pingCtx); err != nil {
		pingCancel()
		log.Error("cannot reach redis", "addr", redisAddr, "err", err)
		os.Exit(1)
	}
	pingCancel()

	hub := gateway.NewHub(bus)
	srv := gateway.NewServer(hub, bus, log, webDir)
	httpServer := &http.Server{Addr: addr, Handler: srv.Router()}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("gateway listening", "addr", addr, "redis", redisAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown initiated")

	srv.SetDraining(true)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hub.CloseAll("server shutting down")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	_ = bus.Close()
	log.Info("shutdown complete")
}
```

- [ ] **Step 11: Tidy modules, build, vet**

Run:
```bash
go mod tidy
go build ./... && go vet ./...
```
Expected: clean, exit 0. (`go mod tidy` now promotes go-redis and miniredis to direct deps since production/test code imports them.)

- [ ] **Step 12: Run the full race suite, repeated**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go test ./... -race -count=3
```
Expected: all `ok`, no race warnings, across all 3 iterations — including `TestCrossGatewayFanout` and the updated hub/client/server tests. If `TestCrossGatewayFanout` is flaky, the likely cause is gateway B's subscription not being active before A publishes; since `hubB.Join` calls `bus.Subscribe` synchronously this should hold, but if needed add a short `waitFor`-style retry — do not weaken the assertion.

- [ ] **Step 13: Commit**

```bash
git add internal/gateway/ cmd/gateway/main.go go.mod go.sum
git -c commit.gpgsign=false commit -m "Wire gateway fan-out through the Redis bus"
```

---

### Task 4: docker-compose + README

**Files:**
- Create: `docker-compose.yml`
- Modify: `README.md`

- [ ] **Step 1: Create `docker-compose.yml`**

Create `docker-compose.yml`:
```yaml
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "no"]
```

- [ ] **Step 2: Update the README**

In `README.md`, replace the `## Run` section (from `## Run` up to but not including `## Endpoints`) with:
```markdown
## Run

Slice 2 requires Redis. Start it with Docker:

```bash
docker compose up -d        # starts Redis 7 on localhost:6379
go run ./cmd/gateway
```

Open http://localhost:8080/ in two browser tabs, join the same room, and chat.
Run a second gateway on another port (`ADDR=:8081 go run ./cmd/gateway`) and the
two replicas fan out to each other through Redis.

Environment variables:

- `ADDR` — listen address (default `:8080`)
- `WEB_DIR` — directory served at `/` (default `web`)
- `REDIS_ADDR` — Redis address (default `localhost:6379`)
```

In the `## Roadmap` section, update the line so slice 2 is marked done:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres history → presence/typing →
rate limiting/auth → observability → K8s + load test.
```

- [ ] **Step 3: Verify the compose file parses**

Run:
```bash
docker compose config >/dev/null && echo "compose ok"
```
Expected: `compose ok`. (This only validates YAML; it does not start anything.)

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml README.md
git -c commit.gpgsign=false commit -m "Add docker-compose Redis and update README"
```

---

### Task 5: Final verification

- [ ] **Step 1: Full suite**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./... && go test ./... -race -count=2
```
Expected: builds clean, vet clean, all tests `ok`, no race warnings.

- [ ] **Step 2: Confirm no stray binary committed**

Run:
```bash
git status --porcelain
```
Expected: empty (clean tree). If a `gateway.exe` appeared from `go build`, delete it; do not commit it.

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** Bus interface + RedisBus (Task 2), round-trip publish/deliverLocal + multiplexed subscriber via single PubSub (Tasks 2-3), subscribe-on-first-join / unsubscribe-on-reap (Task 3 hub), `/readyz` pings Redis (Task 3 server), Redis required + startup ping + `REDIS_ADDR` (Task 3 main), docker-compose Redis (Task 4), miniredis tests incl. cross-gateway fan-out (Tasks 2-3). System join/leave stay local (unchanged room.go) — cross-gateway presence is slice 4, intentionally out of scope.
- **Signature consistency:** `NewHub(bus Bus)`, `NewServer(hub *Hub, bus pinger, log, webDir)`, `roomRegistry.Publish(roomID string, f Frame) error`, `Hub.Publish(...) error`, `Hub.deliverLocal(...)` (unexported, bus-handler only). `*Hub` satisfies `roomRegistry`; `*RedisBus` and `fakeBus` satisfy `Bus` and `pinger`.
- **Why one big commit in Task 3:** the signature changes are mutually dependent; the package cannot compile until hub/client/server/main change together. Tests are written first (Step 6 shows the red compile state).
- **Known tradeoff:** `Subscribe`/`Unsubscribe` run under the hub lock (a brief Redis round-trip). Accepted for slice 2; revisit if contention shows up.
