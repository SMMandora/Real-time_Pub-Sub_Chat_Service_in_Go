# Slice 1 — In-Memory WebSocket Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A runnable single-replica Go WebSocket gateway with an in-memory room hub, a frozen JSON wire protocol, health endpoints, graceful shutdown, structured logging, and a static demo client.

**Architecture:** One `chi` HTTP server exposes `/ws` (WebSocket), `/healthz`, `/readyz`, and static files. A `Hub` is a mutex-guarded registry of `Room`s; each `Room` runs its own goroutine that solely owns its member set and serializes join/leave/broadcast over channels (no locks on the fan-out path). Each connection has a `readPump` and a `writePump` goroutine; a bounded send channel drops slow clients. Lifecycle (room reaping, shutdown drain) is coordinated through the hub.

**Tech Stack:** Go 1.22+, `nhooyr.io/websocket`, `github.com/go-chi/chi/v5`, stdlib `log/slog`, `encoding/json`.

**Commit convention:** Commit locally only. Do NOT push. Do NOT add any Claude/Anthropic co-author or attribution to commit messages.

**Module path:** `github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go`

---

## File Structure

| File | Responsibility |
|------|----------------|
| `go.mod` | Module + dependencies |
| `internal/gateway/protocol.go` | `Frame` struct, type constants, frame constructors, encode/decode |
| `internal/gateway/room.go` | `member` interface, `Room`, per-room goroutine `run` loop, fan-out |
| `internal/gateway/hub.go` | `Hub` registry: Join/Leave (with reaping), Broadcast, Register/Unregister/CloseAll |
| `internal/gateway/client.go` | `roomRegistry` interface, `Client`, id gen, enqueue/close, frame dispatch, read/write pumps |
| `internal/gateway/server.go` | `Server`: chi router, ws handler, health/readyz, static files, draining flag |
| `cmd/gateway/main.go` | Config from env, slog setup, signal handling, graceful shutdown wiring |
| `web/index.html` | Vanilla-JS demo client |
| `internal/gateway/*_test.go` | Unit + integration tests |
| `README.md` | Run/test/demo instructions |

Naming note: the interface for a room participant is `member` (not `client`) to avoid confusion with the `Client` struct.

---

### Task 1: Project scaffold

**Files:**
- Create: `go.mod`
- Create: `cmd/gateway/main.go` (temporary stub)

- [ ] **Step 1: Initialize the module**

Run:
```bash
go mod init github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go
```
Expected: creates `go.mod` with `go 1.2x` directive.

- [ ] **Step 2: Add a temporary main so the module builds**

Create `cmd/gateway/main.go`:
```go
package main

func main() {}
```

- [ ] **Step 3: Add dependencies**

Run:
```bash
go get nhooyr.io/websocket@v1.8.11
go get github.com/go-chi/chi/v5@v5.0.12
```
Expected: `go.mod` lists both; `go.sum` created.

- [ ] **Step 4: Verify it builds**

Run:
```bash
go build ./...
```
Expected: no output, exit 0.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum cmd/gateway/main.go
git commit -m "Scaffold Go module and dependencies"
```

---

### Task 2: Wire protocol (`protocol.go`)

**Files:**
- Create: `internal/gateway/protocol.go`
- Test: `internal/gateway/protocol_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/gateway/protocol_test.go`:
```go
package gateway

import (
	"encoding/json"
	"testing"
)

func TestFrameConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  Frame
		want Frame
	}{
		{
			name: "message",
			got:  messageFrame("general", "ab12", "hi", 1718600000000),
			want: Frame{Type: TypeMessage, Room: "general", From: "ab12", Text: "hi", TS: 1718600000000},
		},
		{
			name: "system",
			got:  systemFrame("general", "join", "ab12"),
			want: Frame{Type: TypeSystem, Room: "general", Event: "join", From: "ab12"},
		},
		{
			name: "error",
			got:  errorFrame("nope"),
			want: Frame{Type: TypeError, Message: "nope"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("got %+v want %+v", tt.got, tt.want)
			}
		})
	}
}

func TestFrameRoundTrip(t *testing.T) {
	in := messageFrame("general", "ab12", "hi", 1718600000000)
	data, err := in.encode()
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	if in != out {
		t.Fatalf("round trip mismatch: %+v != %+v", in, out)
	}
}

func TestEncodeOmitsEmpty(t *testing.T) {
	data, err := errorFrame("boom").encode()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["room"]; ok {
		t.Fatalf("expected room to be omitted, got %v", m)
	}
	if m["type"] != "error" || m["message"] != "boom" {
		t.Fatalf("unexpected payload: %v", m)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
go test ./internal/gateway/ -run TestFrame -v
```
Expected: FAIL — `undefined: Frame` / `undefined: messageFrame`.

- [ ] **Step 3: Implement `protocol.go`**

Create `internal/gateway/protocol.go`:
```go
package gateway

import "encoding/json"

// Frame is the single JSON envelope for every message on the wire.
// omitempty keeps each frame minimal; unused fields are simply absent.
type Frame struct {
	Type    string `json:"type"`
	Room    string `json:"room,omitempty"`
	Text    string `json:"text,omitempty"`
	From    string `json:"from,omitempty"`
	TS      int64  `json:"ts,omitempty"`
	Event   string `json:"event,omitempty"`
	Message string `json:"message,omitempty"`
}

const (
	TypeJoin    = "join"
	TypeLeave   = "leave"
	TypeSend    = "send"
	TypeMessage = "message"
	TypeSystem  = "system"
	TypeError   = "error"
)

func messageFrame(room, from, text string, ts int64) Frame {
	return Frame{Type: TypeMessage, Room: room, From: from, Text: text, TS: ts}
}

func systemFrame(room, event, from string) Frame {
	return Frame{Type: TypeSystem, Room: room, Event: event, From: from}
}

func errorFrame(msg string) Frame {
	return Frame{Type: TypeError, Message: msg}
}

func (f Frame) encode() ([]byte, error) {
	return json.Marshal(f)
}

func decodeFrame(data []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(data, &f)
	return f, err
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
go test ./internal/gateway/ -run TestFrame -v
```
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/protocol.go internal/gateway/protocol_test.go
git commit -m "Add JSON wire protocol frames"
```

---

### Task 3: Room (`room.go`)

**Files:**
- Create: `internal/gateway/room.go`
- Test: `internal/gateway/room_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/gateway/room_test.go`:
```go
package gateway

import (
	"sync"
	"testing"
	"time"
)

// fakeMember is a test double for a room participant.
type fakeMember struct {
	id       string
	mu       sync.Mutex
	received []Frame
	closed   string
}

func (f *fakeMember) ID() string { return f.id }

func (f *fakeMember) enqueue(fr Frame) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, fr)
}

func (f *fakeMember) close(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = reason
}

func (f *fakeMember) frames() []Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Frame, len(f.received))
	copy(out, f.received)
	return out
}

func TestRoomFanout(t *testing.T) {
	r := newRoom("general")
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	r.members["a"] = a
	r.members["b"] = b

	r.fanout(messageFrame("general", "a", "hi", 1))

	for _, m := range []*fakeMember{a, b} {
		got := m.frames()
		if len(got) != 1 || got[0].Text != "hi" {
			t.Fatalf("member %s got %+v", m.id, got)
		}
	}
}

func TestRoomRunJoinLeave(t *testing.T) {
	r := newRoom("general")
	go r.run()
	defer close(r.done)

	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}

	r.join <- a
	r.join <- b

	// Broadcast and let it propagate.
	r.broadcast <- messageFrame("general", "a", "hi", 1)

	waitFor(t, func() bool { return hasText(b.frames(), "hi") })

	// b leaves; room is not empty (a remains).
	reply := make(chan bool, 1)
	r.leave <- leaveReq{m: b, empty: reply}
	if empty := <-reply; empty {
		t.Fatal("room should not be empty while a remains")
	}

	// a leaves; room is now empty.
	r.leave <- leaveReq{m: a, empty: reply}
	if empty := <-reply; !empty {
		t.Fatal("room should be empty after last member leaves")
	}
}

func hasText(frames []Frame, text string) bool {
	for _, f := range frames {
		if f.Text == text {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
go test ./internal/gateway/ -run TestRoom -v
```
Expected: FAIL — `undefined: newRoom` / `undefined: leaveReq`.

- [ ] **Step 3: Implement `room.go`**

Create `internal/gateway/room.go`:
```go
package gateway

// member is anything that can participate in a room. *Client implements it;
// tests use a fake. Methods are unexported because only this package drives them.
type member interface {
	ID() string
	enqueue(Frame)
	close(reason string)
}

// leaveReq carries a leave plus a reply channel reporting whether the room is
// now empty, so the hub can decide to reap the room.
type leaveReq struct {
	m     member
	empty chan bool
}

// Room owns its member set inside a single goroutine (run). All mutation flows
// through channels, so there are no locks on the fan-out path.
type Room struct {
	id        string
	join      chan member
	leave     chan leaveReq
	broadcast chan Frame
	done      chan struct{}
	members   map[string]member
}

func newRoom(id string) *Room {
	return &Room{
		id:        id,
		join:      make(chan member),
		leave:     make(chan leaveReq),
		broadcast: make(chan Frame),
		done:      make(chan struct{}),
		members:   make(map[string]member),
	}
}

func (r *Room) run() {
	for {
		select {
		case m := <-r.join:
			r.members[m.ID()] = m
			r.fanout(systemFrame(r.id, "join", m.ID()))
		case req := <-r.leave:
			if _, ok := r.members[req.m.ID()]; ok {
				delete(r.members, req.m.ID())
				r.fanout(systemFrame(r.id, "leave", req.m.ID()))
			}
			req.empty <- (len(r.members) == 0)
		case f := <-r.broadcast:
			r.fanout(f)
		case <-r.done:
			return
		}
	}
}

func (r *Room) fanout(f Frame) {
	for _, m := range r.members {
		m.enqueue(f)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
go test ./internal/gateway/ -run TestRoom -race -v
```
Expected: PASS (2 tests), no race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/room.go internal/gateway/room_test.go
git commit -m "Add per-room goroutine with channel-driven fan-out"
```

---

### Task 4: Hub (`hub.go`)

**Files:**
- Create: `internal/gateway/hub.go`
- Test: `internal/gateway/hub_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/gateway/hub_test.go`:
```go
package gateway

import "testing"

func TestHubLazyCreateAndReap(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}

	h.Join("general", a)
	if h.roomCount() != 1 {
		t.Fatalf("expected 1 room, got %d", h.roomCount())
	}

	h.Leave("general", a)
	if h.roomCount() != 0 {
		t.Fatalf("expected room reaped, got %d", h.roomCount())
	}
}

func TestHubBroadcastReachesMembers(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Join("general", a)
	h.Join("general", b)

	h.Broadcast("general", messageFrame("general", "a", "hi", 1))

	waitFor(t, func() bool { return hasText(a.frames(), "hi") && hasText(b.frames(), "hi") })
}

func TestHubLeaveUnknownRoomIsNoop(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}
	h.Leave("ghost", a) // must not panic
	if h.roomCount() != 0 {
		t.Fatalf("expected 0 rooms, got %d", h.roomCount())
	}
}

func TestHubCloseAllClosesRegisteredClients(t *testing.T) {
	h := NewHub()
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

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
go test ./internal/gateway/ -run TestHub -v
```
Expected: FAIL — `undefined: NewHub` / `h.roomCount undefined`.

- [ ] **Step 3: Implement `hub.go`**

Create `internal/gateway/hub.go`:
```go
package gateway

import "sync"

// Hub is a registry of rooms and connected clients. The mutex guards the maps
// only; it is NOT held during fan-out (that lives in each Room's goroutine).
type Hub struct {
	mu      sync.Mutex
	rooms   map[string]*Room
	clients map[string]member
}

func NewHub() *Hub {
	return &Hub{
		rooms:   make(map[string]*Room),
		clients: make(map[string]member),
	}
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

// Join adds a member to a room, creating the room (and its goroutine) on first
// use. The lock is held across the channel send so Join and Leave fully
// serialize, preventing a reap from racing an in-flight join.
func (h *Hub) Join(roomID string, m member) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[roomID]
	if !ok {
		r = newRoom(roomID)
		h.rooms[roomID] = r
		go r.run()
	}
	r.join <- m
}

// Leave removes a member and reaps the room if it became empty. The reply from
// the room arrives while the lock is held, so the reap decision is atomic with
// respect to Join.
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
	}
}

// Broadcast delivers a frame to a room without holding the lock during the
// send. If the room was reaped concurrently, the select on r.done abandons the
// send instead of blocking forever.
func (h *Hub) Broadcast(roomID string, f Frame) {
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

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
go test ./internal/gateway/ -run TestHub -race -v
```
Expected: PASS (4 tests), no race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/hub.go internal/gateway/hub_test.go
git commit -m "Add hub registry with room reaping and broadcast"
```

---

### Task 5: Client (`client.go`)

**Files:**
- Create: `internal/gateway/client.go`
- Test: `internal/gateway/client_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/gateway/client_test.go`:
```go
package gateway

import (
	"testing"
)

// fakeRegistry records hub calls so handleFrame can be tested in isolation.
type fakeRegistry struct {
	joined    []string
	left      []string
	broadcast []Frame
}

func (f *fakeRegistry) Join(roomID string, m member)  { f.joined = append(f.joined, roomID) }
func (f *fakeRegistry) Leave(roomID string, m member) { f.left = append(f.left, roomID) }
func (f *fakeRegistry) Broadcast(roomID string, fr Frame) {
	f.broadcast = append(f.broadcast, fr)
}

func drain(c *Client) []Frame {
	var out []Frame
	for {
		select {
		case f := <-c.send:
			out = append(out, f)
		default:
			return out
		}
	}
}

func TestNewIDFormat(t *testing.T) {
	id := newID()
	if len(id) != 8 {
		t.Fatalf("expected 8 hex chars, got %q", id)
	}
}

func TestHandleSendRequiresJoin(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})

	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})

	if len(reg.broadcast) != 0 {
		t.Fatalf("send before join should not broadcast, got %+v", reg.broadcast)
	}
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected one error frame, got %+v", out)
	}
}

func TestHandleJoinThenSend(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "general"})
	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})

	if len(reg.joined) != 1 || reg.joined[0] != "general" {
		t.Fatalf("expected join general, got %+v", reg.joined)
	}
	if len(reg.broadcast) != 1 {
		t.Fatalf("expected one broadcast, got %+v", reg.broadcast)
	}
	got := reg.broadcast[0]
	if got.Type != TypeMessage || got.From != c.ID() || got.Text != "hi" || got.TS == 0 {
		t.Fatalf("unexpected broadcast frame: %+v", got)
	}
}

func TestHandleUnknownType(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})
	c.handleFrame(Frame{Type: "bogus"})
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected error frame for unknown type, got %+v", out)
	}
}

func TestEnqueueOverflowClosesClient(t *testing.T) {
	closed := false
	reg := &fakeRegistry{}
	c := newClient(reg, func() { closed = true })

	// Fill the buffer (cap 16) without draining, then overflow.
	for i := 0; i < cap(c.send)+1; i++ {
		c.enqueue(Frame{Type: TypeMessage})
	}
	if !closed {
		t.Fatal("expected overflow to trigger close/cancel")
	}
}

func TestLeaveAllLeavesEveryJoinedRoom(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})
	c.handleFrame(Frame{Type: TypeJoin, Room: "a"})
	c.handleFrame(Frame{Type: TypeJoin, Room: "b"})

	c.leaveAll()

	if len(reg.left) != 2 {
		t.Fatalf("expected to leave 2 rooms, got %+v", reg.left)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
go test ./internal/gateway/ -run "TestNewID|TestHandle|TestEnqueue|TestLeaveAll" -v
```
Expected: FAIL — `undefined: newClient`.

- [ ] **Step 3: Implement `client.go`**

Create `internal/gateway/client.go`:
```go
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// roomRegistry is the subset of the hub a client depends on, so handleFrame can
// be tested with a fake.
type roomRegistry interface {
	Join(roomID string, m member)
	Leave(roomID string, m member)
	Broadcast(roomID string, f Frame)
}

// Client is one WebSocket connection. enqueue feeds the bounded send channel
// that writePump drains; overflow drops the client by cancelling its context.
type Client struct {
	id     string
	hub    roomRegistry
	send   chan Frame
	cancel context.CancelFunc
	once   sync.Once

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

func newClient(hub roomRegistry, cancel context.CancelFunc) *Client {
	return &Client{
		id:     newID(),
		hub:    hub,
		send:   make(chan Frame, 16),
		cancel: cancel,
		joined: make(map[string]bool),
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

func (c *Client) close(string) {
	c.once.Do(c.cancel)
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
		}
	case TypeLeave:
		c.mu.Lock()
		was := c.joined[f.Room]
		delete(c.joined, f.Room)
		c.mu.Unlock()
		if was {
			c.hub.Leave(f.Room, c)
		}
	case TypeSend:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if !joined {
			c.enqueue(errorFrame("not joined to room"))
			return
		}
		c.hub.Broadcast(f.Room, messageFrame(f.Room, c.id, f.Text, nowMillis()))
	default:
		c.enqueue(errorFrame("unknown frame type"))
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

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
go test ./internal/gateway/ -run "TestNewID|TestHandle|TestEnqueue|TestLeaveAll" -race -v
```
Expected: PASS (6 tests), no race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/client.go internal/gateway/client_test.go
git commit -m "Add client connection: dispatch, bounded send, read/write pumps"
```

---

### Task 6: Server (`server.go`)

**Files:**
- Create: `internal/gateway/server.go`
- Test: `internal/gateway/server_test.go`

- [ ] **Step 1: Write the failing test (health + readyz)**

Create `internal/gateway/server_test.go`:
```go
package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestServer() *Server {
	return NewServer(NewHub(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}

func TestHealthzAlwaysOK(t *testing.T) {
	srv := newTestServer()
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
}

func TestReadyzFlipsOnDraining(t *testing.T) {
	srv := newTestServer()

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz before draining = %d, want 200", rec.Code)
	}

	srv.SetDraining(true)
	rec = httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz while draining = %d, want 503", rec.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
go test ./internal/gateway/ -run "TestHealthz|TestReadyz" -v
```
Expected: FAIL — `undefined: NewServer`.

- [ ] **Step 3: Implement `server.go`**

Create `internal/gateway/server.go`:
```go
package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/go-chi/chi/v5"
	"nhooyr.io/websocket"
)

type Server struct {
	hub      *Hub
	log      *slog.Logger
	webDir   string
	draining atomic.Bool
}

func NewServer(hub *Hub, log *slog.Logger, webDir string) *Server {
	return &Server{hub: hub, log: log, webDir: webDir}
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

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
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
		_ = conn.Close(websocket.StatusGoingAway, "bye")
		s.log.Info("client disconnected", "id", client.ID())
	}()

	go client.writePump(ctx, conn)
	client.readPump(ctx, conn)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
go test ./internal/gateway/ -run "TestHealthz|TestReadyz" -v
```
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/server.go internal/gateway/server_test.go
git commit -m "Add HTTP server: router, ws handler, health and readiness"
```

---

### Task 7: Integration test (real WebSocket end-to-end)

**Files:**
- Modify: `internal/gateway/server_test.go` (append)

- [ ] **Step 1: Write the failing test**

First, extend the existing `import (...)` block in `internal/gateway/server_test.go` so it also includes these (alongside the `io`, `log/slog`, `net/http`, `net/http/httptest`, `testing` already there):
```go
	"context"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
```

Then append this test to the file:
```go
func dialWS(t *testing.T, url string) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, ctx
}

func readUntil(t *testing.T, ctx context.Context, conn *websocket.Conn, typ string) Frame {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		var f Frame
		if err := wsjson.Read(rctx, conn, &f); err != nil {
			t.Fatalf("read (waiting for %q): %v", typ, err)
		}
		if f.Type == typ {
			return f
		}
	}
}

func TestEndToEndFanout(t *testing.T) {
	srv := NewServer(NewHub(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()
	wsURL := "ws" + ts.URL[len("http"):] + "/ws"

	a, actx := dialWS(t, wsURL)
	defer a.Close(websocket.StatusNormalClosure, "")
	b, bctx := dialWS(t, wsURL)
	defer b.Close(websocket.StatusNormalClosure, "")

	if err := wsjson.Write(actx, a, Frame{Type: TypeJoin, Room: "general"}); err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Write(bctx, b, Frame{Type: TypeJoin, Room: "general"}); err != nil {
		t.Fatal(err)
	}

	// Wait until B's join is processed (B receives its own system "join"
	// frame) so A's broadcast cannot race ahead of B becoming a member.
	readUntil(t, bctx, b, TypeSystem)

	if err := wsjson.Write(actx, a, Frame{Type: TypeSend, Room: "general", Text: "hello"}); err != nil {
		t.Fatal(err)
	}

	msg := readUntil(t, bctx, b, TypeMessage)
	if msg.Text != "hello" || msg.Room != "general" || msg.From == "" {
		t.Fatalf("unexpected message frame: %+v", msg)
	}
}
```

- [ ] **Step 2: Run test to verify it fails (then passes)**

Run:
```bash
go test ./internal/gateway/ -run TestEndToEndFanout -race -v
```
Expected: PASS (the production code already exists). If it fails, debug before continuing.

- [ ] **Step 3: Run the whole package**

Run:
```bash
go test ./... -race
```
Expected: `ok` for `internal/gateway`, no race warnings.

- [ ] **Step 4: Commit**

```bash
git add internal/gateway/server_test.go
git commit -m "Add end-to-end WebSocket fan-out test"
```

---

### Task 8: Main entrypoint + graceful shutdown (`main.go`)

**Files:**
- Modify: `cmd/gateway/main.go` (replace the stub)

- [ ] **Step 1: Replace the stub with the real main**

Replace `cmd/gateway/main.go` with:
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

	hub := gateway.NewHub()
	srv := gateway.NewServer(hub, log, webDir)
	httpServer := &http.Server{Addr: addr, Handler: srv.Router()}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("gateway listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown initiated")

	// Stop routing new traffic, then drain.
	srv.SetDraining(true)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hub.CloseAll("server shutting down")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	log.Info("shutdown complete")
}
```

- [ ] **Step 2: Build and vet**

Run:
```bash
go build ./... && go vet ./...
```
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add cmd/gateway/main.go
git commit -m "Wire main entrypoint with graceful shutdown"
```

---

### Task 9: Demo client (`web/index.html`)

**Files:**
- Create: `web/index.html`

- [ ] **Step 1: Create the demo page**

Create `web/index.html`:
```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Chat — Slice 1 demo</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
    #log { border: 1px solid #ccc; height: 320px; overflow-y: auto; padding: .5rem; margin: .5rem 0; }
    .row { display: flex; gap: .5rem; margin: .5rem 0; }
    .row input { flex: 1; padding: .4rem; }
    .sys { color: #888; font-style: italic; }
    .err { color: #c00; }
  </style>
</head>
<body>
  <h1>Chat demo</h1>
  <div class="row">
    <input id="room" value="general" placeholder="room" />
    <button id="join">Join</button>
    <span id="status">disconnected</span>
  </div>
  <div id="log"></div>
  <div class="row">
    <input id="text" placeholder="message" />
    <button id="send">Send</button>
  </div>

  <script>
    const log = document.getElementById("log");
    const statusEl = document.getElementById("status");
    let ws = null;
    let room = null;

    function add(text, cls) {
      const div = document.createElement("div");
      if (cls) div.className = cls;
      div.textContent = text;
      log.appendChild(div);
      log.scrollTop = log.scrollHeight;
    }

    function connect() {
      ws = new WebSocket(`ws://${location.host}/ws`);
      ws.onopen = () => { statusEl.textContent = "connected"; };
      ws.onclose = () => { statusEl.textContent = "disconnected"; };
      ws.onmessage = (ev) => {
        const f = JSON.parse(ev.data);
        if (f.type === "message") add(`${f.from}: ${f.text}`);
        else if (f.type === "system") add(`* ${f.from} ${f.event}ed ${f.room}`, "sys");
        else if (f.type === "error") add(`! ${f.message}`, "err");
      };
    }

    document.getElementById("join").onclick = () => {
      if (!ws || ws.readyState !== WebSocket.OPEN) connect();
      const tryJoin = () => {
        room = document.getElementById("room").value.trim();
        ws.send(JSON.stringify({ type: "join", room }));
        add(`joining ${room}…`, "sys");
      };
      if (ws.readyState === WebSocket.OPEN) tryJoin();
      else ws.addEventListener("open", tryJoin, { once: true });
    };

    document.getElementById("send").onclick = () => {
      const text = document.getElementById("text").value;
      if (!room || !ws || ws.readyState !== WebSocket.OPEN || !text) return;
      ws.send(JSON.stringify({ type: "send", room, text }));
      document.getElementById("text").value = "";
    };
  </script>
</body>
</html>
```

- [ ] **Step 2: Manual verification**

Run:
```bash
go run ./cmd/gateway
```
Then open `http://localhost:8080/` in two browser tabs. In both, click **Join** (room `general`). Type a message in one tab and click **Send**; it appears in both tabs. Each tab also shows a system line when the other joins. Press Ctrl+C in the terminal and confirm the process exits and both tabs show `disconnected`.

- [ ] **Step 3: Commit**

```bash
git add web/index.html
git commit -m "Add vanilla-JS demo client"
```

---

### Task 10: README + final verification

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write the README**

Create `README.md`:
```markdown
# Real-time Chat Service

Horizontally scalable real-time chat, built in slices. This is **slice 1**: a
single in-memory WebSocket gateway.

## Run

```bash
go run ./cmd/gateway
```

Open http://localhost:8080/ in two browser tabs, join the same room, and chat.

Environment variables:

- `ADDR` — listen address (default `:8080`)
- `WEB_DIR` — directory served at `/` (default `web`)

## Endpoints

- `GET /ws` — WebSocket. JSON frames: `{"type":"join","room":"general"}`,
  `{"type":"send","room":"general","text":"hi"}`, `{"type":"leave","room":"general"}`.
  Server sends `message`, `system`, and `error` frames.
- `GET /healthz` — liveness (always 200).
- `GET /readyz` — readiness (503 while shutting down).

## Test

```bash
go test ./... -race
```

## Roadmap

Slice 1 (this) → Redis fan-out → Postgres history → presence/typing →
rate limiting/auth → observability → K8s + load test.
```

- [ ] **Step 2: Full verification suite**

Run:
```bash
go build ./... && go vet ./... && go test ./... -race
```
Expected: builds clean, vet clean, all tests `ok`, no race warnings.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "Add README with run, test, and roadmap"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `/ws` + protocol (Tasks 2,5,6), in-memory multi-room hub (Tasks 3,4), fan-out (Tasks 3,4,7), `/healthz`+`/readyz` (Task 6), graceful shutdown (Task 8), slog (Tasks 6,8), table-driven tests (Tasks 2,3,4,5), demo client (Task 9). All spec acceptance criteria are exercised.
- **Reaping race:** `Join`/`Leave` serialize under the hub lock; `Broadcast` selects on `r.done` so a concurrent reap cannot deadlock a send. Run every package test with `-race`.
- **Interface naming:** room participant interface is `member` (ID/enqueue/close); the hub-facing interface used by `Client` is `roomRegistry` (Join/Leave/Broadcast). `*Client` implements `member`; `*Hub` implements `roomRegistry`. These names are used consistently across Tasks 3–6.
- **Slow-client drop:** `enqueue` is non-blocking; overflow calls `close` → `cancel()` (once), which ends both pumps and triggers `leaveAll` + conn close in `handleWS`.
