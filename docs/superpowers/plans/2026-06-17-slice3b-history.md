# Slice 3b — Message History (read path) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replay recent (or since-cursor) history to a joining client and serve a paginated history REST endpoint, backed by Postgres reads in the gateway.

**Architecture:** Add read methods to `persistence.PgxStore`. The gateway defines its own `history` interface + `StoredMessage` type; an adapter in `cmd/gateway/main.go` bridges to `PgxStore` (so `gateway` never imports `persistence`). On JOIN the client asynchronously replays history to itself (cursor via the existing `Frame.id`); a `GET /api/rooms/{room}/messages` route serves paginated history; `/readyz` also pings Postgres.

**Tech Stack:** Go 1.22+, `github.com/jackc/pgx/v5` (+ pgxpool), existing chi + go-redis; tests use the existing testcontainers + miniredis setup.

**Commit convention:** Commit locally on `main`. Do NOT push. Do NOT add Claude/Anthropic attribution. Use `git -c commit.gpgsign=false commit`.

**-race needs a C compiler on this machine.** Prefix race-test Bash commands with:
```
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
```

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `internal/persistence/store_pg.go` | modify | `RecentMessages`, `MessagesSince`, `MessagesBefore`, `Ping` |
| `internal/persistence/store_pg_test.go` | modify | read-method tests (testcontainers, skip w/o Docker) |
| `internal/gateway/history.go` | **new** | `StoredMessage`, `history` interface |
| `internal/gateway/history_fake_test.go` | **new** | `fakeHistory` test double |
| `internal/gateway/client.go` | modify | `newClient` gains ctx/history/log; replay on join |
| `internal/gateway/client_test.go` | modify | new `newClient` calls; replay tests |
| `internal/gateway/server.go` | modify | `NewServer` gains history; REST route; readyz pings PG |
| `internal/gateway/server_test.go` | modify | new constructor; PG-down readyz; REST tests |
| `cmd/gateway/main.go` | modify | pgx pool, startup ping, history adapter |
| `web/index.html` | modify | dedup rendered messages by id (for clean replay) |
| `README.md` | modify | history endpoint + roadmap |

---

### Task 1: Postgres read methods

**Files:**
- Modify: `internal/persistence/store_pg.go`
- Test: `internal/persistence/store_pg_test.go`

- [ ] **Step 1: Append read-method tests (testcontainers, skip without Docker)**

Append to `internal/persistence/store_pg_test.go`:
```go
func seedMessages(t *testing.T, store *PgxStore) {
	t.Helper()
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	msgs := make([]Message, 0, 5)
	for i := int64(1); i <= 5; i++ {
		msgs = append(msgs, Message{RoomID: "x", ID: i, Sender: "u", Body: "m", CreatedMS: i})
	}
	if err := store.InsertBatch(ctx, msgs); err != nil {
		t.Fatal(err)
	}
}

func ids(msgs []Message) []int64 {
	out := make([]int64, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

func TestRecentMessagesReturnsNewestAscending(t *testing.T) {
	store := NewPgxStore(startPostgres(t))
	seedMessages(t, store)
	got, err := store.RecentMessages(context.Background(), "x", 3)
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 3 || g[0] != 3 || g[1] != 4 || g[2] != 5 {
		t.Fatalf("expected newest-3 ascending [3 4 5], got %v", g)
	}
}

func TestMessagesSinceFiltersAndOrders(t *testing.T) {
	store := NewPgxStore(startPostgres(t))
	seedMessages(t, store)
	got, err := store.MessagesSince(context.Background(), "x", 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 3 || g[0] != 3 || g[2] != 5 {
		t.Fatalf("expected ids >2 ascending [3 4 5], got %v", g)
	}
}

func TestMessagesBeforeReturnsOlderAscending(t *testing.T) {
	store := NewPgxStore(startPostgres(t))
	seedMessages(t, store)
	got, err := store.MessagesBefore(context.Background(), "x", 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	// id < 4 → {1,2,3}; newest 2 → {2,3}; ascending.
	if g := ids(got); len(g) != 2 || g[0] != 2 || g[1] != 3 {
		t.Fatalf("expected [2 3], got %v", g)
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/persistence/ -run "TestRecentMessages|TestMessagesSince|TestMessagesBefore" 2>&1 | head -20
```
Expected: FAIL to compile — `store.RecentMessages undefined` (etc.).

- [ ] **Step 3: Implement the read methods**

Append to `internal/persistence/store_pg.go` (the `pgx` import is already present):
```go
func (s *PgxStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const selectColumns = `SELECT room_id, id, sender, body, created_ms FROM messages`

func (s *PgxStore) RecentMessages(ctx context.Context, room string, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		selectColumns+` WHERE room_id=$1 ORDER BY id DESC LIMIT $2`, room, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	reverse(msgs)
	return msgs, nil
}

func (s *PgxStore) MessagesSince(ctx context.Context, room string, sinceID int64, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		selectColumns+` WHERE room_id=$1 AND id > $2 ORDER BY id ASC LIMIT $3`, room, sinceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *PgxStore) MessagesBefore(ctx context.Context, room string, beforeID int64, limit int) ([]Message, error) {
	rows, err := s.pool.Query(ctx,
		selectColumns+` WHERE room_id=$1 AND id < $2 ORDER BY id DESC LIMIT $3`, room, beforeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	reverse(msgs)
	return msgs, nil
}

func scanMessages(rows pgx.Rows) ([]Message, error) {
	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.RoomID, &m.ID, &m.Sender, &m.Body, &m.CreatedMS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func reverse(msgs []Message) {
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
}
```

- [ ] **Step 4: Verify build/vet and that the suite is green (PG tests skip)**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/persistence/ -race -v
```
Expected: build+vet clean; the new read tests report `--- SKIP` (Docker down), everything else PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/persistence/store_pg.go internal/persistence/store_pg_test.go
git -c commit.gpgsign=false commit -m "Add Postgres read methods for history"
```

---

### Task 2: Gateway history (replay + REST)

This changes `newClient` and `NewServer` signatures, rippling to `main.go` and the gateway tests. It all lands in **one commit**. Write tests first for the red state, then the production code.

**Files:**
- Create: `internal/gateway/history.go`, `internal/gateway/history_fake_test.go`
- Modify: `internal/gateway/client.go`, `internal/gateway/server.go`, `cmd/gateway/main.go`
- Modify (tests): `internal/gateway/client_test.go`, `internal/gateway/server_test.go`

- [ ] **Step 1: Create `history.go`**

Create `internal/gateway/history.go`:
```go
package gateway

import "context"

// StoredMessage is a persisted message as the gateway consumes it for replay
// and history responses.
type StoredMessage struct {
	ID   int64  `json:"id"`
	From string `json:"from"`
	Text string `json:"text"`
	TS   int64  `json:"ts"`
}

// history is the read store the gateway depends on. An adapter over the
// persistence layer satisfies it; tests use a fake.
type history interface {
	Recent(ctx context.Context, room string, limit int) ([]StoredMessage, error)
	Since(ctx context.Context, room string, sinceID int64, limit int) ([]StoredMessage, error)
	Before(ctx context.Context, room string, beforeID int64, limit int) ([]StoredMessage, error)
	Ping(ctx context.Context) error
}
```

- [ ] **Step 2: Create the fake history test double**

Create `internal/gateway/history_fake_test.go`:
```go
package gateway

import (
	"context"
	"sync"
)

type fakeHistory struct {
	mu        sync.Mutex
	recent    []StoredMessage
	since     []StoredMessage
	before    []StoredMessage
	err       error
	pingErr   error
	sinceArg  int64
	beforeArg int64
}

func (h *fakeHistory) Recent(_ context.Context, _ string, _ int) ([]StoredMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.recent, h.err
}

func (h *fakeHistory) Since(_ context.Context, _ string, sinceID int64, _ int) ([]StoredMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sinceArg = sinceID
	return h.since, h.err
}

func (h *fakeHistory) Before(_ context.Context, _ string, beforeID int64, _ int) ([]StoredMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeArg = beforeID
	return h.before, h.err
}

func (h *fakeHistory) Ping(_ context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pingErr
}

func (h *fakeHistory) sinceCalledWith() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sinceArg
}

func (h *fakeHistory) beforeCalledWith() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.beforeArg
}
```

- [ ] **Step 3: Update `client_test.go`**

Change the `client_test.go` import block from `import (\n\t"testing"\n)` to:
```go
import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)
```

Add a constructor helper and a frame-waiting helper (place after the `drain` function):
```go
func newTestClient(reg roomRegistry, hist history, cancel context.CancelFunc) *Client {
	return newClient(context.Background(), reg, hist, slog.New(slog.NewTextHandler(io.Discard, nil)), cancel)
}

func waitForFrames(t *testing.T, c *Client, n int) []Frame {
	t.Helper()
	var out []Frame
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		out = append(out, drain(c)...)
		if len(out) >= n {
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d frames, got %d", n, len(out))
	return nil
}
```

Replace every existing `newClient(reg, func() {})` call with `newTestClient(reg, &fakeHistory{}, func() {})`, and the one in `TestEnqueueOverflowClosesClient` (`newClient(reg, func() { closed = true })`) with `newTestClient(reg, &fakeHistory{}, func() { closed = true })`. (Affected tests: `TestHandleSendRequiresJoin`, `TestHandleJoinThenSend`, `TestHandleUnknownType`, `TestEnqueueOverflowClosesClient`, `TestLeaveAllLeavesEveryJoinedRoom`, `TestHandleLeaveUnjoinedIsNoop`, `TestHandleSendPublishErrorReturnsErrorFrame`.)

Append the replay tests:
```go
func TestJoinReplaysRecentWithoutCursor(t *testing.T) {
	reg := &fakeRegistry{}
	hist := &fakeHistory{recent: []StoredMessage{
		{ID: 1, From: "u", Text: "a", TS: 1},
		{ID: 2, From: "u", Text: "b", TS: 2},
	}}
	c := newTestClient(reg, hist, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	got := waitForFrames(t, c, 2)
	if got[0].Type != TypeMessage || got[0].ID != 1 || got[1].ID != 2 {
		t.Fatalf("unexpected replay frames: %+v", got)
	}
}

func TestJoinReplaysSinceWithCursor(t *testing.T) {
	reg := &fakeRegistry{}
	hist := &fakeHistory{since: []StoredMessage{{ID: 43, From: "u", Text: "c", TS: 3}}}
	c := newTestClient(reg, hist, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x", ID: 42})

	got := waitForFrames(t, c, 1)
	if got[0].ID != 43 {
		t.Fatalf("expected replayed id 43, got %+v", got)
	}
	if hist.sinceCalledWith() != 42 {
		t.Fatalf("expected Since called with cursor 42, got %d", hist.sinceCalledWith())
	}
}

func TestJoinReplayErrorEnqueuesNothing(t *testing.T) {
	reg := &fakeRegistry{}
	hist := &fakeHistory{err: errors.New("db down")}
	c := newTestClient(reg, hist, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	time.Sleep(100 * time.Millisecond)
	if got := drain(c); len(got) != 0 {
		t.Fatalf("expected no frames on history error, got %+v", got)
	}
}
```

- [ ] **Step 4: Update `server_test.go`**

Add `"encoding/json"` to the `server_test.go` import block.

Replace `newTestServer` with:
```go
func newTestServer() *Server {
	bus := newFakeBus()
	hub := NewHub(bus)
	return NewServer(hub, bus, &fakeHistory{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}
```

In `TestReadyzFailsWhenRedisDown`, replace its construction line:
```go
	hub := NewHub(bus)
	srv := NewServer(hub, bus, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
```
with:
```go
	hub := NewHub(bus)
	srv := NewServer(hub, bus, &fakeHistory{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
```

Append these tests:
```go
func TestReadyzFailsWhenPostgresDown(t *testing.T) {
	bus := newFakeBus()
	hist := &fakeHistory{pingErr: errors.New("pg down")}
	srv := NewServer(NewHub(bus), bus, hist, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with postgres down = %d, want 503", rec.Code)
	}
}

func TestHistoryEndpointReturnsMessages(t *testing.T) {
	bus := newFakeBus()
	hist := &fakeHistory{recent: []StoredMessage{
		{ID: 1, From: "u", Text: "a", TS: 1},
		{ID: 2, From: "u", Text: "b", TS: 2},
	}}
	srv := NewServer(NewHub(bus), bus, hist, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/x/messages", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("history = %d, want 200", rec.Code)
	}
	var resp struct {
		Messages []StoredMessage `json:"messages"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 2 || resp.Messages[0].ID != 1 || resp.Messages[1].ID != 2 {
		t.Fatalf("unexpected messages: %+v", resp.Messages)
	}
}

func TestHistoryEndpointBeforeParam(t *testing.T) {
	bus := newFakeBus()
	hist := &fakeHistory{before: []StoredMessage{{ID: 5, From: "u", Text: "e", TS: 5}}}
	srv := NewServer(NewHub(bus), bus, hist, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/x/messages?before=10", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("history?before = %d, want 200", rec.Code)
	}
	if hist.beforeCalledWith() != 10 {
		t.Fatalf("expected Before called with 10, got %d", hist.beforeCalledWith())
	}
}

func TestHistoryEndpointStoreErrorReturns503(t *testing.T) {
	bus := newFakeBus()
	hist := &fakeHistory{err: errors.New("down")}
	srv := NewServer(NewHub(bus), bus, hist, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/x/messages", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("history with store error = %d, want 503", rec.Code)
	}
}
```

- [ ] **Step 5: Run to verify red (compile failure)**

Run:
```bash
go test ./internal/gateway/ -run TestJoinReplays 2>&1 | head -20
```
Expected: FAIL to compile — `not enough arguments in call to newClient` / `NewServer`.

- [ ] **Step 6: Replace `client.go`**

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
}

// Client is one WebSocket connection. enqueue feeds the bounded send channel
// that writePump drains; overflow drops the client by cancelling its context.
type Client struct {
	id          string
	ctx         context.Context
	hub         roomRegistry
	history     history
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

func newClient(ctx context.Context, hub roomRegistry, hist history, log *slog.Logger, cancel context.CancelFunc) *Client {
	return &Client{
		id:      newID(),
		ctx:     ctx,
		hub:     hub,
		history: hist,
		log:     log,
		send:    make(chan Frame, 16),
		cancel:  cancel,
		joined:  make(map[string]bool),
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
			c.enqueue(errorFrame(fmt.Sprintf("not joined to room %q", f.Room)))
			return
		}
		if err := c.hub.Publish(f.Room, messageFrame(f.Room, c.id, f.Text, nowMillis())); err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
	default:
		c.enqueue(errorFrame("unknown frame type"))
	}
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

- [ ] **Step 7: Replace `server.go`**

Replace the entire contents of `internal/gateway/server.go` with:
```go
package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
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

type Server struct {
	hub      *Hub
	bus      pinger
	hist     history
	log      *slog.Logger
	webDir   string
	draining atomic.Bool
}

func NewServer(hub *Hub, bus pinger, hist history, log *slog.Logger, webDir string) *Server {
	return &Server{hub: hub, bus: bus, hist: hist, log: log, webDir: webDir}
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
	// InsecureSkipVerify allows the local demo page to connect during dev.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		s.log.Warn("ws accept failed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	client := newClient(ctx, s.hub, s.hist, s.log, cancel)
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

- [ ] **Step 8: Replace `cmd/gateway/main.go`**

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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/gateway"
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/persistence"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// histAdapter bridges persistence.PgxStore to gateway.history, mapping
// persistence.Message to gateway.StoredMessage.
type histAdapter struct {
	store *persistence.PgxStore
}

func toStored(msgs []persistence.Message) []gateway.StoredMessage {
	out := make([]gateway.StoredMessage, len(msgs))
	for i, m := range msgs {
		out[i] = gateway.StoredMessage{ID: m.ID, From: m.Sender, Text: m.Body, TS: m.CreatedMS}
	}
	return out
}

func (h histAdapter) Recent(ctx context.Context, room string, limit int) ([]gateway.StoredMessage, error) {
	msgs, err := h.store.RecentMessages(ctx, room, limit)
	return toStored(msgs), err
}

func (h histAdapter) Since(ctx context.Context, room string, sinceID int64, limit int) ([]gateway.StoredMessage, error) {
	msgs, err := h.store.MessagesSince(ctx, room, sinceID, limit)
	return toStored(msgs), err
}

func (h histAdapter) Before(ctx context.Context, room string, beforeID int64, limit int) ([]gateway.StoredMessage, error) {
	msgs, err := h.store.MessagesBefore(ctx, room, beforeID, limit)
	return toStored(msgs), err
}

func (h histAdapter) Ping(ctx context.Context) error { return h.store.Ping(ctx) }

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := getenv("ADDR", ":8080")
	webDir := getenv("WEB_DIR", "web")
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	dbURL := getenv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")

	bus := gateway.NewRedisBus(redisAddr)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := bus.Ping(pingCtx); err != nil {
		pingCancel()
		log.Error("cannot reach redis", "addr", redisAddr, "err", err)
		os.Exit(1)
	}
	pingCancel()

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Error("cannot create pg pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	pgCtx, pgCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Ping(pgCtx); err != nil {
		pgCancel()
		log.Error("cannot reach postgres", "err", err)
		os.Exit(1)
	}
	pgCancel()

	hist := histAdapter{store: persistence.NewPgxStore(pool)}

	hub := gateway.NewHub(bus)
	srv := gateway.NewServer(hub, bus, hist, log, webDir)
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

- [ ] **Step 9: Tidy, build, vet, race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go mod tidy
go build ./... && go vet ./...
go test ./... -race -count=2
```
Expected: build+vet clean; all tests `ok` (PG integration tests skip), no race warnings — including the new replay and REST tests.

- [ ] **Step 10: Commit**

```bash
git add internal/gateway/ cmd/gateway/main.go go.mod go.sum
git -c commit.gpgsign=false commit -m "Add history replay on join and history REST endpoint"
```

---

### Task 3: Demo dedup + README + final verification

**Files:**
- Modify: `web/index.html`, `README.md`

- [ ] **Step 1: Dedup rendered messages by id in the demo**

In `web/index.html`, the `ws.onmessage` handler renders every `message` frame. Replayed and live messages can overlap by `id`, so track seen ids. Find this exact line:
```js
        if (f.type === "message") add(`${f.from}: ${f.text}`);
```
and replace it with (note: NO trailing `else` — the existing `else if (f.type === "system")` line follows unchanged, giving a valid `if (...) {…} else if (...)` chain):
```js
        if (f.type === "message") {
          if (f.id && seenIds.has(f.id)) return;
          if (f.id) seenIds.add(f.id);
          add(`${f.from}: ${f.text}`);
        }
```
Then, just before the `let ws = null;` line, add:
```js
    const seenIds = new Set();
```
After the edit, verify the resulting JS is well-formed: `if (f.type === "message") {…} else if (f.type === "system") {…} else if (f.type === "error") {…}`.

- [ ] **Step 2: Update the README**

In `README.md`, under `## Endpoints`, add after the `GET /ws` bullet block (before `GET /healthz`):
```markdown
- `GET /api/rooms/{room}/messages?limit=&before=` — paginated history JSON
  (`{"messages":[{"id","from","text","ts"}]}`); `limit` default 100 (max 200),
  `before` returns messages with id < before. Joining a room also replays the
  last 100 messages (or messages after the `id` sent on the JOIN frame).
```

Update the `## Roadmap` line to:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history + replay (done) → presence/typing → rate limiting/auth → observability → K8s + load test.
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
git -c commit.gpgsign=false commit -m "Dedup demo messages by id and document history endpoint"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** read methods + Ping (Task 1), `StoredMessage`/`history` interface (Task 2 history.go), async replay on join with `Frame.id` cursor capped at 100 (Task 2 client.go), REST `GET /api/rooms/{room}/messages` with limit/before (Task 2 server.go), `/readyz` pings PG (Task 2 server.go), gateway pgx pool + adapter + startup ping (Task 2 main.go), demo dedup + docs (Task 3). Out-of-scope (room CRUD, presence, rate limiting, React) absent.
- **Signature consistency:** `history{Recent,Since,Before,Ping}`; `newClient(ctx, hub, hist, log, cancel)`; `NewServer(hub, bus, hist, log, webDir)`; `StoredMessage{ID,From,Text,TS}` with json tags; `PgxStore.RecentMessages/MessagesSince/MessagesBefore/Ping`; `histAdapter` maps `persistence.Message`→`gateway.StoredMessage` (Sender→From, Body→Text, CreatedMS→TS). `histAdapter` and `fakeHistory` both satisfy `gateway.history`.
- **Why one commit in Task 2:** `newClient`/`NewServer` signature changes ripple to `main.go` + tests; the package can't compile until they change together (Step 5 shows the red state).
- **Replay is async** (goroutine per join) and best-effort; the client dedups by id (demo updated). Unbounded join-replay goroutines are a known v1 exposure (rate limiting is slice 5).
