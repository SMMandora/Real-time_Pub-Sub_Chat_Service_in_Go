# Slice 3a — Message Persistence (write path) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Assign every chat message a per-room ordered ID at send time and persist messages to Postgres via a separate batching worker process.

**Architecture:** The gateway stamps each message with `id = INCR seq:{room}` in `Hub.Publish` (adding an `id` field to `Frame`), then publishes as in slice 2. A new `cmd/worker` process `PSUBSCRIBE room:*`, decodes message frames, batches them (100 messages or 50ms), and writes them idempotently to Postgres. Gateway and worker share only Redis + Postgres.

**Tech Stack:** Go 1.22+, `github.com/jackc/pgx/v5`, existing `github.com/redis/go-redis/v9`, `github.com/alicebob/miniredis/v2` + `github.com/testcontainers/testcontainers-go` (tests).

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
| `internal/gateway/protocol.go` | modify | add `ID` field to `Frame` |
| `internal/gateway/bus.go` | modify | `Bus.NextID`, `RedisBus.NextID` (INCR), `seqKey` |
| `internal/gateway/bus_fake_test.go` | modify | `fakeBus.NextID` + counter, `publishedFrames` helper |
| `internal/gateway/hub.go` | modify | `Publish` stamps `id` via `NextID` |
| `internal/gateway/bus_test.go` | modify | `NextID` test |
| `internal/gateway/hub_test.go` | modify | publish stamps increasing id test |
| `internal/persistence/message.go` | **new** | `Message`, `MessageStore` |
| `internal/persistence/batcher.go` | **new** | size/interval batching + retry-once |
| `internal/persistence/worker.go` | **new** | `PSUBSCRIBE room:*`, decode, forward |
| `internal/persistence/store_pg.go` | **new** | `PgxStore` (Migrate + idempotent InsertBatch) |
| `internal/persistence/helpers_test.go` | **new** | `fakeStore`, `waitUntil` |
| `internal/persistence/batcher_test.go` | **new** | batching tests |
| `internal/persistence/worker_test.go` | **new** | decode/filter + miniredis integration |
| `internal/persistence/store_pg_test.go` | **new** | testcontainers PG (skips w/o Docker) |
| `cmd/worker/main.go` | **new** | wiring + health + graceful flush |
| `docker-compose.yml` | modify | add `postgres:16` |
| `README.md` | modify | Postgres + worker docs |

---

### Task 1: Add dependencies

**Files:** `go.mod`, `go.sum`

- [ ] **Step 1: Add pgx and testcontainers**

Run:
```bash
go get github.com/jackc/pgx/v5@v5.7.1
go get github.com/testcontainers/testcontainers-go@v0.33.0
go get github.com/testcontainers/testcontainers-go/modules/postgres@v0.33.0
```
Expected: all added to `go.mod`. Patch/minor versions may resolve slightly differently; that is fine if the build and tests pass. Do NOT run `go mod tidy` yet (deps are unused until later tasks).

- [ ] **Step 2: Verify build**

Run:
```bash
go build ./...
```
Expected: clean, exit 0.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git -c commit.gpgsign=false commit -m "Add pgx and testcontainers dependencies"
```

---

### Task 2: Message IDs in the gateway

Adds `NextID` to the `Bus` interface and stamps a per-room id in `Hub.Publish`. The interface change means `fakeBus` and `RedisBus` must both gain `NextID`; everything lands in one commit (write tests first for the red state).

**Files:**
- Modify: `internal/gateway/protocol.go`, `internal/gateway/bus.go`, `internal/gateway/hub.go`
- Modify (tests): `internal/gateway/bus_fake_test.go`, `internal/gateway/bus_test.go`, `internal/gateway/hub_test.go`

- [ ] **Step 1: Add the `NextID` test to `bus_test.go`**

Append to `internal/gateway/bus_test.go`:
```go
func TestRedisBusNextIDIncrementsPerRoom(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	bus := NewRedisBus(mr.Addr())
	defer bus.Close()
	ctx := context.Background()

	id1, err := bus.NextID(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := bus.NextID(ctx, "x")
	idY, _ := bus.NextID(ctx, "y")

	if id1 != 1 || id2 != 2 {
		t.Fatalf("room x ids = %d,%d, want 1,2", id1, id2)
	}
	if idY != 1 {
		t.Fatalf("room y id = %d, want 1", idY)
	}
}
```

- [ ] **Step 2: Add the publish-stamps-id test to `hub_test.go`**

Add `"encoding/json"` to the `hub_test.go` import block (it currently imports only `"testing"`; make it `import (\n\t"encoding/json"\n\t"testing"\n)`).

Append:
```go
func TestHubPublishStampsIncreasingID(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("general", &fakeMember{id: "a"})

	_ = h.Publish("general", messageFrame("general", "a", "one", 1))
	_ = h.Publish("general", messageFrame("general", "a", "two", 1))

	payloads := bus.publishedFrames()
	if len(payloads) != 2 {
		t.Fatalf("expected 2 published frames, got %d", len(payloads))
	}
	var f1, f2 Frame
	_ = json.Unmarshal(payloads[0], &f1)
	_ = json.Unmarshal(payloads[1], &f2)
	if f1.ID != 1 || f2.ID != 2 {
		t.Fatalf("expected stamped ids 1,2, got %d,%d", f1.ID, f2.ID)
	}
}
```

- [ ] **Step 3: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run "TestRedisBusNextID|TestHubPublishStamps" 2>&1 | head -20
```
Expected: FAIL to compile — `bus.NextID undefined`, `bus.publishedFrames undefined`.

- [ ] **Step 4: Add `ID` to `Frame` in `protocol.go`**

Replace the `Frame` struct in `internal/gateway/protocol.go` with:
```go
type Frame struct {
	Type    string `json:"type"`
	Room    string `json:"room,omitempty"`
	ID      int64  `json:"id,omitempty"`
	Text    string `json:"text,omitempty"`
	From    string `json:"from,omitempty"`
	TS      int64  `json:"ts,omitempty"`
	Event   string `json:"event,omitempty"`
	Message string `json:"message,omitempty"`
}
```

- [ ] **Step 5: Add `NextID` to the bus**

In `internal/gateway/bus.go`, add to the `Bus` interface (after `Unsubscribe`):
```go
	NextID(ctx context.Context, room string) (int64, error)
```

Add a seq-key helper near `roomChannel` (after the `roomFromChannel` function):
```go
func seqKey(room string) string { return "seq:" + room }
```

Add the `RedisBus` method (after `Unsubscribe`):
```go
func (b *RedisBus) NextID(ctx context.Context, room string) (int64, error) {
	return b.rdb.Incr(ctx, seqKey(room)).Result()
}
```

- [ ] **Step 6: Add `NextID` + helpers to `fakeBus`**

In `internal/gateway/bus_fake_test.go`, add a `seq` field to the struct (after `subCount`):
```go
	seq map[string]int64
```
Change `newFakeBus` to initialize it:
```go
func newFakeBus() *fakeBus {
	return &fakeBus{subscribed: make(map[string]bool), seq: make(map[string]int64)}
}
```
Add these methods (anywhere in the file):
```go
func (b *fakeBus) NextID(_ context.Context, room string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq[room]++
	return b.seq[room], nil
}

func (b *fakeBus) publishedFrames() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([][]byte, len(b.published))
	for i, m := range b.published {
		out[i] = m.payload
	}
	return out
}
```

- [ ] **Step 7: Stamp the id in `Hub.Publish`**

In `internal/gateway/hub.go`, replace the `Publish` method body with:
```go
func (h *Hub) Publish(roomID string, f Frame) error {
	id, err := h.bus.NextID(context.Background(), roomID)
	if err != nil {
		return err
	}
	f.ID = id
	payload, err := f.encode()
	if err != nil {
		return err
	}
	return h.bus.Publish(context.Background(), roomChannel(roomID), payload)
}
```

- [ ] **Step 8: Run the gateway tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go test ./internal/gateway/ -race
```
Expected: `ok`, no race warnings (all existing tests plus the two new ones pass).

- [ ] **Step 9: Commit**

```bash
git add internal/gateway/
git -c commit.gpgsign=false commit -m "Stamp per-room message IDs via Redis INCR"
```

---

### Task 3: Message model + batcher

**Files:**
- Create: `internal/persistence/message.go`, `internal/persistence/batcher.go`
- Test: `internal/persistence/helpers_test.go`, `internal/persistence/batcher_test.go`

- [ ] **Step 1: Write the test helpers**

Create `internal/persistence/helpers_test.go`:
```go
package persistence

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStore records InsertBatch calls and can be told to fail the next N calls.
type fakeStore struct {
	mu       sync.Mutex
	batches  [][]Message
	calls    int
	failNext int
}

func (s *fakeStore) Migrate(context.Context) error { return nil }

func (s *fakeStore) InsertBatch(_ context.Context, msgs []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failNext > 0 {
		s.failNext--
		return errors.New("insert failed")
	}
	cp := make([]Message, len(msgs))
	copy(cp, msgs)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *fakeStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

func (s *fakeStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeStore) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
```

- [ ] **Step 2: Write `batcher_test.go`**

Create `internal/persistence/batcher_test.go`:
```go
package persistence

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestBatcherFlushesOnSize(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 3, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	for i := 0; i < 3; i++ {
		b.Submit(Message{RoomID: "x", ID: int64(i + 1), Body: "hi"})
	}
	waitUntil(t, time.Second, func() bool { return store.batchCount() == 1 && store.count() == 3 })
}

func TestBatcherFlushesOnInterval(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1000, 20*time.Millisecond, testLogger())
	go b.Run()
	defer b.Close()

	b.Submit(Message{RoomID: "x", ID: 1, Body: "a"})
	b.Submit(Message{RoomID: "x", ID: 2, Body: "b"})
	waitUntil(t, time.Second, func() bool { return store.count() == 2 })
}

func TestBatcherFlushesRemainderOnClose(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1000, time.Hour, testLogger())
	go b.Run()

	b.Submit(Message{RoomID: "x", ID: 1, Body: "a"})
	b.Submit(Message{RoomID: "x", ID: 2, Body: "b"})
	b.Close() // must flush the remainder

	if store.count() != 2 {
		t.Fatalf("expected 2 messages flushed on close, got %d", store.count())
	}
}

func TestBatcherRetriesOnceOnFailure(t *testing.T) {
	store := &fakeStore{failNext: 1}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	b.Submit(Message{RoomID: "x", ID: 1, Body: "a"})
	waitUntil(t, time.Second, func() bool { return store.callCount() == 2 && store.count() == 1 })
}
```

- [ ] **Step 3: Run to verify red**

Run:
```bash
go test ./internal/persistence/ 2>&1 | head -20
```
Expected: FAIL to compile — `undefined: Message`, `undefined: NewBatcher`.

- [ ] **Step 4: Implement `message.go`**

Create `internal/persistence/message.go`:
```go
package persistence

import "context"

// Message is one persisted chat message. ID is the per-room sequence assigned
// by the gateway.
type Message struct {
	RoomID    string
	ID        int64
	Sender    string
	Body      string
	CreatedMS int64
}

// MessageStore persists batches of messages.
type MessageStore interface {
	Migrate(ctx context.Context) error
	InsertBatch(ctx context.Context, msgs []Message) error
}
```

- [ ] **Step 5: Implement `batcher.go`**

Create `internal/persistence/batcher.go`:
```go
package persistence

import (
	"context"
	"log/slog"
	"time"
)

// Batcher accumulates messages and flushes them to a MessageStore when the
// batch reaches maxSize or the flush interval elapses, whichever comes first.
type Batcher struct {
	store    MessageStore
	maxSize  int
	interval time.Duration
	log      *slog.Logger
	in       chan Message
	done     chan struct{}
	closed   chan struct{}
}

func NewBatcher(store MessageStore, maxSize int, interval time.Duration, log *slog.Logger) *Batcher {
	return &Batcher{
		store:    store,
		maxSize:  maxSize,
		interval: interval,
		log:      log,
		in:       make(chan Message, maxSize*2),
		done:     make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

// Submit enqueues a message for batched persistence. Safe to call until Close.
func (b *Batcher) Submit(m Message) { b.in <- m }

// Run flushes batches until Close is called. Call it in its own goroutine.
func (b *Batcher) Run() {
	defer close(b.closed)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	batch := make([]Message, 0, b.maxSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.writeBatch(batch)
		batch = batch[:0]
	}

	for {
		select {
		case m := <-b.in:
			batch = append(batch, m)
			if len(batch) >= b.maxSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.done:
			// Drain anything still queued, then do a final flush.
			for {
				select {
				case m := <-b.in:
					batch = append(batch, m)
					if len(batch) >= b.maxSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// writeBatch persists a copy of the batch, retrying once before dropping.
func (b *Batcher) writeBatch(batch []Message) {
	msgs := make([]Message, len(batch))
	copy(msgs, batch)
	if err := b.tryInsert(msgs); err != nil {
		if err2 := b.tryInsert(msgs); err2 != nil {
			b.log.Warn("dropping batch after retry", "count", len(msgs), "err", err2)
		}
	}
}

func (b *Batcher) tryInsert(msgs []Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return b.store.InsertBatch(ctx, msgs)
}

// Close stops Run and waits for the final flush to complete.
func (b *Batcher) Close() {
	close(b.done)
	<-b.closed
}
```

- [ ] **Step 6: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go test ./internal/persistence/ -race -v
```
Expected: PASS (4 batcher tests), no race warnings.

- [ ] **Step 7: Commit**

```bash
git add internal/persistence/message.go internal/persistence/batcher.go internal/persistence/helpers_test.go internal/persistence/batcher_test.go
git -c commit.gpgsign=false commit -m "Add message model and flushing batcher"
```

---

### Task 4: Worker (Redis consumer)

**Files:**
- Create: `internal/persistence/worker.go`
- Test: `internal/persistence/worker_test.go`

- [ ] **Step 1: Write `worker_test.go`**

Create `internal/persistence/worker_test.go`:
```go
package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestWorkerHandleDecodesMessage(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger()) // size 1 flushes immediately
	go b.Run()
	defer b.Close()

	w := NewWorker(nil, b, testLogger())
	w.handle(`{"type":"message","room":"x","id":7,"from":"u1","text":"hi","ts":123}`)

	waitUntil(t, time.Second, func() bool { return store.count() == 1 })
	got := store.batches[0][0]
	if got.RoomID != "x" || got.ID != 7 || got.Sender != "u1" || got.Body != "hi" || got.CreatedMS != 123 {
		t.Fatalf("unexpected message: %+v", got)
	}
}

func TestWorkerHandleIgnoresNonMessageAndGarbage(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	w := NewWorker(nil, b, testLogger())
	w.handle(`{"type":"system","room":"x","event":"join","from":"u1"}`)
	w.handle(`not json`)

	time.Sleep(50 * time.Millisecond)
	if store.count() != 0 {
		t.Fatalf("expected nothing stored, got %d", store.count())
	}
}

func TestWorkerConsumesPublishedMessage(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := &fakeStore{}
	b := NewBatcher(store, 1, 20*time.Millisecond, testLogger())
	go b.Run()
	defer b.Close()

	w := NewWorker(rdb, b, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// Republish until delivered: pub/sub may drop a publish issued before the
	// PSUBSCRIBE is active, so retry until the worker has stored the message.
	payload := `{"type":"message","room":"x","id":9,"from":"u2","text":"yo","ts":5}`
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rdb.Publish(context.Background(), "room:x", payload)
		if store.count() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if store.count() == 0 {
		t.Fatal("worker did not persist the published message")
	}
	got := store.batches[0][0]
	if got.RoomID != "x" || got.ID != 9 || got.Body != "yo" {
		t.Fatalf("unexpected message: %+v", got)
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/persistence/ -run TestWorker 2>&1 | head -20
```
Expected: FAIL to compile — `undefined: NewWorker`.

- [ ] **Step 3: Implement `worker.go`**

Create `internal/persistence/worker.go`:
```go
package persistence

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

const roomPattern = "room:*"

// inbound mirrors the gateway's message frame (only the fields we persist).
// Its json tags must match what the gateway publishes.
type inbound struct {
	Type string `json:"type"`
	Room string `json:"room"`
	ID   int64  `json:"id"`
	From string `json:"from"`
	Text string `json:"text"`
	TS   int64  `json:"ts"`
}

// Worker pattern-subscribes to room:* and forwards decoded chat messages to a
// Batcher.
type Worker struct {
	rdb     *redis.Client
	batcher *Batcher
	log     *slog.Logger
}

func NewWorker(rdb *redis.Client, batcher *Batcher, log *slog.Logger) *Worker {
	return &Worker{rdb: rdb, batcher: batcher, log: log}
}

// Run consumes messages until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	pubsub := w.rdb.PSubscribe(ctx, roomPattern)
	defer pubsub.Close()
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			w.handle(msg.Payload)
		}
	}
}

// handle decodes a payload and, if it is a chat message, submits it to the
// batcher. Non-message or malformed payloads are skipped.
func (w *Worker) handle(payload string) {
	var in inbound
	if err := json.Unmarshal([]byte(payload), &in); err != nil {
		w.log.Debug("skipping malformed payload", "err", err)
		return
	}
	if in.Type != "message" {
		return
	}
	w.batcher.Submit(Message{
		RoomID:    in.Room,
		ID:        in.ID,
		Sender:    in.From,
		Body:      in.Text,
		CreatedMS: in.TS,
	})
}
```

- [ ] **Step 4: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go test ./internal/persistence/ -race -v
```
Expected: PASS (batcher + worker tests), no race warnings. If `TestWorkerConsumesPublishedMessage` fails, confirm miniredis supports `PSUBSCRIBE` in the resolved version; do not weaken the assertion.

- [ ] **Step 5: Commit**

```bash
git add internal/persistence/worker.go internal/persistence/worker_test.go
git -c commit.gpgsign=false commit -m "Add persistence worker consuming room:* from Redis"
```

---

### Task 5: Postgres store

**Files:**
- Create: `internal/persistence/store_pg.go`
- Test: `internal/persistence/store_pg_test.go`

- [ ] **Step 1: Write `store_pg_test.go` (testcontainers, skips without Docker)**

Create `internal/persistence/store_pg_test.go`:
```go
package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("chat"),
		postgres.WithUsername("chat"),
		postgres.WithPassword("chat"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Skipf("skipping: Docker/testcontainers unavailable: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestPgxStoreInsertAndDedup(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()
	store := NewPgxStore(pool)

	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	if err := store.InsertBatch(ctx, []Message{
		{RoomID: "x", ID: 1, Sender: "a", Body: "first", CreatedMS: 10},
		{RoomID: "x", ID: 2, Sender: "b", Body: "second", CreatedMS: 20},
	}); err != nil {
		t.Fatal(err)
	}

	// Duplicate (x,1) with a different body must be ignored (ON CONFLICT).
	if err := store.InsertBatch(ctx, []Message{
		{RoomID: "x", ID: 1, Sender: "a", Body: "CHANGED", CreatedMS: 99},
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE room_id=$1`, "x").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}

	var body string
	if err := pool.QueryRow(ctx, `SELECT body FROM messages WHERE room_id=$1 AND id=$2`, "x", 1).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if body != "first" {
		t.Fatalf("expected original body preserved, got %q", body)
	}

	// Ordered retrieval by id.
	rows, err := pool.Query(ctx, `SELECT id FROM messages WHERE room_id=$1 ORDER BY id ASC`, "x")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("expected ids [1 2], got %v", ids)
	}
}
```

**Compilation note:** this test is always compiled (it only *skips* at runtime when Docker is down). The testcontainers `postgres` module API has changed across versions — if the resolved version does not provide `postgres.Run` with these option functions (older versions used `postgres.RunContainer`), adapt the `startPostgres` helper to the installed version's API so the package compiles. Keep the `t.Skipf` on container-start failure.

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/persistence/ -run TestPgxStore 2>&1 | head -20
```
Expected: FAIL to compile — `undefined: NewPgxStore`.

- [ ] **Step 3: Implement `store_pg.go`**

Create `internal/persistence/store_pg.go`:
```go
package persistence

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgxStore persists messages to Postgres via a pgx connection pool.
type PgxStore struct {
	pool *pgxpool.Pool
}

func NewPgxStore(pool *pgxpool.Pool) *PgxStore { return &PgxStore{pool: pool} }

const createMessagesTable = `
CREATE TABLE IF NOT EXISTS messages (
	room_id    TEXT   NOT NULL,
	id         BIGINT NOT NULL,
	sender     TEXT   NOT NULL,
	body       TEXT   NOT NULL,
	created_ms BIGINT NOT NULL,
	PRIMARY KEY (room_id, id)
);`

func (s *PgxStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, createMessagesTable)
	return err
}

// InsertBatch writes all messages in a single transaction, ignoring rows whose
// (room_id, id) already exists.
func (s *PgxStore) InsertBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, m := range msgs {
		batch.Queue(
			`INSERT INTO messages (room_id, id, sender, body, created_ms)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (room_id, id) DO NOTHING`,
			m.RoomID, m.ID, m.Sender, m.Body, m.CreatedMS,
		)
	}
	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
```

- [ ] **Step 4: Verify it compiles and the suite is green (PG test skips without Docker)**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/persistence/ -race -v
```
Expected: build + vet clean. Tests pass; `TestPgxStoreInsertAndDedup` reports `--- SKIP` (Docker daemon not running on this machine). If it does NOT skip and instead fails to compile, fix the testcontainers API per the compilation note in Step 1.

- [ ] **Step 5: Commit**

```bash
git add internal/persistence/store_pg.go internal/persistence/store_pg_test.go
git -c commit.gpgsign=false commit -m "Add pgx Postgres store with idempotent batch insert"
```

---

### Task 6: Worker entrypoint

**Files:**
- Create: `cmd/worker/main.go`

- [ ] **Step 1: Implement `cmd/worker/main.go`**

Create `cmd/worker/main.go`:
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
	"github.com/redis/go-redis/v9"

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/persistence"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	dbURL := getenv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")
	workerAddr := getenv("WORKER_ADDR", ":8090")

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Error("cannot create pg pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := persistence.NewPgxStore(pool)
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := store.Migrate(migrateCtx); err != nil {
		migrateCancel()
		log.Error("migrate failed", "err", err)
		os.Exit(1)
	}
	migrateCancel()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	batcher := persistence.NewBatcher(store, 100, 50*time.Millisecond, log)
	go batcher.Run()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	worker := persistence.NewWorker(rdb, batcher, log)
	workerDone := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(workerDone)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(pctx); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	healthSrv := &http.Server{Addr: workerAddr, Handler: mux}
	go func() {
		log.Info("worker health listening", "addr", workerAddr)
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server error", "err", err)
			stop()
		}
	}()

	log.Info("worker started", "redis", redisAddr)
	<-ctx.Done()
	log.Info("worker shutting down")

	<-workerDone   // worker has stopped consuming, so no more Submit calls
	batcher.Close() // flush remaining messages

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = healthSrv.Shutdown(shutdownCtx)
	log.Info("worker stopped")
}
```

- [ ] **Step 2: Build and vet**

Run:
```bash
go build ./... && go vet ./...
```
Expected: clean, exit 0.

- [ ] **Step 3: Commit**

```bash
git add cmd/worker/main.go
git -c commit.gpgsign=false commit -m "Add persistence worker entrypoint with graceful flush"
```

---

### Task 7: docker-compose Postgres + README

**Files:**
- Modify: `docker-compose.yml`, `README.md`

- [ ] **Step 1: Add Postgres to `docker-compose.yml`**

Replace the entire contents of `docker-compose.yml` with:
```yaml
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "no"]
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: chat
      POSTGRES_USER: chat
      POSTGRES_PASSWORD: chat
    ports:
      - "5432:5432"
```

- [ ] **Step 2: Update the README `## Run` section**

In `README.md`, replace the `## Run` section (from `## Run` up to but not including `## Endpoints`) with:
```markdown
## Run

Slice 2+ needs Redis; slice 3a adds Postgres. Start both with Docker:

```bash
docker compose up -d        # Redis 7 on :6379, Postgres 16 on :5432
go run ./cmd/gateway        # WebSocket gateway on :8080
go run ./cmd/worker         # persistence worker on :8090
```

Open http://localhost:8080/ in two browser tabs, join the same room, and chat.
Messages fan out in real time and the worker persists them to Postgres.

Environment variables:

- `ADDR` — gateway listen address (default `:8080`)
- `WEB_DIR` — directory served at `/` (default `web`)
- `REDIS_ADDR` — Redis address (default `localhost:6379`)
- `DATABASE_URL` — Postgres DSN (default `postgres://chat:chat@localhost:5432/chat?sslmode=disable`)
- `WORKER_ADDR` — worker health address (default `:8090`)
```

In the `## Roadmap` section, update the line to:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history API → presence/typing → rate limiting/auth → observability → K8s + load test.
```

- [ ] **Step 3: Validate compose YAML**

Run:
```bash
docker compose config >/dev/null && echo "compose ok"
```
Expected: `compose ok` (YAML parse only; does not need the Docker daemon).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml README.md
git -c commit.gpgsign=false commit -m "Add Postgres to compose and document the worker"
```

---

### Task 8: Final verification

- [ ] **Step 1: Full suite**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./... && go test ./... -race -count=2
```
Expected: builds clean, vet clean, all tests `ok` (the Postgres integration test skips), no race warnings.

- [ ] **Step 2: Confirm tree is clean**

Run:
```bash
git status --porcelain
```
Expected: empty (ignore any local `*.exe` build artifact; delete it, do not commit).

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `Frame.ID` + per-room `INCR` (Task 2), `Message`/`MessageStore` (Task 3), batcher 100/50ms + retry-once-then-drop (Task 3), worker `PSUBSCRIBE room:*` + decode/filter (Task 4), `PgxStore` idempotent insert + schema (Task 5), `cmd/worker` with health + graceful flush (Task 6), docker-compose Postgres + README (Task 7), fake-store + miniredis + skippable testcontainers tests (Tasks 3-5). Out of scope (history API, replay, room CRUD, multi-worker) is correctly absent.
- **Signature consistency:** `Bus.NextID(ctx, room) (int64, error)`; `Hub.Publish` stamps `f.ID`; `Message{RoomID,ID,Sender,Body,CreatedMS}`; `MessageStore{Migrate, InsertBatch}`; `NewBatcher(store, maxSize, interval, log)` / `Submit` / `Run` / `Close`; `NewWorker(rdb, batcher, log)` / `Run(ctx)` / `handle(payload)`; `NewPgxStore(pool)`. `fakeStore` and `PgxStore` both satisfy `MessageStore`; `fakeBus` and `RedisBus` both satisfy `Bus` (now incl. `NextID`).
- **Shutdown ordering (worker):** cancel ctx → wait `workerDone` (worker stopped submitting) → `batcher.Close()` (flush) → close health server. This avoids a `Submit` on a closed batcher.
- **testcontainers API churn:** the PG test must compile against the resolved version (adapt `postgres.Run`/`RunContainer` if needed) and skip at runtime when Docker is down.
