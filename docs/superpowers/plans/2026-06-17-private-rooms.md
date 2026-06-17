# Private Rooms + Room CRUD Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add registered public/private rooms (private require an invite token to join) and a room CRUD REST API, keeping ad-hoc public chat working.

**Architecture:** A `rooms` table in Postgres. The gateway owns a `RoomStore` interface + `RoomRecord` type (adapter in `main` bridges to `persistence.PgxStore`). JOIN is gated: unregistered/public rooms join freely; a registered private room requires its token. `/api/rooms` provides CRUD.

**Tech Stack:** Go 1.22+, existing pgx/chi; tests use the existing testcontainers + httptest setup.

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
| `internal/persistence/rooms.go` | **new** | `RoomRecord`, `ErrRoomExists`, Create/Get/List/Delete |
| `internal/persistence/store_pg.go` | modify | `Migrate` also creates `rooms` |
| `internal/persistence/rooms_test.go` | **new** | room store CRUD (testcontainers) |
| `internal/gateway/rooms.go` | **new** | `RoomRecord`, `RoomStore`, `ErrRoomExists`, `newInviteToken` |
| `internal/gateway/rooms_fake_test.go` | **new** | `fakeRoomStore` |
| `internal/gateway/protocol.go` | modify | `Frame.Token` |
| `internal/gateway/client.go` | modify | `clientConfig.rooms`; JOIN gate |
| `internal/gateway/client_test.go` | modify | helpers add rooms; JOIN-gate tests |
| `internal/gateway/server.go` | modify | `NewServer` rooms; REST handlers (Task 3) |
| `internal/gateway/server_test.go` | modify | ctor arity; REST tests (Task 3) |
| `cmd/gateway/main.go` | modify | `roomAdapter` |
| `web/index.html` | modify | invite-token input on join |
| `README.md` | modify | room CRUD docs |

---

### Task 1: Postgres room store

**Files:**
- Create: `internal/persistence/rooms.go`, `internal/persistence/rooms_test.go`
- Modify: `internal/persistence/store_pg.go`

- [ ] **Step 1: Write `rooms_test.go` (testcontainers, skips w/o Docker)**

Create `internal/persistence/rooms_test.go`:
```go
package persistence

import (
	"context"
	"errors"
	"testing"
)

func TestRoomStoreCRUD(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()
	store := NewPgxStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	if err := store.CreateRoom(ctx, RoomRecord{ID: "team", Visibility: "private", InviteToken: "tok", CreatedMS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRoom(ctx, RoomRecord{ID: "team", Visibility: "private", InviteToken: "x", CreatedMS: 2}); !errors.Is(err, ErrRoomExists) {
		t.Fatalf("expected ErrRoomExists on duplicate, got %v", err)
	}

	got, found, err := store.GetRoom(ctx, "team")
	if err != nil || !found {
		t.Fatalf("get team: err=%v found=%v", err, found)
	}
	if got.Visibility != "private" || got.InviteToken != "tok" {
		t.Fatalf("unexpected room: %+v", got)
	}

	if err := store.CreateRoom(ctx, RoomRecord{ID: "lounge", Visibility: "public", CreatedMS: 3}); err != nil {
		t.Fatal(err)
	}
	pub, _, _ := store.GetRoom(ctx, "lounge")
	if pub.InviteToken != "" {
		t.Fatalf("public room should have empty token, got %q", pub.InviteToken)
	}

	rooms, err := store.ListRooms(ctx)
	if err != nil || len(rooms) != 2 {
		t.Fatalf("list: err=%v n=%d", err, len(rooms))
	}

	if _, found, _ := store.GetRoom(ctx, "ghost"); found {
		t.Fatal("ghost should not be found")
	}

	if err := store.DeleteRoom(ctx, "team"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.GetRoom(ctx, "team"); found {
		t.Fatal("team should be deleted")
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/persistence/ -run TestRoomStoreCRUD 2>&1 | head -10
```
Expected: FAIL to compile — `undefined: RoomRecord` / `store.CreateRoom`.

- [ ] **Step 3: Add the rooms table to `Migrate`**

In `internal/persistence/store_pg.go`, add the rooms DDL constant after `createMessagesTable`:
```go
const createRoomsTable = `
CREATE TABLE IF NOT EXISTS rooms (
	id           TEXT PRIMARY KEY,
	visibility   TEXT NOT NULL,
	invite_token TEXT,
	created_ms   BIGINT NOT NULL
);`
```
Replace `Migrate` with:
```go
func (s *PgxStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, createMessagesTable); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, createRoomsTable)
	return err
}
```

- [ ] **Step 4: Create `rooms.go`**

Create `internal/persistence/rooms.go`:
```go
package persistence

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrRoomExists is returned by CreateRoom when the id is already taken.
var ErrRoomExists = errors.New("room already exists")

// RoomRecord is a registered room.
type RoomRecord struct {
	ID          string
	Visibility  string
	InviteToken string
	CreatedMS   int64
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *PgxStore) CreateRoom(ctx context.Context, r RoomRecord) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO rooms (id, visibility, invite_token, created_ms)
		 VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
		r.ID, r.Visibility, nullIfEmpty(r.InviteToken), r.CreatedMS)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrRoomExists
	}
	return nil
}

func (s *PgxStore) GetRoom(ctx context.Context, id string) (RoomRecord, bool, error) {
	var r RoomRecord
	var token *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, visibility, invite_token, created_ms FROM rooms WHERE id=$1`, id).
		Scan(&r.ID, &r.Visibility, &token, &r.CreatedMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return RoomRecord{}, false, nil
	}
	if err != nil {
		return RoomRecord{}, false, err
	}
	if token != nil {
		r.InviteToken = *token
	}
	return r, true, nil
}

func (s *PgxStore) ListRooms(ctx context.Context) ([]RoomRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, visibility, invite_token, created_ms FROM rooms ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var token *string
		if err := rows.Scan(&r.ID, &r.Visibility, &token, &r.CreatedMS); err != nil {
			return nil, err
		}
		if token != nil {
			r.InviteToken = *token
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PgxStore) DeleteRoom(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM rooms WHERE id=$1`, id)
	return err
}
```

- [ ] **Step 5: Verify build/vet and that the suite is green (PG tests skip)**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/persistence/ -race
```
Expected: build+vet clean; `ok`, the room test reports `--- SKIP` (Docker down).

- [ ] **Step 6: Commit**

```bash
git add internal/persistence/rooms.go internal/persistence/rooms_test.go internal/persistence/store_pg.go
git -c commit.gpgsign=false commit -m "Add Postgres room store and rooms table"
```

---

### Task 2: Gateway room store + JOIN gate

Changes `clientConfig` and `NewServer` (adds a `rooms` arg) — ripples to tests and `main.go`. One commit. Tests first.

**Files:**
- Create: `internal/gateway/rooms.go`, `internal/gateway/rooms_fake_test.go`
- Modify: `internal/gateway/protocol.go`, `internal/gateway/client.go`, `internal/gateway/server.go`, `cmd/gateway/main.go`, `internal/gateway/client_test.go`, `internal/gateway/server_test.go`

- [ ] **Step 1: Create the gateway room types + fake**

Create `internal/gateway/rooms.go`:
```go
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// ErrRoomExists is returned by Create when the id is already taken.
var ErrRoomExists = errors.New("room already exists")

// RoomRecord is a registered room as the gateway consumes it.
type RoomRecord struct {
	ID          string
	Visibility  string
	InviteToken string
}

// RoomStore is the room metadata the gateway depends on; an adapter over the
// persistence layer satisfies it, tests use a fake.
type RoomStore interface {
	Lookup(ctx context.Context, id string) (RoomRecord, bool, error)
	Create(ctx context.Context, r RoomRecord) error
	List(ctx context.Context) ([]RoomRecord, error)
	Delete(ctx context.Context, id string) error
}

func newInviteToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
```

Create `internal/gateway/rooms_fake_test.go`:
```go
package gateway

import (
	"context"
	"sync"
)

type fakeRoomStore struct {
	mu        sync.Mutex
	rooms     map[string]RoomRecord
	lookupErr error
}

func newFakeRoomStore() *fakeRoomStore {
	return &fakeRoomStore{rooms: make(map[string]RoomRecord)}
}

func (s *fakeRoomStore) put(r RoomRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rooms[r.ID] = r
}

func (s *fakeRoomStore) Lookup(_ context.Context, id string) (RoomRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lookupErr != nil {
		return RoomRecord{}, false, s.lookupErr
	}
	r, ok := s.rooms[id]
	return r, ok, nil
}

func (s *fakeRoomStore) Create(_ context.Context, r RoomRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[r.ID]; ok {
		return ErrRoomExists
	}
	s.rooms[r.ID] = r
	return nil
}

func (s *fakeRoomStore) List(_ context.Context) ([]RoomRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RoomRecord, 0, len(s.rooms))
	for _, r := range s.rooms {
		out = append(out, r)
	}
	return out, nil
}

func (s *fakeRoomStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rooms, id)
	return nil
}
```

- [ ] **Step 2: Update `client_test.go` helpers + add JOIN-gate tests**

In `client_test.go`, add `rooms` to the three constructor helpers' `clientConfig` (each currently builds a `clientConfig{...}`): add the field `rooms: newFakeRoomStore(),` to `newTestClient`, `newPresenceClient`, and `newRateClient`.

Add a JOIN-gate helper (after the other helpers):
```go
func newJoinGateClient(reg roomRegistry, rooms RoomStore) *Client {
	cfg := clientConfig{
		hub:      reg,
		history:  &fakeHistory{},
		presence: newFakePresenceStore(),
		limiter:  &fakeRateLimiter{allow: true},
		rooms:    rooms,
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(context.Background(), "tester", cfg, func() {})
}
```

Append the JOIN-gate tests:
```go
func TestJoinUnregisteredRoomAllowed(t *testing.T) {
	reg := &fakeRegistry{}
	c := newJoinGateClient(reg, newFakeRoomStore())
	c.handleFrame(Frame{Type: TypeJoin, Room: "general"})
	if len(reg.joined) != 1 {
		t.Fatalf("expected join of public room, got %+v", reg.joined)
	}
}

func TestJoinPrivateRoomRequiresToken(t *testing.T) {
	reg := &fakeRegistry{}
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "secret", Visibility: "private", InviteToken: "tok"})
	c := newJoinGateClient(reg, rooms)

	c.handleFrame(Frame{Type: TypeJoin, Room: "secret"})

	if len(reg.joined) != 0 {
		t.Fatal("join of private room without token must be rejected")
	}
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected one error frame, got %+v", out)
	}
}

func TestJoinPrivateRoomWithToken(t *testing.T) {
	reg := &fakeRegistry{}
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "secret", Visibility: "private", InviteToken: "tok"})
	c := newJoinGateClient(reg, rooms)

	c.handleFrame(Frame{Type: TypeJoin, Room: "secret", Token: "tok"})

	if len(reg.joined) != 1 {
		t.Fatalf("join with correct token should be allowed, got %+v", reg.joined)
	}
}

func TestJoinLookupErrorFailsClosed(t *testing.T) {
	reg := &fakeRegistry{}
	rooms := newFakeRoomStore()
	rooms.lookupErr = errors.New("db down")
	c := newJoinGateClient(reg, rooms)

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	if len(reg.joined) != 0 {
		t.Fatal("a lookup error must fail closed (no join)")
	}
}
```

- [ ] **Step 3: Update `server_test.go` constructor arity**

Every `NewServer(...)` call gains a `RoomStore` argument **after `limiter` and before `log`**. Update `newTestServer`:
```go
func newTestServer() *Server {
	bus := newFakeBus()
	hub := NewHub(bus)
	return NewServer(hub, bus, &fakeHistory{}, newFakePresenceStore(), &fakeRateLimiter{allow: true}, newFakeRoomStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}
```
In `TestReadyzFailsWhenRedisDown`, `TestReadyzFailsWhenPostgresDown`, `TestHistoryEndpointReturnsMessages`, `TestHistoryEndpointBeforeParam`, `TestHistoryEndpointStoreErrorReturns503`, insert `newFakeRoomStore()` before the logger arg in each `NewServer(...)` call.

- [ ] **Step 4: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run "TestJoinUnregistered|TestJoinPrivate|TestJoinLookup" 2>&1 | head -20
```
Expected: FAIL to compile — `unknown field rooms in clientConfig` / `Frame.Token undefined` / `NewServer` arity.

- [ ] **Step 5: Add `Frame.Token`**

In `internal/gateway/protocol.go`, add to `Frame` (after `Trace`):
```go
	Token string `json:"token,omitempty"`
```

- [ ] **Step 6: Add the room store to `clientConfig` + the JOIN gate (`client.go`)**

In `internal/gateway/client.go`:

Add `rooms RoomStore` to `clientConfig`:
```go
type clientConfig struct {
	hub      roomRegistry
	history  history
	presence PresenceStore
	limiter  RateLimiter
	rooms    RoomStore
	log      *slog.Logger
}
```
Add a `rooms RoomStore` field to `Client` (after `limiter`), and set it in `newClient`:
```go
		rooms:      cfg.rooms,
```
Add the gate as the first thing in the `TypeJoin` case (after the empty-room check, before taking the lock):
```go
	case TypeJoin:
		if f.Room == "" {
			c.enqueue(errorFrame("join requires a room"))
			return
		}
		if !c.allowJoin(f.Room, f.Token) {
			return
		}
		c.mu.Lock()
```
(the rest of the `TypeJoin` body is unchanged). Add the `allowJoin` method (after `handleFrame`):
```go
// allowJoin gates a join on room visibility. Unregistered or public rooms are
// allowed; a private room requires a matching token. A lookup error fails
// closed (rejects), so a DB blip cannot leak a private room. It enqueues the
// error frame on denial.
func (c *Client) allowJoin(room, token string) bool {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	rec, found, err := c.rooms.Lookup(ctx, room)
	if err != nil {
		c.log.Warn("room lookup failed", "room", room, "err", err)
		c.enqueue(errorFrame("room unavailable"))
		return false
	}
	if found && rec.Visibility == "private" && rec.InviteToken != token {
		c.enqueue(errorFrame("invalid invite token"))
		return false
	}
	return true
}
```

- [ ] **Step 7: Add the room store to `NewServer` (`server.go`)**

In `internal/gateway/server.go`, add a `rooms RoomStore` field to `Server` (after `hist`):
```go
	rooms     RoomStore
```
Update `NewServer` to take `rooms RoomStore` (after `limiter`, before `log`) and set both the field and `clientCfg.rooms`:
```go
func NewServer(hub *Hub, bus pinger, hist history, presence PresenceStore, limiter RateLimiter, rooms RoomStore, log *slog.Logger, webDir string) *Server {
	return &Server{
		hub:    hub,
		bus:    bus,
		hist:   hist,
		rooms:  rooms,
		log:    log,
		webDir: webDir,
		clientCfg: clientConfig{
			hub:      hub,
			history:  hist,
			presence: presence,
			limiter:  limiter,
			rooms:    rooms,
			log:      log,
		},
	}
}
```
(REST routes/handlers come in Task 3.)

- [ ] **Step 8: Add the `roomAdapter` in `cmd/gateway/main.go`**

In `cmd/gateway/main.go`, add `"errors"` to imports if not present. Build the pgx store once and reuse it for both adapters. Replace the `hist := histAdapter{store: persistence.NewPgxStore(pool)}` line with:
```go
	store := persistence.NewPgxStore(pool)
	hist := histAdapter{store: store}
	rooms := roomAdapter{store: store}
```
Add the adapter (next to `histAdapter`):
```go
type roomAdapter struct {
	store *persistence.PgxStore
}

func (a roomAdapter) Lookup(ctx context.Context, id string) (gateway.RoomRecord, bool, error) {
	r, found, err := a.store.GetRoom(ctx, id)
	if err != nil || !found {
		return gateway.RoomRecord{}, found, err
	}
	return gateway.RoomRecord{ID: r.ID, Visibility: r.Visibility, InviteToken: r.InviteToken}, true, nil
}

func (a roomAdapter) Create(ctx context.Context, r gateway.RoomRecord) error {
	err := a.store.CreateRoom(ctx, persistence.RoomRecord{
		ID: r.ID, Visibility: r.Visibility, InviteToken: r.InviteToken, CreatedMS: time.Now().UnixMilli(),
	})
	if errors.Is(err, persistence.ErrRoomExists) {
		return gateway.ErrRoomExists
	}
	return err
}

func (a roomAdapter) List(ctx context.Context) ([]gateway.RoomRecord, error) {
	recs, err := a.store.ListRooms(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]gateway.RoomRecord, len(recs))
	for i, r := range recs {
		out[i] = gateway.RoomRecord{ID: r.ID, Visibility: r.Visibility, InviteToken: r.InviteToken}
	}
	return out, nil
}

func (a roomAdapter) Delete(ctx context.Context, id string) error {
	return a.store.DeleteRoom(ctx, id)
}
```
Change the server construction to pass `rooms`:
```go
	srv := gateway.NewServer(hub, bus, hist, presence, limiter, rooms, log, webDir)
```

- [ ] **Step 9: Build, vet, race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./... -race -count=2
```
Expected: build+vet clean; all tests `ok` (PG tests skip), no race warnings — including the four JOIN-gate tests and all existing tests (which now go through the gate with an empty fake store → public → allowed).

- [ ] **Step 10: Commit**

```bash
git add internal/gateway/ cmd/gateway/main.go
git -c commit.gpgsign=false commit -m "Gate joins on room visibility and invite token"
```

---

### Task 3: Room CRUD REST endpoints

**Files:**
- Modify: `internal/gateway/server.go`, `internal/gateway/server_test.go`

- [ ] **Step 1: Write the REST tests**

Append to `internal/gateway/server_test.go` (the import block already has `strings`, `encoding/json`, `net/http`, `httptest`):
```go
func newRoomServer(rooms RoomStore) *Server {
	bus := newFakeBus()
	return NewServer(NewHub(bus), bus, &fakeHistory{}, newFakePresenceStore(), &fakeRateLimiter{allow: true}, rooms, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
}

func TestCreateRoomPrivateReturnsToken(t *testing.T) {
	srv := newRoomServer(newFakeRoomStore())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rooms", strings.NewReader(`{"id":"secret","visibility":"private"}`))
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d, want 201", rec.Code)
	}
	var resp struct {
		ID          string `json:"id"`
		Visibility  string `json:"visibility"`
		InviteToken string `json:"invite_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.InviteToken == "" {
		t.Fatal("expected an invite token for a private room")
	}
}

func TestCreateRoomPublicHasNoToken(t *testing.T) {
	srv := newRoomServer(newFakeRoomStore())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rooms", strings.NewReader(`{"id":"lounge","visibility":"public"}`))
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d, want 201", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "invite_token") {
		t.Fatalf("public room response should omit invite_token: %s", rec.Body.String())
	}
}

func TestCreateRoomDuplicate409(t *testing.T) {
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "taken", Visibility: "public"})
	srv := newRoomServer(rooms)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rooms", strings.NewReader(`{"id":"taken","visibility":"public"}`))
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate create = %d, want 409", rec.Code)
	}
}

func TestCreateRoomBadVisibility400(t *testing.T) {
	srv := newRoomServer(newFakeRoomStore())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rooms", strings.NewReader(`{"id":"x","visibility":"secret"}`))
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad visibility = %d, want 400", rec.Code)
	}
}

func TestListRoomsOmitsTokens(t *testing.T) {
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "secret", Visibility: "private", InviteToken: "tok"})
	srv := newRoomServer(rooms)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d, want 200", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "tok") {
		t.Fatalf("list must not expose invite tokens: %s", rec.Body.String())
	}
}

func TestGetRoomNotFound404(t *testing.T) {
	srv := newRoomServer(newFakeRoomStore())
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/ghost", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get absent = %d, want 404", rec.Code)
	}
}

func TestDeleteRoom204(t *testing.T) {
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "tmp", Visibility: "public"})
	srv := newRoomServer(rooms)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/rooms/tmp", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", rec.Code)
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run "TestCreateRoom|TestListRooms|TestGetRoom|TestDeleteRoom" 2>&1 | head -20
```
Expected: the create/list/get/delete routes return 404/405 (not registered yet), so the tests fail their status assertions.

- [ ] **Step 3: Add the REST routes + handlers (`server.go`)**

Add `"errors"` to the `server.go` import block.

In `Router()`, add the room routes (after the existing `/api/rooms/{room}/messages` line):
```go
	r.Post("/api/rooms", s.handleCreateRoom)
	r.Get("/api/rooms", s.handleListRooms)
	r.Get("/api/rooms/{id}", s.handleGetRoom)
	r.Delete("/api/rooms/{id}", s.handleDeleteRoom)
```

Add a view type and the four handlers (near `handleHistory`):
```go
type roomView struct {
	ID         string `json:"id"`
	Visibility string `json:"visibility"`
}

func (s *Server) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID         string `json:"id"`
		Visibility string `json:"visibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.ID == "" || (body.Visibility != "public" && body.Visibility != "private") {
		http.Error(w, "id required; visibility must be public or private", http.StatusBadRequest)
		return
	}
	rec := RoomRecord{ID: body.ID, Visibility: body.Visibility}
	if body.Visibility == "private" {
		rec.InviteToken = newInviteToken()
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.rooms.Create(ctx, rec); err != nil {
		if errors.Is(err, ErrRoomExists) {
			http.Error(w, "room already exists", http.StatusConflict)
			return
		}
		s.log.Warn("create room failed", "id", rec.ID, "err", err)
		http.Error(w, "create failed", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(struct {
		ID          string `json:"id"`
		Visibility  string `json:"visibility"`
		InviteToken string `json:"invite_token,omitempty"`
	}{rec.ID, rec.Visibility, rec.InviteToken})
}

func (s *Server) handleListRooms(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	recs, err := s.rooms.List(ctx)
	if err != nil {
		s.log.Warn("list rooms failed", "err", err)
		http.Error(w, "list failed", http.StatusServiceUnavailable)
		return
	}
	out := make([]roomView, len(recs))
	for i, rec := range recs {
		out[i] = roomView{ID: rec.ID, Visibility: rec.Visibility}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Rooms []roomView `json:"rooms"`
	}{Rooms: out})
}

func (s *Server) handleGetRoom(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rec, found, err := s.rooms.Lookup(ctx, id)
	if err != nil {
		http.Error(w, "lookup failed", http.StatusServiceUnavailable)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(roomView{ID: rec.ID, Visibility: rec.Visibility})
}

func (s *Server) handleDeleteRoom(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.rooms.Delete(ctx, id); err != nil {
		http.Error(w, "delete failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 4: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/gateway/ -race
```
Expected: `ok`, no race warnings (the 7 REST tests pass).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/server.go internal/gateway/server_test.go
git -c commit.gpgsign=false commit -m "Add room CRUD REST endpoints"
```

---

### Task 4: Demo + README + final verification

**Files:**
- Modify: `web/index.html`, `README.md`

- [ ] **Step 1: Add an invite-token input to the demo**

In `web/index.html`, add a token input to the room row. Find:
```html
    <input id="room" value="general" placeholder="room" />
    <button id="join">Join</button>
```
and insert a token input between them:
```html
    <input id="room" value="general" placeholder="room" />
    <input id="token" placeholder="invite token (private)" />
    <button id="join">Join</button>
```
In the join handler, find:
```js
        room = document.getElementById("room").value.trim();
        ws.send(JSON.stringify({ type: "join", room }));
```
and replace the `ws.send` with one that includes the token:
```js
        room = document.getElementById("room").value.trim();
        const token = document.getElementById("token").value.trim();
        ws.send(JSON.stringify({ type: "join", room, token }));
```
After editing, read the file back and confirm the JS is well-formed.

- [ ] **Step 2: Update the README**

In `README.md`, under `## Endpoints`, add the room CRUD entry after the history endpoint bullet:
```markdown
- `POST /api/rooms` — create a room. Body `{"id":"...","visibility":"public|private"}`;
  a private room returns `{"id","visibility","invite_token"}` (join private rooms
  by sending that token in the JOIN frame's `token` field). Duplicate id → 409.
- `GET /api/rooms` — list registered rooms (`{"rooms":[{"id","visibility"}]}`,
  never tokens). `GET /api/rooms/{id}` — room metadata or 404.
  `DELETE /api/rooms/{id}` — remove a room (204).
```

- [ ] **Step 3: Final verification**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./... && go test ./... -race -count=2
```
Expected: builds clean, vet clean, all tests `ok` (PG tests skip), no race warnings.

- [ ] **Step 4: Confirm tree clean and commit**

Run:
```bash
git status --porcelain
```
Expected: empty after the commit (ignore/delete any local `*.exe`).

```bash
git add web/index.html README.md
git -c commit.gpgsign=false commit -m "Add invite-token input to demo and document room CRUD"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** rooms table + store (Task 1), gateway `RoomStore`/`RoomRecord`/`ErrRoomExists`/token (Task 2), JOIN gate with fail-closed (Task 2 client), CRUD endpoints with token-hiding on list/get and 409/400/404 (Task 3), demo token input + docs (Task 4), join-gate + REST + store tests (all tasks). Out-of-scope (ownership, roles, token rotation) absent.
- **Signature consistency:** `RoomStore{Lookup,Create,List,Delete}`; `RoomRecord{ID,Visibility,InviteToken}`; `NewServer(hub, bus, hist, presence, limiter, rooms, log, webDir)`; `clientConfig.rooms`. `fakeRoomStore` and `roomAdapter` both satisfy `gateway.RoomStore`; `persistence.PgxStore` provides Create/Get/List/Delete.
- **NewServer is now 8 positional params.** All are distinct types (the compiler catches mis-ordering), so it's acceptable; a future `ServerConfig` struct would be a reasonable cleanup but is out of scope here.
- **JOIN gate is synchronous** (a small bounded DB read before joining); fail-closed on lookup error. Existing join tests pass an empty fake store → unregistered → public → allowed, so they are unaffected.
- **Token hygiene:** the list and get views are a dedicated `roomView{ID,Visibility}` — invite tokens are never serialized there; only the create response returns the token (to its creator).
