# F2 — Membership + Roster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Persist room membership with last-seen and serve `GET /api/rooms/{room}/members` with online/away/offline status.

**Architecture:** A `room_members` table + `MemberStore`; the client touches last-seen on join/heartbeat/leave; the roster endpoint derives status from the live presence set + last-seen. `NewServer` is refactored to a `ServerConfig` struct (it would otherwise reach 9 positional params).

**Tech Stack:** Go 1.22+, pgx/chi.

**Commit convention:** Commit on `main`, no push, no attribution, `git -c commit.gpgsign=false`.

**Note:** the race detector's C compiler is currently unavailable; run `CGO_ENABLED=0 go test ./...`. If disk errors: `go clean -cache -testcache`.

---

### Task 1: Persistence membership

**Files:** modify `internal/persistence/store_pg.go`, `internal/persistence/rooms.go`; add `internal/persistence/members_test.go`.

- [ ] **Step 1: Test** — create `internal/persistence/members_test.go`:
```go
package persistence

import (
	"context"
	"testing"
)

func TestMemberStoreTouchAndList(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()
	store := NewPgxStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchMember(ctx, "x", "alice", 100); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchMember(ctx, "x", "bob", 50); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchMember(ctx, "x", "alice", 200); err != nil { // upsert
		t.Fatal(err)
	}
	got, err := store.ListMembers(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Username != "alice" || got[0].LastSeenMs != 200 {
		t.Fatalf("unexpected members: %+v", got)
	}
}
```

- [ ] **Step 2: Run red** — `go test ./internal/persistence/ -run TestMemberStore 2>&1 | head` → undefined: TouchMember.

- [ ] **Step 3: Migrate** — in `store_pg.go`, add to the end of `Migrate` (before its final `return`), chain another Exec for the table. Replace the current final `ALTER TABLE` Exec/return with:
```go
	if _, err := s.pool.Exec(ctx,
		`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '';
		 ALTER TABLE rooms ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';`); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, createRoomMembersTable)
	return err
```
and add the constant near `createRoomsTable`:
```go
const createRoomMembersTable = `
CREATE TABLE IF NOT EXISTS room_members (
	room_id      TEXT   NOT NULL,
	username     TEXT   NOT NULL,
	last_seen_ms BIGINT NOT NULL,
	PRIMARY KEY (room_id, username)
);`
```

- [ ] **Step 4: Methods** — append to `internal/persistence/rooms.go`:
```go
type MemberRecord struct {
	Username   string
	LastSeenMs int64
}

func (s *PgxStore) TouchMember(ctx context.Context, room, username string, lastSeenMs int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_members (room_id, username, last_seen_ms)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (room_id, username) DO UPDATE SET last_seen_ms = EXCLUDED.last_seen_ms`,
		room, username, lastSeenMs)
	return err
}

func (s *PgxStore) ListMembers(ctx context.Context, room string) ([]MemberRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT username, last_seen_ms FROM room_members WHERE room_id=$1 ORDER BY username ASC`, room)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemberRecord
	for rows.Next() {
		var m MemberRecord
		if err := rows.Scan(&m.Username, &m.LastSeenMs); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
```

- [ ] **Step 5: Verify** — `go build ./... && go vet ./...`; `CGO_ENABLED=0 go test ./internal/persistence/` → member test SKIPs (Docker down), rest pass.

- [ ] **Step 6: Commit** — `git add internal/persistence/ && git -c commit.gpgsign=false commit -m "Add room_members store (touch + list)"`.

---

### Task 2: Gateway roster + membership wiring + ServerConfig

**Files:** add `internal/gateway/members_fake_test.go`; modify `internal/gateway/rooms.go`, `client.go`, `server.go`, `server_test.go`, `client_test.go`, `cmd/gateway/main.go`.

- [ ] **Step 1: Gateway member types** — append to `internal/gateway/rooms.go`:
```go
const awayWindowMs int64 = 300000 // 5 minutes

type MemberRecord struct {
	Username   string
	LastSeenMs int64
}

type MemberStore interface {
	Touch(ctx context.Context, room, username string, lastSeenMs int64) error
	List(ctx context.Context, room string) ([]MemberRecord, error)
}
```

- [ ] **Step 2: Fake member store** — create `internal/gateway/members_fake_test.go`:
```go
package gateway

import (
	"context"
	"sync"
)

type fakeMemberStore struct {
	mu      sync.Mutex
	members map[string][]MemberRecord
	touches []string
}

func newFakeMemberStore() *fakeMemberStore {
	return &fakeMemberStore{members: make(map[string][]MemberRecord)}
}

func (s *fakeMemberStore) put(room string, m MemberRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members[room] = append(s.members[room], m)
}

func (s *fakeMemberStore) Touch(_ context.Context, room, username string, _ int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touches = append(s.touches, room+"/"+username)
	return nil
}

func (s *fakeMemberStore) List(_ context.Context, room string) ([]MemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.members[room], nil
}

func (s *fakeMemberStore) touchedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.touches)
}
```

- [ ] **Step 3: ServerConfig refactor (`server.go`)** — add a `members MemberStore` field to `Server`; replace `NewServer` with the struct form:
```go
type ServerConfig struct {
	Hub      *Hub
	Bus      pinger
	History  history
	Presence PresenceStore
	Limiter  RateLimiter
	Rooms    RoomStore
	Members  MemberStore
	Log      *slog.Logger
	WebDir   string
}

func NewServer(cfg ServerConfig) *Server {
	return &Server{
		hub:     cfg.Hub,
		bus:     cfg.Bus,
		hist:    cfg.History,
		rooms:   cfg.Rooms,
		members: cfg.Members,
		log:     cfg.Log,
		webDir:  cfg.WebDir,
		clientCfg: clientConfig{
			hub:      cfg.Hub,
			history:  cfg.History,
			presence: cfg.Presence,
			limiter:  cfg.Limiter,
			rooms:    cfg.Rooms,
			members:  cfg.Members,
			log:      cfg.Log,
		},
	}
}
```
Add `"sort"` to the `server.go` imports. Add the route in `Router()` (after the `/api/rooms/{id}` GET):
```go
	r.Get("/api/rooms/{room}/members", s.handleRoomMembers)
```
Add the handler (near `handleGetRoom`):
```go
func (s *Server) handleRoomMembers(w http.ResponseWriter, r *http.Request) {
	room := chi.URLParam(r, "room")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	now := nowMillis()
	onlineList, err := s.clientCfg.presence.Snapshot(ctx, room, now-presenceTTLms)
	if err != nil {
		http.Error(w, "members unavailable", http.StatusServiceUnavailable)
		return
	}
	online := make(map[string]bool, len(onlineList))
	for _, u := range onlineList {
		online[u] = true
	}
	members, err := s.members.List(ctx, room)
	if err != nil {
		http.Error(w, "members unavailable", http.StatusServiceUnavailable)
		return
	}

	type memberView struct {
		Username string `json:"username"`
		Status   string `json:"status"`
	}
	rank := map[string]int{"online": 0, "away": 1, "offline": 2}
	out := make([]memberView, 0, len(members))
	for _, m := range members {
		status := "offline"
		if online[m.Username] {
			status = "online"
		} else if now-m.LastSeenMs <= awayWindowMs {
			status = "away"
		}
		out = append(out, memberView{Username: m.Username, Status: status})
	}
	sort.Slice(out, func(i, j int) bool {
		if rank[out[i].Status] != rank[out[j].Status] {
			return rank[out[i].Status] < rank[out[j].Status]
		}
		return out[i].Username < out[j].Username
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Members []memberView `json:"members"`
	}{Members: out})
}
```

- [ ] **Step 4: Update every `NewServer(...)` call site to the struct form.** In `server_test.go` (`newTestServer` + `TestReadyzFailsWhenRedisDown` + `TestReadyzFailsWhenPostgresDown` + `TestHistoryEndpointReturnsMessages` + `TestHistoryEndpointBeforeParam` + `TestHistoryEndpointStoreErrorReturns503` + `TestListRoomsIncludesMetadataAndOnlineCount` + `newRoomServer`) and in `cmd/gateway/main.go`. Each positional `NewServer(hub, bus, hist, presence, limiter, rooms, log, webDir)` becomes `NewServer(ServerConfig{Hub: hub, Bus: bus, History: hist, Presence: presence, Limiter: limiter, Rooms: rooms, Members: <fake-or-adapter>, Log: log, WebDir: webDir})`. In tests use `Members: newFakeMemberStore()`; in `main.go` use `Members: memberAdapter{store: store}` (Step 7). Example `newTestServer`:
```go
func newTestServer() *Server {
	bus := newFakeBus()
	return NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{},
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})
}
```
And `newRoomServer(rooms RoomStore)`:
```go
func newRoomServer(rooms RoomStore) *Server {
	bus := newFakeBus()
	return NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{},
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: rooms, Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})
}
```
(The compiler will flag every site that still uses the old positional form — fix each.)

- [ ] **Step 5: Roster + touch tests** — append to `internal/gateway/server_test.go`:
```go
func TestRoomMembersStatus(t *testing.T) {
	bus := newFakeBus()
	ps := newFakePresenceStore()
	ps.Add(context.Background(), "general", "online_user", nowMillis())
	ms := newFakeMemberStore()
	now := nowMillis()
	ms.put("general", MemberRecord{Username: "online_user", LastSeenMs: now})
	ms.put("general", MemberRecord{Username: "away_user", LastSeenMs: now - 60000})
	ms.put("general", MemberRecord{Username: "offline_user", LastSeenMs: now - 600000})
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{}, Presence: ps,
		Limiter: &fakeRateLimiter{allow: true}, Rooms: newFakeRoomStore(), Members: ms,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/general/members", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("members = %d, want 200", rec.Code)
	}
	var resp struct {
		Members []struct {
			Username string `json:"username"`
			Status   string `json:"status"`
		} `json:"members"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, m := range resp.Members {
		got[m.Username] = m.Status
	}
	if got["online_user"] != "online" || got["away_user"] != "away" || got["offline_user"] != "offline" {
		t.Fatalf("unexpected statuses: %+v", resp.Members)
	}
	if resp.Members[0].Status != "online" || resp.Members[len(resp.Members)-1].Status != "offline" {
		t.Fatalf("expected online-first/offline-last ordering: %+v", resp.Members)
	}
}
```
Append to `internal/gateway/client_test.go`:
```go
func TestJoinTouchesMembership(t *testing.T) {
	reg := &fakeRegistry{}
	ms := newFakeMemberStore()
	cfg := clientConfig{
		hub: reg, history: &fakeHistory{}, presence: newFakePresenceStore(),
		limiter: &fakeRateLimiter{allow: true}, rooms: newFakeRoomStore(), members: ms,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	c := newClient(context.Background(), "tester", cfg, func() {})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	if ms.touchedCount() == 0 {
		t.Fatal("expected join to touch membership")
	}
}
```

- [ ] **Step 6: Client wiring (`client.go`)** — add `members MemberStore` to `clientConfig` (after `rooms`) and to `Client` (after `rooms`); set `members: cfg.members` in `newClient`. Add a helper:
```go
func (c *Client) touchMember(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.members.Touch(ctx, room, c.username, nowMillis()); err != nil {
		c.log.Warn("member touch failed", "room", room, "err", err)
	}
}
```
Call it: in the `TypeJoin` `if !already` block (after `c.addPresence(f.Room)`): `c.touchMember(f.Room)`. In the `TypeLeave` `if was` block (after `c.removePresence(f.Room)`): `c.touchMember(f.Room)`. In `leaveAll`'s loop (after `c.removePresence(room)`): `c.touchMember(room)`. In `heartbeat`'s per-room loop (after the `c.presence.Add(...)` line): `_ = c.members.Touch(ctx, room, c.username, nowMillis())`.
Update the three client_test helpers (`newTestClient`, `newPresenceClient`, `newRateClient`, `newJoinGateClient`) to add `members: newFakeMemberStore(),` to their `clientConfig`.

- [ ] **Step 7: main adapter (`cmd/gateway/main.go`)** — add:
```go
type memberAdapter struct {
	store *persistence.PgxStore
}

func (a memberAdapter) Touch(ctx context.Context, room, username string, lastSeenMs int64) error {
	return a.store.TouchMember(ctx, room, username, lastSeenMs)
}

func (a memberAdapter) List(ctx context.Context, room string) ([]gateway.MemberRecord, error) {
	recs, err := a.store.ListMembers(ctx, room)
	if err != nil {
		return nil, err
	}
	out := make([]gateway.MemberRecord, len(recs))
	for i, r := range recs {
		out[i] = gateway.MemberRecord{Username: r.Username, LastSeenMs: r.LastSeenMs}
	}
	return out, nil
}
```
Change the `srv := gateway.NewServer(...)` call to the `ServerConfig` form with `Members: memberAdapter{store: store}` (the `store` var already exists from F1).

- [ ] **Step 8: Build, vet, test** — `go build ./... && go vet ./...`; `CGO_ENABLED=0 go test ./...` → all green (PG tests skip).

- [ ] **Step 9: Commit** — `git add internal/gateway/ cmd/gateway/main.go && git -c commit.gpgsign=false commit -m "Add membership roster endpoint and last-seen touches"`.

---

## Self-Review Notes

- **Spec coverage:** room_members table + TouchMember/ListMembers (Task 1), MemberStore + away window (Task 2 Step 1), roster endpoint with online/away/offline + ordering (Step 3), client touches on join/heartbeat/leave (Step 6), ServerConfig refactor (Steps 3-4,7), adapter (Step 7), tests (Tasks 1-2). No timestamps leaked (memberView is username+status only).
- **Routing:** the members route uses `{room}` (same param name as `/api/rooms/{room}/messages`) to avoid a chi wildcard-name conflict.
- **Consistency:** `MemberStore{Touch,List}`, `MemberRecord{Username,LastSeenMs}` in both packages; `ServerConfig` fields; `awayWindowMs`/`presenceTTLms`/`nowMillis()` existing gateway symbols. `fakeMemberStore` and `memberAdapter` satisfy `gateway.MemberStore`.
