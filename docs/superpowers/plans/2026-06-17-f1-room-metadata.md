# F1 — Room Metadata + Online Count Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rooms carry `name`/`description`, and `GET /api/rooms` / `GET /api/rooms/{id}` return them plus a live per-room `online` count.

**Architecture:** Add columns via idempotent `ALTER TABLE` in `Migrate`; thread `Name`/`Description` through `persistence.RoomRecord` → `gateway.RoomRecord` → `roomView`; compute `online` from the presence store the server already holds. One cohesive commit.

**Tech Stack:** Go 1.22+, existing pgx/chi.

**Commit convention:** Commit locally on `main`. Do NOT push. No Claude/Anthropic attribution. Use `git -c commit.gpgsign=false commit`.

**Disk is nearly full.** If a build/test fails with "no space left on device", run `go clean -cache -testcache` and retry. -race needs the MinGW gcc on PATH:
```
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
```

---

### Task 1: Room metadata + online count

**Files:**
- Modify: `internal/persistence/store_pg.go`, `internal/persistence/rooms.go`, `internal/persistence/rooms_test.go`
- Modify: `internal/gateway/rooms.go`, `internal/gateway/server.go`, `internal/gateway/server_test.go`
- Modify: `cmd/gateway/main.go`

- [ ] **Step 1: Extend the persistence room test**

In `internal/persistence/rooms_test.go`, change the first `CreateRoom` call and the get-assertion to include name/description. Replace:
```go
	if err := store.CreateRoom(ctx, RoomRecord{ID: "team", Visibility: "private", InviteToken: "tok", CreatedMS: 1}); err != nil {
		t.Fatal(err)
	}
```
with:
```go
	if err := store.CreateRoom(ctx, RoomRecord{ID: "team", Name: "Team", Description: "private team room", Visibility: "private", InviteToken: "tok", CreatedMS: 1}); err != nil {
		t.Fatal(err)
	}
```
and after the existing `got, found, err := store.GetRoom(ctx, "team")` block's visibility/token check, add:
```go
	if got.Name != "Team" || got.Description != "private team room" {
		t.Fatalf("expected name/description round-trip, got %+v", got)
	}
```

- [ ] **Step 2: Add a gateway REST test for metadata + online count**

Append to `internal/gateway/server_test.go`:
```go
func TestListRoomsIncludesMetadataAndOnlineCount(t *testing.T) {
	bus := newFakeBus()
	ps := newFakePresenceStore()
	ps.Add(context.Background(), "general", "alice", nowMillis())
	ps.Add(context.Background(), "general", "bob", nowMillis())
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "general", Name: "General", Description: "main room", Visibility: "public"})
	srv := NewServer(NewHub(bus), bus, &fakeHistory{}, ps, &fakeRateLimiter{allow: true}, rooms, slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d, want 200", rec.Code)
	}
	var resp struct {
		Rooms []struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			Description string `json:"description"`
			Visibility  string `json:"visibility"`
			Online      int    `json:"online"`
		} `json:"rooms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Rooms) != 1 {
		t.Fatalf("expected 1 room, got %d", len(resp.Rooms))
	}
	r := resp.Rooms[0]
	if r.Name != "General" || r.Description != "main room" || r.Online != 2 {
		t.Fatalf("unexpected room view: %+v", r)
	}
}
```

- [ ] **Step 3: Run to verify red**

```bash
go test ./internal/gateway/ -run TestListRoomsIncludesMetadata 2>&1 | head -15
```
Expected: FAIL — `RoomRecord` has no `Name`/`Description`, or the response lacks `name`/`online`.

- [ ] **Step 4: Add columns in `Migrate` (`store_pg.go`)**

In `internal/persistence/store_pg.go`, replace `Migrate` with:
```go
func (s *PgxStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, createMessagesTable); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, createRoomsTable); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '';
		 ALTER TABLE rooms ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';`)
	return err
}
```

- [ ] **Step 5: Add fields + columns in `persistence/rooms.go`**

In `internal/persistence/rooms.go`, add to `RoomRecord` (after `ID`):
```go
	Name        string
	Description string
```
Replace `CreateRoom` with:
```go
func (s *PgxStore) CreateRoom(ctx context.Context, r RoomRecord) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO rooms (id, name, description, visibility, invite_token, created_ms)
		 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`,
		r.ID, r.Name, r.Description, r.Visibility, nullIfEmpty(r.InviteToken), r.CreatedMS)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrRoomExists
	}
	return nil
}
```
Replace `GetRoom` with:
```go
func (s *PgxStore) GetRoom(ctx context.Context, id string) (RoomRecord, bool, error) {
	var r RoomRecord
	var token *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, description, visibility, invite_token, created_ms FROM rooms WHERE id=$1`, id).
		Scan(&r.ID, &r.Name, &r.Description, &r.Visibility, &token, &r.CreatedMS)
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
```
Replace `ListRooms` with:
```go
func (s *PgxStore) ListRooms(ctx context.Context) ([]RoomRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, description, visibility, invite_token, created_ms FROM rooms ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var token *string
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Visibility, &token, &r.CreatedMS); err != nil {
			return nil, err
		}
		if token != nil {
			r.InviteToken = *token
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
```

- [ ] **Step 6: Add fields in `gateway/rooms.go`**

In `internal/gateway/rooms.go`, add to `RoomRecord` (after `ID`):
```go
	Name        string
	Description string
```

- [ ] **Step 7: Update `roomView` + handlers (`server.go`)**

In `internal/gateway/server.go`, replace the `roomView` type with:
```go
type roomView struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Visibility  string `json:"visibility"`
	Online      int    `json:"online"`
}

func (s *Server) onlineCount(ctx context.Context, room string) int {
	members, err := s.clientCfg.presence.Snapshot(ctx, room, nowMillis()-presenceTTLms)
	if err != nil {
		return 0
	}
	return len(members)
}

func (s *Server) roomViewOf(ctx context.Context, r RoomRecord) roomView {
	return roomView{ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, Online: s.onlineCount(ctx, r.ID)}
}
```
In `handleCreateRoom`, change the body struct to include name/description and set them on the record. Replace:
```go
	var body struct {
		ID         string `json:"id"`
		Visibility string `json:"visibility"`
	}
```
with:
```go
	var body struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Visibility  string `json:"visibility"`
	}
```
and replace `rec := RoomRecord{ID: body.ID, Visibility: body.Visibility}` with:
```go
	rec := RoomRecord{ID: body.ID, Name: body.Name, Description: body.Description, Visibility: body.Visibility}
```
In `handleListRooms`, replace the loop that builds `out`:
```go
	out := make([]roomView, len(recs))
	for i, rec := range recs {
		out[i] = roomView{ID: rec.ID, Visibility: rec.Visibility}
	}
```
with:
```go
	out := make([]roomView, len(recs))
	for i, rec := range recs {
		out[i] = s.roomViewOf(ctx, rec)
	}
```
In `handleGetRoom`, replace the final encode:
```go
	_ = json.NewEncoder(w).Encode(roomView{ID: rec.ID, Visibility: rec.Visibility})
```
with:
```go
	_ = json.NewEncoder(w).Encode(s.roomViewOf(ctx, rec))
```

- [ ] **Step 8: Map fields in `roomAdapter` (`main.go`)**

In `cmd/gateway/main.go`, in `roomAdapter.Lookup`, `roomAdapter.Create`, and `roomAdapter.List`, carry `Name`/`Description`:
- `Lookup` return: `gateway.RoomRecord{ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, InviteToken: r.InviteToken}`.
- `Create`: `persistence.RoomRecord{ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, InviteToken: r.InviteToken, CreatedMS: time.Now().UnixMilli()}`.
- `List` per-item: `gateway.RoomRecord{ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, InviteToken: r.InviteToken}`.

- [ ] **Step 9: Build, vet, race**

```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./... -race
```
Expected: build+vet clean; all tests `ok` (PG tests skip), no race warnings — incl. the two new metadata tests.

- [ ] **Step 10: Commit**

```bash
git add internal/ cmd/gateway/main.go
git -c commit.gpgsign=false commit -m "Add room name/description and per-room online count"
```

---

## Self-Review Notes

- **Spec coverage:** name/description columns + ALTER migration (Steps 4-5), RoomRecord fields both packages (Steps 5-6), create accepts name/description (Step 7), list/get return name/description + online count via presence (Step 7), adapter maps fields (Step 8), tests (Steps 1-2). Tokens still hidden (roomView has no token field).
- **Consistency:** `roomView{ID,Name,Description,Visibility,Online}`; `RoomRecord` gains `Name`/`Description` in both `persistence` and `gateway`; `onlineCount` uses `presenceTTLms`/`nowMillis()` (existing gateway symbols) and `s.clientCfg.presence`.
- **Online best-effort:** a presence snapshot error yields `online: 0`, never fails the list.
