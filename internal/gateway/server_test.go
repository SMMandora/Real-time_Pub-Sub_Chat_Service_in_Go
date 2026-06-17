package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func TestParseLimitClampsAndDefaults(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{"", 100},
		{"abc", 100},
		{"0", 100},
		{"-5", 100},
		{"50", 50},
		{"200", 200},
		{"500", 200},
	}
	for _, tt := range tests {
		if got := parseLimit(tt.in); got != tt.want {
			t.Errorf("parseLimit(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func newTestServer() *Server {
	bus := newFakeBus()
	return NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{},
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})
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
	srv := newTestServer()
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()
	wsURL := "ws" + ts.URL[len("http"):] + "/ws"

	a, actx := dialWS(t, wsURL+"?username=alice")
	defer a.Close(websocket.StatusNormalClosure, "")
	b, bctx := dialWS(t, wsURL+"?username=bob")
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

func TestMalformedJSONReturnsError(t *testing.T) {
	srv := newTestServer()
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()
	wsURL := "ws" + ts.URL[len("http"):] + "/ws"

	c, ctx := dialWS(t, wsURL+"?username=carol")
	defer c.Close(websocket.StatusNormalClosure, "")

	if err := c.Write(ctx, websocket.MessageText, []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	f := readUntil(t, ctx, c, TypeError)
	if f.Message == "" {
		t.Fatalf("expected non-empty error message, got %+v", f)
	}
}

func TestReadyzFailsWhenRedisDown(t *testing.T) {
	bus := newFakeBus()
	bus.pingErr = errors.New("redis down")
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{},
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with redis down = %d, want 503", rec.Code)
	}
}

func TestReadyzFailsWhenPostgresDown(t *testing.T) {
	bus := newFakeBus()
	hist := &fakeHistory{pingErr: errors.New("pg down")}
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: hist,
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

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
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: hist,
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

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
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: hist,
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

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
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: hist,
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/x/messages", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("history with store error = %d, want 503", rec.Code)
	}
}

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

func newRoomServer(rooms RoomStore) *Server {
	bus := newFakeBus()
	return NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{},
		Presence: newFakePresenceStore(), Limiter: &fakeRateLimiter{allow: true},
		Rooms: rooms, Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})
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

func TestGetRoomReturnsMetadataWithoutToken(t *testing.T) {
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "secret", Visibility: "private", InviteToken: "tok"})
	srv := newRoomServer(rooms)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/secret", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get existing = %d, want 200", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "tok") {
		t.Fatalf("get must not expose the invite token: %s", rec.Body.String())
	}
	var v struct {
		ID         string `json:"id"`
		Visibility string `json:"visibility"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.ID != "secret" || v.Visibility != "private" {
		t.Fatalf("unexpected room metadata: %+v", v)
	}
}

func TestListRoomsIncludesMetadataAndOnlineCount(t *testing.T) {
	bus := newFakeBus()
	ps := newFakePresenceStore()
	ps.Add(context.Background(), "general", "alice", nowMillis())
	ps.Add(context.Background(), "general", "bob", nowMillis())
	rooms := newFakeRoomStore()
	rooms.put(RoomRecord{ID: "general", Name: "General", Description: "main room", Visibility: "public"})
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{}, Presence: ps,
		Limiter: &fakeRateLimiter{allow: true}, Rooms: rooms, Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web",
	})

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

func TestMetricsQueryProxiesToPrometheus(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("upstream path = %s, want /api/v1/query", r.URL.Path)
		}
		if r.URL.Query().Get("query") != "up" {
			t.Errorf("query not forwarded: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}))
	defer prom.Close()

	bus := newFakeBus()
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{}, Presence: newFakePresenceStore(),
		Limiter: &fakeRateLimiter{allow: true}, Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web", PrometheusURL: prom.URL,
	})

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/metrics/query?query=up", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("proxy = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "success") {
		t.Fatalf("expected upstream body, got %s", rec.Body.String())
	}
}

func TestMetricsQueryUnconfigured503(t *testing.T) {
	srv := newTestServer() // no PrometheusURL
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/metrics/query?query=up", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured proxy = %d, want 503", rec.Code)
	}
}

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
