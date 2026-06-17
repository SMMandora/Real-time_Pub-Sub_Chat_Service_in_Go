package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	hub := NewHub(bus)
	return NewServer(hub, bus, &fakeHistory{}, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")
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

func TestMalformedJSONReturnsError(t *testing.T) {
	srv := newTestServer()
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()
	wsURL := "ws" + ts.URL[len("http"):] + "/ws"

	c, ctx := dialWS(t, wsURL)
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
	hub := NewHub(bus)
	srv := NewServer(hub, bus, &fakeHistory{}, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with redis down = %d, want 503", rec.Code)
	}
}

func TestReadyzFailsWhenPostgresDown(t *testing.T) {
	bus := newFakeBus()
	hist := &fakeHistory{pingErr: errors.New("pg down")}
	srv := NewServer(NewHub(bus), bus, hist, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

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
	srv := NewServer(NewHub(bus), bus, hist, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

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
	srv := NewServer(NewHub(bus), bus, hist, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

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
	srv := NewServer(NewHub(bus), bus, hist, newFakePresenceStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), "web")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rooms/x/messages", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("history with store error = %d, want 503", rec.Code)
	}
}
