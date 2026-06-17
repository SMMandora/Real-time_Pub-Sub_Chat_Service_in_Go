package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
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
