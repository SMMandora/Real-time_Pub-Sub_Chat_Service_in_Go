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
