package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
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

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

func validUsername(s string) bool { return usernamePattern.MatchString(s) }

type Server struct {
	hub       *Hub
	bus       pinger
	hist      history
	rooms     RoomStore
	members   MemberStore
	log       *slog.Logger
	webDir    string
	clientCfg clientConfig
	draining  atomic.Bool
}

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

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/ws", s.handleWS)
	r.Get("/api/rooms/{room}/messages", s.handleHistory)
	r.Post("/api/rooms", s.handleCreateRoom)
	r.Get("/api/rooms", s.handleListRooms)
	r.Get("/api/rooms/{id}", s.handleGetRoom)
	r.Get("/api/rooms/{room}/members", s.handleRoomMembers)
	r.Delete("/api/rooms/{id}", s.handleDeleteRoom)
	r.Handle("/metrics", metricsHandler())
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

func (s *Server) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Visibility  string `json:"visibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.ID == "" || (body.Visibility != "public" && body.Visibility != "private") {
		http.Error(w, "id required; visibility must be public or private", http.StatusBadRequest)
		return
	}
	rec := RoomRecord{ID: body.ID, Name: body.Name, Description: body.Description, Visibility: body.Visibility}
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
		out[i] = s.roomViewOf(ctx, rec)
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
	_ = json.NewEncoder(w).Encode(s.roomViewOf(ctx, rec))
}

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

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if !validUsername(username) {
		http.Error(w, "invalid username", http.StatusBadRequest)
		return
	}

	// InsecureSkipVerify allows the local demo page to connect during dev.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		s.log.Warn("ws accept failed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	client := newClient(ctx, username, s.clientCfg, cancel)
	s.hub.Register(client)
	ActiveConnections.Inc()
	s.log.Info("client connected", "id", client.ID(), "user", username)
	go client.heartbeat()

	defer func() {
		ActiveConnections.Dec()
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
