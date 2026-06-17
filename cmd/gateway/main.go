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
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
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

type roomAdapter struct {
	store *persistence.PgxStore
}

func (a roomAdapter) Lookup(ctx context.Context, id string) (gateway.RoomRecord, bool, error) {
	r, found, err := a.store.GetRoom(ctx, id)
	if err != nil || !found {
		return gateway.RoomRecord{}, found, err
	}
	return gateway.RoomRecord{ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, InviteToken: r.InviteToken}, true, nil
}

func (a roomAdapter) Create(ctx context.Context, r gateway.RoomRecord) error {
	err := a.store.CreateRoom(ctx, persistence.RoomRecord{
		ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, InviteToken: r.InviteToken, CreatedMS: time.Now().UnixMilli(),
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
		out[i] = gateway.RoomRecord{ID: r.ID, Name: r.Name, Description: r.Description, Visibility: r.Visibility, InviteToken: r.InviteToken}
	}
	return out, nil
}

func (a roomAdapter) Delete(ctx context.Context, id string) error {
	return a.store.DeleteRoom(ctx, id)
}

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

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	shutdownTracing, err := tracing.Init(context.Background(), "gateway")
	if err != nil {
		log.Error("tracing init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownTracing(sctx)
		scancel()
	}()

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

	store := persistence.NewPgxStore(pool)
	hist := histAdapter{store: store}
	rooms := roomAdapter{store: store}

	hub := gateway.NewHub(bus)
	presence := gateway.NewRedisPresenceStore(redisAddr)
	defer presence.Close()
	limiter := gateway.NewRedisRateLimiter(redisAddr, 30, 0.5)
	defer limiter.Close()
	srv := gateway.NewServer(gateway.ServerConfig{
		Hub: hub, Bus: bus, History: hist, Presence: presence, Limiter: limiter,
		Rooms: rooms, Members: memberAdapter{store: store}, Log: log, WebDir: webDir,
	})
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
