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

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

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

	hist := histAdapter{store: persistence.NewPgxStore(pool)}

	hub := gateway.NewHub(bus)
	srv := gateway.NewServer(hub, bus, hist, log, webDir)
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
