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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/persistence"
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	shutdownTracing, err := tracing.Init(context.Background(), "worker")
	if err != nil {
		log.Error("tracing init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownTracing(sctx)
		scancel()
	}()

	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	dbURL := getenv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")
	workerAddr := getenv("WORKER_ADDR", ":8090")

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Error("cannot create pg pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := persistence.NewPgxStore(pool)
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := store.Migrate(migrateCtx); err != nil {
		migrateCancel()
		log.Error("migrate failed", "err", err)
		os.Exit(1)
	}
	migrateCancel()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	batcher := persistence.NewBatcher(store, 100, 50*time.Millisecond, log)
	go batcher.Run()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	worker := persistence.NewWorker(rdb, batcher, log)
	workerDone := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(workerDone)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(pctx); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	healthSrv := &http.Server{Addr: workerAddr, Handler: mux}
	go func() {
		log.Info("worker health listening", "addr", workerAddr)
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server error", "err", err)
			stop()
		}
	}()

	log.Info("worker started", "redis", redisAddr)
	<-ctx.Done()
	log.Info("worker shutting down")

	<-workerDone   // worker has stopped consuming, so no more Submit calls
	batcher.Close() // flush remaining messages

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = healthSrv.Shutdown(shutdownCtx)
	log.Info("worker stopped")
}
