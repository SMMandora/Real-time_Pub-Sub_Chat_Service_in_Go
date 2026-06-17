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

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/gateway"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	webDir := os.Getenv("WEB_DIR")
	if webDir == "" {
		webDir = "web"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	bus := gateway.NewRedisBus(redisAddr)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := bus.Ping(pingCtx); err != nil {
		pingCancel()
		log.Error("cannot reach redis", "addr", redisAddr, "err", err)
		os.Exit(1)
	}
	pingCancel()

	hub := gateway.NewHub(bus)
	srv := gateway.NewServer(hub, bus, log, webDir)
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
