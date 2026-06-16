package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Mrilki/ReserveLock/internal/platform/config"
	"github.com/Mrilki/ReserveLock/internal/platform/httpx"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"go.uber.org/zap"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := config.LoadBase("public-api")

	logg, err := logger.New(cfg.AppName, cfg.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialization logger: %v", err)
	}

	defer func() {
		_ = logg.Sync()
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", httpx.LiveHandler(cfg.AppName))

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	serverErr := make(chan error, 1)

	go func() {
		logg.Info("service started", zap.String("http_addr", cfg.HTTPAddr))

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")
	case err := <-serverErr:
		logg.Error("http server failed", zap.Error(err))
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logg.Error("http server shutdown failed", zap.Error(err))
		return
	}

	logg.Info("http server stopped")
}
