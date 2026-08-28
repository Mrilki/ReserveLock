package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	admincomfig "github.com/Mrilki/ReserveLock/internal/admin/config"
	adminhttp "github.com/Mrilki/ReserveLock/internal/admin/transport/http"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/Mrilki/ReserveLock/internal/platform/httpx"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"time"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := admincomfig.Load()

	logg, err := logger.New(cfg.Base.AppName, cfg.Base.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	defer func() {
		_ = logg.Sync()
	}()

	conn, err := grpc.NewClient(
		cfg.EventContentGRPCAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		logg.Error("failed to create event content grpc client", zap.Error(err))
		return
	}

	defer func() {
		_ = conn.Close()
	}()

	eventContentClient := eventcontentv1.NewEventContentServiceClient(conn)

	handler := adminhttp.NewHandler(eventContentClient)

	router := gin.New()
	router.Use(gin.Recovery())

	router.GET(
		"/health/live",
		gin.WrapH(httpx.LiveHandler(cfg.Base.AppName)),
	)

	admin := router.Group("/admin")
	{
		admin.POST("/venues", handler.CreateVenue)
	}

	server := &http.Server{
		Addr:              cfg.Base.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)

	go func() {
		logg.Info(
			"admin service started",
			zap.String("http_addr", cfg.Base.HTTPAddr),
			zap.String("event_content_grpc_addr", cfg.EventContentGRPCAddr),
		)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("event_content_grpc_addr")
	case <-serverErr:
		logg.Error("http server failed", zap.Error(err))
		return
	}

	shutdownCtx, cannel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cannel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logg.Error("http server shutdown failed", zap.Error(err))
		return
	}

	logg.Info("admin service stopped")
}
