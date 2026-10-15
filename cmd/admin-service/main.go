package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	adminconfig "github.com/Mrilki/ReserveLock/internal/admin/config"
	adminhttp "github.com/Mrilki/ReserveLock/internal/admin/transport/http"
	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
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
	cfg := adminconfig.Load()

	logg, err := logger.New(cfg.Base.AppName, cfg.Base.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	exitCode := 0

	defer func() {
		_ = logg.Sync()

		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()

	conn, err := grpc.NewClient(
		cfg.EventContentGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logg.Error("failed to create event content grpc client", zap.Error(err))
		exitCode = 1
		return
	}

	defer func() {
		_ = conn.Close()
	}()
	eventContentClient := eventcontentv1.NewEventContentServiceClient(conn)

	analyticsConn, err := grpc.NewClient(
		cfg.AnalyticsGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logg.Error("failed to create analytics grpc client", zap.Error(err))
		exitCode = 1
		return
	}
	defer func() {
		_ = analyticsConn.Close()
	}()
	analyticsClient := analyticsv1.NewAnalyticsServiceClient(analyticsConn)

	handler := adminhttp.NewHandler(eventContentClient, analyticsClient)

	router := gin.New()
	router.Use(gin.Recovery())

	router.GET(
		"/health/live",
		gin.WrapH(httpx.LiveHandler(cfg.Base.AppName)),
	)

	admin := router.Group("/admin")
	{
		admin.POST("/venues", handler.CreateVenue)
		admin.POST("/halls", handler.CreateHall)
		admin.POST("/seat-maps", handler.CreateSeatMap)
		admin.POST("/events", handler.CreateEvent)
		admin.POST("/events/:id/publish", handler.PublishEvent)
		admin.GET("/events/:id/analytics", handler.GetEventSummary)
	}

	server := &http.Server{
		Addr:              cfg.Base.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serverErr := make(chan error, 1)

	go func() {
		logg.Info(
			"admin service started",
			zap.String("http_addr", cfg.Base.HTTPAddr),
			zap.String("event_content_grpc_addr", cfg.EventContentGRPCAddr),
			zap.String("analytics_grpc_addr", cfg.AnalyticsGRPCAddr),
		)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")
	case err := <-serverErr:
		logg.Error("http server failed", zap.Error(err))
		exitCode = 1
		return
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logg.Error("http server shutdown failed", zap.Error(err))
		exitCode = 1
		return
	}

	logg.Info("admin service stopped")
}
