package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventcontentconfig "github.com/Mrilki/ReserveLock/internal/eventcontent/config"
	eventcontentgrpc "github.com/Mrilki/ReserveLock/internal/eventcontent/transport/grpc"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := eventcontentconfig.Load()

	logg, err := logger.New(cfg.Base.AppName, cfg.Base.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialize logger, %v", err)
	}

	defer func() {
		_ = logg.Sync()
	}()

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logg.Error("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
	}

	grpcServer := grpc.NewServer()

	eventcontentv1.RegisterVenueServiceServer(
		grpcServer,
		eventcontentgrpc.NewServer(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)

	go func() {
		logg.Info("grpc server started", zap.String("addr", cfg.GRPCAddr))

		if err := grpcServer.Serve(listener); err != nil {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")
	case err := <-serverErr:
		logg.Error("grpc server failed", zap.Error(err))
		return
	}

	shutdownCtx, cannel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cannel()

	stopped := make(chan struct{})

	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logg.Info("grpc server stopped")
	case <-shutdownCtx.Done():
		logg.Warn("grpc graceful shutdown timed out")
		grpcServer.Stop()
	}
}
