package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	reservationconfig "github.com/Mrilki/ReserveLock/internal/reservation/config"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
	reservationgrpc "github.com/Mrilki/ReserveLock/internal/reservation/transport/grpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	shutdownTimeout        = 10 * time.Second
	expirationScanInterval = time.Second
)

func main() {
	cfg := reservationconfig.Load()

	logg, err := logger.New(
		cfg.Base.AppName,
		cfg.Base.AppEnv,
	)
	if err != nil {
		log.Fatalf(
			"failed to initialize logger: %v",
			err,
		)
	}

	defer func() {
		_ = logg.Sync()
	}()

	listener, err := net.Listen(
		"tcp",
		cfg.GRPCAddr,
	)
	if err != nil {
		logg.Error(
			"failed to listen",
			zap.String("addr", cfg.GRPCAddr),
			zap.Error(err),
		)
		return
	}

	inventoryRepository :=
		memory.NewInventoryRepository()

	reservationRepository :=
		memory.NewReservationRepository()

	reservationService := application.NewService(
		inventoryRepository,
		reservationRepository,
	)

	grpcServer := grpc.NewServer()

	reservationv1.RegisterReservationServiceServer(
		grpcServer,
		reservationgrpc.NewServer(reservationService),
	)

	if cfg.Base.AppEnv == "local" {
		reflection.Register(grpcServer)
		logg.Info("grpc reflection enabled")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		ticker := time.NewTicker(expirationScanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				expiredCount, err := reservationService.ExpireReservations(
					ctx,
					now.UTC(),
				)
				if err != nil {
					logg.Error(
						"failed to expire reservations",
						zap.Error(err),
					)

					continue
				}
				if expiredCount > 0 {
					logg.Info(
						"reservations expired",
						zap.Int(
							"count",
							expiredCount,
						),
					)
				}
			}
		}
	}()

	serverErr := make(chan error, 1)

	go func() {
		logg.Info(
			"grpc server started",
			zap.String("addr", cfg.GRPCAddr),
		)

		if err := grpcServer.Serve(listener); err != nil {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")

	case err := <-serverErr:
		logg.Error(
			"grpc server failed",
			zap.Error(err),
		)
		return
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancel()

	stopped := make(chan struct{})

	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logg.Info("grpc server stopped")

	case <-shutdownCtx.Done():
		logg.Warn(
			"grpc graceful shutdown timed out",
		)
		grpcServer.Stop()
	}
}
