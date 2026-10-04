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
	reservationkafka "github.com/Mrilki/ReserveLock/internal/reservation/messaging/kafka"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	reservationredis "github.com/Mrilki/ReserveLock/internal/reservation/repository/redis"
	reservationgrpc "github.com/Mrilki/ReserveLock/internal/reservation/transport/grpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	shutdownTimeout        = 10 * time.Second
	expirationScanInterval = time.Second
	postgresConnectTimeout = 5 * time.Second
	redisConnectTimeout    = time.Second
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

	postgresPool, err := pgxpool.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		logg.Error("failed to create postgres pool", zap.Error(err))
		return
	}
	defer postgresPool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
	err = postgresPool.Ping(pingCtx)
	pingCancel()

	if err != nil {
		logg.Error("failed to ping postgres", zap.Error(err))
		return
	}

	logg.Info("connected to postgres")

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logg.Error(
			"failed to listen",
			zap.String("addr", cfg.GRPCAddr),
			zap.Error(err),
		)
		return
	}

	reservationStore := reservationpostgres.NewStore(postgresPool)

	redisClient, err := reservationredis.NewClient(cfg.RedisURL)
	if err != nil {
		logg.Error("invalid redis configuration", zap.Error(err))
		return
	}
	defer redisClient.Close()

	redisCtx, redisCancel := context.WithTimeout(context.Background(), redisConnectTimeout)
	err = redisClient.Ping(redisCtx).Err()
	redisCancel()

	if err != nil {
		logg.Warn("redis unavailable, availability will use postgres", zap.Error(err))
	} else {
		logg.Info("connected to redis")
	}

	availabilityReader := reservationredis.NewAvailabilityReader(reservationStore, redisClient, logg)

	reservationService := application.NewService(reservationStore, availabilityReader)

	kafkaClient, err := reservationkafka.NewClient(cfg.KafkaBrokers)
	if err != nil {
		logg.Error(
			"failed to create kafka consumer client",
			zap.Error(err),
		)
		return
	}
	defer kafkaClient.Close()

	eventPublishedConsumer := reservationkafka.NewConsumer(
		kafkaClient,
		reservationService,
		logg,
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

	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)
		eventPublishedConsumer.Run(ctx)
	}()

	go func() {
		ticker := time.NewTicker(expirationScanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				expiredCount, err := reservationService.ExpireReservations(ctx, now.UTC())
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
						zap.Int("count", expiredCount),
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
		stop()
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

	select {
	case <-consumerDone:
		logg.Info("event published consumer stopped")

	case <-shutdownCtx.Done():
		logg.Warn("event published consumer shutdown timed out")
	}
}
