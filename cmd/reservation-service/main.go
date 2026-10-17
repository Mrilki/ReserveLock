package main

import (
	"context"
	"fmt"
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
	reservationoutbox "github.com/Mrilki/ReserveLock/internal/reservation/outbox"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	reservationredis "github.com/Mrilki/ReserveLock/internal/reservation/repository/redis"
	reservationgrpc "github.com/Mrilki/ReserveLock/internal/reservation/transport/grpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

	postgresPool, err := pgxpool.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		logg.Error("failed to create postgres pool", zap.Error(err))
		exitCode = 1
		return
	}
	defer postgresPool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
	err = postgresPool.Ping(pingCtx)
	pingCancel()

	if err != nil {
		logg.Error("failed to ping postgres", zap.Error(err))
		exitCode = 1
		return
	}

	logg.Info("connected to postgres")

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logg.Error("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
		exitCode = 1
		return
	}

	reservationStore := reservationpostgres.NewStore(postgresPool)

	redisClient, err := reservationredis.NewClient(cfg.RedisURL)
	if err != nil {
		logg.Error("invalid redis configuration", zap.Error(err))
		exitCode = 1
		return
	}
	defer func() {
		_ = redisClient.Close()
	}()

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

	kafkaClient, err := reservationkafka.NewEventPublishedClient(cfg.KafkaBrokers)
	if err != nil {
		logg.Error("failed to create kafka consumer client", zap.Error(err))
		exitCode = 1
		return
	}
	defer kafkaClient.CloseAllowingRebalance()

	eventPublishedConsumer := reservationkafka.NewEventPublishedConsumer(
		kafkaClient,
		reservationService,
		logg,
	)
	paymentKafkaClient, err := reservationkafka.NewPaymentClient(cfg.KafkaBrokers)
	if err != nil {
		logg.Error("failed to create payment kafka consumer client", zap.Error(err))
		exitCode = 1
		return
	}
	defer paymentKafkaClient.CloseAllowingRebalance()

	paymentConsumer := reservationkafka.NewPaymentConsumer(
		paymentKafkaClient,
		reservationService,
		logg,
	)

	publisherClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		logg.Error("failed to create kafka publisher client", zap.Error(err))
		exitCode = 1
		return
	}
	defer publisherClient.Close()

	outboxRepository := reservationpostgres.NewOutboxRepository(postgresPool)
	outboxPublisher := reservationkafka.NewPublisher(publisherClient)
	outboxWorker := reservationoutbox.NewWorker(outboxRepository, outboxPublisher, logg)
	grpcServer := grpc.NewServer()

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	reservationv1.RegisterReservationServiceServer(
		grpcServer,
		reservationgrpc.NewServer(reservationService, logg),
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
	consumerErr := make(chan error, 2)

	go func() {
		defer close(consumerDone)

		if err := eventPublishedConsumer.Run(ctx); err != nil {
			consumerErr <- fmt.Errorf("event published consumer: %w", err)
		}
	}()

	paymentConsumerDone := make(chan struct{})

	go func() {
		defer close(paymentConsumerDone)

		if err := paymentConsumer.Run(ctx); err != nil {
			consumerErr <- fmt.Errorf("payment consumer: %w", err)
		}
	}()

	outboxDone := make(chan struct{})

	go func() {
		defer close(outboxDone)
		outboxWorker.Run(ctx)
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
					logg.Error("failed to expire reservations", zap.Error(err))

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
		exitCode = 1
		stop()
	case err := <-consumerErr:
		logg.Error("kafka consumer failed", zap.Error(err))
		exitCode = 1
		stop()
	}

	healthServer.Shutdown()

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
	select {
	case <-paymentConsumerDone:
		logg.Info("payment consumer stopped")

	case <-shutdownCtx.Done():
		logg.Warn("payment consumer shutdown timed out")
	}

	select {
	case <-outboxDone:
		logg.Info("reservation outbox worker stopped")

	case <-shutdownCtx.Done():
		logg.Warn("reservation outbox worker shutdown timed out")
	}
}
