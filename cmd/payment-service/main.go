package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	"github.com/Mrilki/ReserveLock/internal/payment/application"
	paymentconfig "github.com/Mrilki/ReserveLock/internal/payment/config"
	paymentkafka "github.com/Mrilki/ReserveLock/internal/payment/messaging/kafka"
	paymentoutbox "github.com/Mrilki/ReserveLock/internal/payment/outbox"
	paymentpostgres "github.com/Mrilki/ReserveLock/internal/payment/repository/postgres"
	paymentgrpc "github.com/Mrilki/ReserveLock/internal/payment/transport/grpc"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	postgresConnectTimeout = 5 * time.Second
	shutdownTimeout        = 10 * time.Second
)

func main() {
	cfg := paymentconfig.Load()

	logg, err := logger.New(cfg.Base.AppName, cfg.Base.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer func() {
		_ = logg.Sync()
	}()

	pool, err := pgxpool.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		logg.Error("failed to create payment postgres pool", zap.Error(err))
		return
	}
	defer pool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
	err = pool.Ping(pingCtx)
	pingCancel()
	if err != nil {
		logg.Error("failed to ping payment postgres", zap.Error(err))
		return
	}

	client, err := paymentkafka.NewClient(cfg.KafkaBrokers)
	if err != nil {
		logg.Error("failed to create payment kafka client", zap.Error(err))
		return
	}
	defer client.Close()

	publisherClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		logg.Error("failed to create payment kafka publisher client", zap.Error(err))
		return
	}

	defer publisherClient.Close()
	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logg.Error("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
		return
	}
	defer func() {
		_ = listener.Close()
	}()

	service := application.NewService(paymentpostgres.NewStore(pool))
	consumer := paymentkafka.NewConsumer(client, service, logg)

	outboxRepository := paymentpostgres.NewOutboxRepository(pool)
	outboxPublisher := paymentkafka.NewPublisher(publisherClient)
	outboxWorker := paymentoutbox.NewWorker(outboxRepository, outboxPublisher, logg)

	grpcServer := grpc.NewServer()

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	paymentv1.RegisterPaymentServiceServer(grpcServer, paymentgrpc.NewServer(service))

	if cfg.Base.AppEnv == "local" {
		reflection.Register(grpcServer)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consumer.Run(ctx)
	}()

	outboxDone := make(chan struct{})
	go func() {
		defer close(outboxDone)
		outboxWorker.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		logg.Info("payment grpc server started", zap.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(listener); err != nil {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")

	case err := <-serverErr:
		logg.Error("payment grpc server failed", zap.Error(err))
	}
	stop()

	healthServer.Shutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	serverDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(serverDone)
	}()

	select {
	case <-serverDone:
	case <-shutdownCtx.Done():
		logg.Warn("payment grpc shutdown timed out")
		grpcServer.Stop()
	}

	select {
	case <-consumerDone:
		logg.Info("payment consumer stopped")
	case <-shutdownCtx.Done():
		logg.Warn("payment consumer shutdown timed out")
	}

	select {
	case <-outboxDone:
		logg.Info("payment outbox worker stopped")
	case <-shutdownCtx.Done():
		logg.Warn("payment outbox worker shutdown timed out")
	}
}
