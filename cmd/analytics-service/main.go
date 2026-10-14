package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	analyticsconfig "github.com/Mrilki/ReserveLock/internal/analytics/config"
	analyticskafka "github.com/Mrilki/ReserveLock/internal/analytics/messaging/kafka"
	analyticsclickhouse "github.com/Mrilki/ReserveLock/internal/analytics/repository/clickhouse"
	analyticsgrpc "github.com/Mrilki/ReserveLock/internal/analytics/transport/grpc"
	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	clickhouseConnectTimeout = 5 * time.Second
	kafkaConnectTimeout      = 5 * time.Second
	shutdownTimeout          = 10 * time.Second
)

func main() {
	cfg := analyticsconfig.Load()

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clickhouseClient, err := analyticsclickhouse.NewClient(cfg.ClickHouseDSN)
	if err != nil {
		logg.Error("failed to create analytics clickhouse client", zap.Error(err))
		exitCode = 1
		return
	}
	defer func() {
		_ = clickhouseClient.Close()
	}()

	pingCtx, pingCancel := context.WithTimeout(ctx, clickhouseConnectTimeout)
	err = clickhouseClient.Ping(pingCtx)
	pingCancel()
	if err != nil {
		logg.Error("failed to ping analytics clickhouse", zap.Error(err))
		exitCode = 1
		return
	}

	kafkaClient, err := analyticskafka.NewClient(cfg.KafkaBrokers)
	if err != nil {
		logg.Error("failed to create analytics kafka client", zap.Error(err))
		exitCode = 1
		return
	}
	defer kafkaClient.CloseAllowingRebalance()

	pingCtx, pingCancel = context.WithTimeout(ctx, kafkaConnectTimeout)
	err = kafkaClient.Ping(pingCtx)
	pingCancel()
	if err != nil {
		logg.Error("failed to ping analytics kafka", zap.Error(err))
		exitCode = 1
		return
	}

	store := analyticsclickhouse.NewStore(clickhouseClient)
	service := application.NewService(store)
	consumer := analyticskafka.NewConsumer(kafkaClient, service, logg)

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logg.Error("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
		exitCode = 1
		return
	}
	defer func() {
		_ = listener.Close()
	}()

	grpcServer := grpc.NewServer()

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, analyticsgrpc.NewServer(service))

	if cfg.Base.AppEnv == "local" {
		reflection.Register(grpcServer)
	}

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		logg.Info("analytics grpc server started", zap.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(listener); err != nil {
			serverErr <- err
		}
	}()

	logg.Info("analytics consumer started")

	consumerStopped := false

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")

	case err := <-consumerErr:
		consumerStopped = true
		if err != nil {
			logg.Error("analytics consumer failed", zap.Error(err))
			exitCode = 1
		}

	case err := <-serverErr:
		logg.Error("analytics grpc server failed", zap.Error(err))
		exitCode = 1
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
		logg.Warn("analytics grpc shutdown timed out")
		exitCode = 1
		grpcServer.Stop()
	}

	if !consumerStopped {
		select {
		case err := <-consumerErr:
			if err != nil {
				logg.Error("analytics consumer shutdown failed", zap.Error(err))
				exitCode = 1
			}

		case <-shutdownCtx.Done():
			logg.Warn("analytics consumer shutdown timed out")
			exitCode = 1
		}
	}

	logg.Info("analytics service stopped")
}
