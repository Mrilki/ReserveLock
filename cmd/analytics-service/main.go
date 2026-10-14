package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	analyticsconfig "github.com/Mrilki/ReserveLock/internal/analytics/config"
	analyticskafka "github.com/Mrilki/ReserveLock/internal/analytics/messaging/kafka"
	analyticsclickhouse "github.com/Mrilki/ReserveLock/internal/analytics/repository/clickhouse"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"go.uber.org/zap"
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

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Run(ctx)
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
	}
	stop()

	if consumerStopped {
		logg.Info("analytics consumer stopped")
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	select {
	case err := <-consumerErr:
		if err != nil {
			logg.Error("analytics consumer shutdown failed", zap.Error(err))
			exitCode = 1
		} else {
			logg.Info("analytics consumer stopped")
		}

	case <-shutdownCtx.Done():
		logg.Warn("analytics consumer shutdown timed out")
		exitCode = 1
	}
}
