package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	eventcontentconfig "github.com/Mrilki/ReserveLock/internal/eventcontent/config"
	"github.com/Mrilki/ReserveLock/internal/eventcontent/repository/mongodb"
	eventcontentgrpc "github.com/Mrilki/ReserveLock/internal/eventcontent/transport/grpc"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	shutdownTimeout       = 10 * time.Second
	mongoOperationTimeout = 5 * time.Second
)

func main() {
	cfg := eventcontentconfig.Load()

	logg, err := logger.New(cfg.Base.AppName, cfg.Base.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialize logger, %v", err)
	}

	defer func() {
		_ = logg.Sync()
	}()

	mongoClient, err := mongo.Connect(
		options.Client().ApplyURI(
			cfg.MongoURI,
		),
	)
	if err != nil {
		logg.Error(
			"failed to create mongo client",
			zap.Error(err),
		)
		return
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(
			context.Background(),
			mongoOperationTimeout,
		)
		defer cancel()

		if err := mongoClient.Disconnect(
			disconnectCtx,
		); err != nil {
			logg.Error(
				"failed to disconnect mongo client",
				zap.Error(err),
			)
		}
	}()

	pingCtx, pingCancel := context.WithTimeout(
		context.Background(),
		mongoOperationTimeout,
	)

	err = mongoClient.Ping(pingCtx, readpref.Primary())

	pingCancel()

	if err != nil {
		logg.Error(
			"failed to ping mongo",
			zap.Error(err),
		)

		return
	}

	database := mongoClient.Database(cfg.MongoDatabase)

	venueRepository := mongodb.NewVenueRepository(database)
	hallRepository := mongodb.NewHallRepository(database)
	seatMapRepository := mongodb.NewSeatMapRepository(database)
	eventRepository := mongodb.NewEventRepository(database)

	eventContentService := application.NewService(
		venueRepository,
		hallRepository,
		seatMapRepository,
		eventRepository,
	)

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logg.Error("failed to listen", zap.String("addr", cfg.GRPCAddr), zap.Error(err))
		return
	}

	grpcServer := grpc.NewServer()

	eventcontentv1.RegisterEventContentServiceServer(
		grpcServer,
		eventcontentgrpc.NewServer(eventContentService),
	)
	if cfg.Base.AppEnv == "local" {
		reflection.Register(grpcServer)
		logg.Info("grpc reflection enable")
	}

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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
		logg.Warn("grpc graceful shutdown timed out")
		grpcServer.Stop()
	}
}
