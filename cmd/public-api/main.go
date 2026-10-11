package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	"github.com/Mrilki/ReserveLock/internal/platform/httpx"
	"github.com/Mrilki/ReserveLock/internal/platform/logger"
	publicapi "github.com/Mrilki/ReserveLock/internal/publicapi/config"
	publichttp "github.com/Mrilki/ReserveLock/internal/publicapi/transport/http"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := publicapi.Load()

	logg, err := logger.New(cfg.Base.AppName, cfg.Base.AppEnv)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	defer func() {
		_ = logg.Sync()
	}()

	eventContentConn, err := grpc.NewClient(
		cfg.EventContentGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logg.Error(
			"failed to create event content grpc client", zap.Error(err))
		return
	}
	defer func() {
		_ = eventContentConn.Close()
	}()

	reservationConn, err := grpc.NewClient(
		cfg.ReservationGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logg.Error(
			"failed to create reservation grpc client", zap.Error(err))
		return
	}
	defer func() {
		_ = reservationConn.Close()
	}()

	paymentConn, err := grpc.NewClient(
		cfg.PaymentGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logg.Error("failed to create payment grpc client", zap.Error(err))
		return
	}
	defer func() {
		_ = paymentConn.Close()
	}()

	eventContentClient := eventcontentv1.NewEventContentServiceClient(eventContentConn)
	reservationConnClient := reservationv1.NewReservationServiceClient(reservationConn)
	paymentConnClient := paymentv1.NewPaymentServiceClient(paymentConn)

	handler := publichttp.NewHandler(
		eventContentClient,
		reservationConnClient,
		paymentConnClient,
	)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health/live", httpx.LiveHandler(cfg.Base.AppName))

	mux.HandleFunc("GET /events", handler.ListEvents)
	mux.HandleFunc("GET /events/{id}", handler.GetEvent)

	mux.HandleFunc("GET /events/{id}/availability", handler.GetAvailability)

	mux.HandleFunc("POST /events/{id}/reservations", handler.CreateReservation)

	mux.HandleFunc("GET /reservations/{id}/payment", handler.GetPayment)
	mux.HandleFunc("POST /reservations/{id}/payment/simulate", handler.SimulatePayment)

	server := &http.Server{
		Addr:              cfg.Base.HTTPAddr,
		Handler:           mux,
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
		logg.Info("service started", zap.String("http_addr", cfg.Base.HTTPAddr))

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logg.Info("shutdown signal received")
	case err := <-serverErr:
		logg.Error("http server failed", zap.Error(err))
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logg.Error("http server shutdown failed", zap.Error(err))
		return
	}

	logg.Info("http server stopped")
}
