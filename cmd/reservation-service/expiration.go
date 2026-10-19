package main

import (
	"context"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"go.uber.org/zap"
)

const expirationBatchTimeout = 5 * time.Second

func runExpirationWorker(
	ctx context.Context,
	service *application.Service,
	logger *zap.Logger,
) {
	ticker := time.NewTicker(expirationScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if ctx.Err() != nil {
			return
		}

		cutoff := time.Now().UTC()
		batchCtx, cancel := context.WithTimeout(ctx, expirationBatchTimeout)
		expiredCount, err := service.ExpireReservations(batchCtx, cutoff)
		cancel()

		if ctx.Err() != nil {
			return
		}

		if err != nil {
			logger.Error("failed to expire reservations", zap.Error(err))
		}

		if expiredCount > 0 {
			logger.Info("reservations expired", zap.Int("count", expiredCount))
		}
	}
}
