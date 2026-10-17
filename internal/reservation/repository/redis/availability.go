package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const availabilityCacheTTL = 2 * time.Second

type AvailabilityReader struct {
	source application.AvailabilityReader
	client *goredis.Client
	logger *zap.Logger
}

type availabilitySnapshot struct {
	ValidUntil time.Time              `json:"validUntil"`
	Seats      []domain.SeatInventory `json:"seats"`
}

func NewAvailabilityReader(
	source application.AvailabilityReader,
	client *goredis.Client,
	logger *zap.Logger,
) *AvailabilityReader {
	return &AvailabilityReader{
		source: source,
		client: client,
		logger: logger,
	}
}

func (r *AvailabilityReader) ListSeatInventory(ctx context.Context, eventID string) ([]domain.SeatInventory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	seats, found, cacheErr := r.readCache(ctx, eventID)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if cacheErr != nil {
		r.logger.Warn("availability cache read failed",
			zap.String("event_id", eventID),
			zap.Error(cacheErr),
		)
	}
	if found {
		return seats, nil
	}

	validUntil := time.Now().UTC().Add(availabilityCacheTTL)

	seats, err := r.source.ListSeatInventory(ctx, eventID)
	if err != nil {
		return nil, err
	}

	if cacheErr == nil && ctx.Err() == nil {
		r.writeCache(ctx, eventID, seats, validUntil)
	}

	return seats, nil
}

func (r *AvailabilityReader) readCache(ctx context.Context, eventID string) ([]domain.SeatInventory, bool, error) {
	cacheCtx, cancel := context.WithTimeout(ctx, redisOperationTimeout)
	defer cancel()

	payload, err := r.client.Get(cacheCtx, availabilityKey(eventID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get availability snapshot: %w", err)
	}
	var snapshot availabilitySnapshot

	if err := json.Unmarshal(payload, &snapshot); err != nil {
		r.logger.Warn("invalid availability cache snapshot",
			zap.String("event_id", eventID),
			zap.Error(err),
		)

		return nil, false, nil
	}

	if !time.Now().Before(snapshot.ValidUntil) {
		return nil, false, nil
	}

	return snapshot.Seats, true, nil
}

func (r *AvailabilityReader) writeCache(
	ctx context.Context,
	eventID string,
	seats []domain.SeatInventory,
	validUntil time.Time,
) {
	payload, err := json.Marshal(availabilitySnapshot{
		ValidUntil: validUntil,
		Seats:      seats,
	})
	if err != nil {
		r.logger.Warn("availability cache encoding failed",
			zap.String("event_id", eventID),
			zap.Error(err),
		)
		return
	}

	ttl := time.Until(validUntil)
	if ttl <= 0 {
		return
	}

	cacheCtx, cancel := context.WithTimeout(ctx, redisOperationTimeout)
	defer cancel()

	if err := r.client.Set(cacheCtx, availabilityKey(eventID), payload, ttl).Err(); err != nil && ctx.Err() == nil {
		r.logger.Warn("availability cache write failed",
			zap.String("event_id", eventID),
			zap.Error(err),
		)
	}
}

func availabilityKey(eventID string) string {
	return "reservelock:reservation:availability:v1:" + eventID
}
