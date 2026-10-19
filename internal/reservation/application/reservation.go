package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/google/uuid"
)

const (
	reservationHoldDuration = 10 * time.Minute
	expirationBatchSize     = 100
)

type CreateReservationInput struct {
	EventID string
	UserID  string
	SeatIDs []string
}

func (s *Service) GetReservation(ctx context.Context, reservationID string) (domain.Reservation, error) {
	if reservationID == "" {
		return domain.Reservation{}, ErrInvalidReservationID
	}

	return s.store.GetReservationByID(ctx, reservationID)
}

func (s *Service) CreateReservation(ctx context.Context, input CreateReservationInput) (domain.Reservation, error) {
	if input.EventID == "" {
		return domain.Reservation{}, ErrInvalidEventID
	}
	if input.UserID == "" {
		return domain.Reservation{}, ErrInvalidUserID
	}
	if len(input.SeatIDs) == 0 {
		return domain.Reservation{}, ErrSeatIDsRequired
	}

	seen := make(map[string]struct{}, len(input.SeatIDs))

	for _, seatID := range input.SeatIDs {
		if seatID == "" {
			return domain.Reservation{}, ErrInvalidSeatID
		}

		if _, exists := seen[seatID]; exists {
			return domain.Reservation{}, ErrDuplicateSeatID
		}

		seen[seatID] = struct{}{}
	}

	now := time.Now().UTC().Truncate(time.Microsecond)

	reservation := domain.Reservation{
		ID:        uuid.NewString(),
		EventID:   input.EventID,
		UserID:    input.UserID,
		SeatIDs:   append([]string(nil), input.SeatIDs...),
		Status:    domain.ReservationStatusPending,
		CreatedAt: now,
		ExpiresAt: now.Add(reservationHoldDuration),
	}

	err := s.store.CreateReservation(ctx, reservation)
	if err != nil {
		return domain.Reservation{}, err
	}

	return reservation, nil
}

func (s *Service) ExpireReservations(ctx context.Context, cutoff time.Time) (int, error) {
	ids, err := s.store.ListExpiredPendingIDs(ctx, cutoff, expirationBatchSize)
	if err != nil {
		return 0, fmt.Errorf("list expired reservations: %w", err)
	}

	expiredCount := 0
	expirationErrors := make([]error, 0)

	for _, id := range ids {
		expired, err := s.store.ExpireReservation(ctx, id, cutoff)
		if err != nil {
			expirationErrors = append(
				expirationErrors,
				fmt.Errorf("expire reservation %s: %w", id, err),
			)
			continue
		}

		if expired {
			expiredCount++
		}
	}

	return expiredCount, errors.Join(expirationErrors...)
}
