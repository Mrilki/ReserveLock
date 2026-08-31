package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/google/uuid"
)

const reservationHoldDuration = 10 * time.Minute

type CreateReservationInput struct {
	EventID string
	UserID  string
	SeatIDs []string
}

func (s *Service) GetReservation(
	ctx context.Context,
	reservationID string,
) (domain.Reservation, error) {
	if reservationID == "" {
		return domain.Reservation{}, ErrInvalidReservationID
	}

	return s.reservationRepository.GetByID(
		ctx,
		reservationID,
	)
}

func (s *Service) CreateReservation(
	ctx context.Context,
	input CreateReservationInput,
) (domain.Reservation, error) {
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

	now := time.Now().UTC()

	reservation := domain.Reservation{
		ID:        uuid.NewString(),
		EventID:   input.EventID,
		UserID:    input.UserID,
		SeatIDs:   append([]string(nil), input.SeatIDs...),
		Status:    domain.ReservationStatusPending,
		CreatedAt: now,
		ExpiresAt: now.Add(reservationHoldDuration),
	}

	err := s.inventoryRepository.Hold(
		ctx,
		reservation.EventID,
		reservation.SeatIDs,
		reservation.ID,
		reservation.ExpiresAt,
	)
	if err != nil {
		return domain.Reservation{}, err
	}

	err = s.reservationRepository.Save(ctx, reservation)
	if err != nil {
		releaseErr := s.inventoryRepository.Release(
			ctx,
			reservation.EventID,
			reservation.SeatIDs,
			reservation.ID,
		)

		if releaseErr != nil {
			return domain.Reservation{}, errors.Join(
				fmt.Errorf("save reservation: %w", err),
				fmt.Errorf("rollback seat hold: %w", releaseErr),
			)
		}

		return domain.Reservation{}, fmt.Errorf(
			"save reservation: %w",
			err,
		)
	}

	return reservation, nil
}
