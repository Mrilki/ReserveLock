package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func (s *Store) ProcessPaymentSucceeded(ctx context.Context, messageID, reservationID, userID string) (bool, error) {
	return s.processPaymentResult(ctx, messageID, reservationID, userID, true)
}

func (s *Store) ProcessPaymentFailed(ctx context.Context, messageID, reservationID, userID string) (bool, error) {
	return s.processPaymentResult(ctx, messageID, reservationID, userID, false)
}

func (s *Store) processPaymentResult(ctx context.Context, messageID, reservationID, userID string, confirm bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}

	if _, exists := s.processedMessages[messageID]; exists {
		return false, nil
	}

	reservation, exists := s.reservations[reservationID]
	if !exists {
		return false, domain.ErrReservationNotFound
	}
	if reservation.UserID != userID {
		return false, domain.ErrReservationUserMismatch
	}

	if confirm &&
		reservation.Status == domain.ReservationStatusPending &&
		reservation.ExpiresAt.After(time.Now().UTC()) {
		if len(reservation.SeatIDs) == 0 {
			return false, fmt.Errorf("reservation %s has no reservation seats", reservationID)
		}

		eventInventory, exists := s.inventory[reservation.EventID]
		if !exists {
			return false, domain.ErrInventoryNotFound
		}

		for _, seatID := range reservation.SeatIDs {
			seat, exists := eventInventory[seatID]
			if !exists {
				return false, domain.ErrSeatNotFound
			}
			if seat.Status != domain.SeatInventoryStatusHeld || seat.ReservationID != reservationID {
				return false, fmt.Errorf(
					"reservation %s has inconsistent inventory for seat %s",
					reservationID,
					seatID,
				)
			}
		}

		for _, seatID := range reservation.SeatIDs {
			seat := eventInventory[seatID]
			seat.Status = domain.SeatInventoryStatusReserved
			seat.HeldUntil = time.Time{}
			eventInventory[seatID] = seat
		}

		reservation.Status = domain.ReservationStatusConfirmed
		s.reservations[reservationID] = reservation
	}

	s.processedMessages[messageID] = struct{}{}

	return true, nil
}
