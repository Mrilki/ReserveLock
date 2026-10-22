package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) ProcessPaymentSucceeded(ctx context.Context, messageID, reservationID, userID string) (bool, error) {
	return s.processPaymentResult(ctx, messageID, reservationID, userID, true)
}

func (s *Store) ProcessPaymentFailed(ctx context.Context, messageID, reservationID, userID string) (bool, error) {
	return s.processPaymentResult(ctx, messageID, reservationID, userID, false)
}

func (s *Store) processPaymentResult(ctx context.Context, messageID, reservationID, userID string, confirm bool) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin payment result transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	inserted, err := insertInboxMessage(ctx, tx, messageID)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}

	reservation := domain.Reservation{ID: reservationID}
	var status string

	err = tx.QueryRow(ctx, `
		SELECT
			r.event_id,
			r.user_id,
			ARRAY(
				SELECT seats.seat_id
				FROM reservation_seats AS seats
				WHERE seats.reservation_id = r.id
				ORDER BY seats.position
			),
			r.status,
			r.created_at,
			r.expires_at
		FROM reservations AS r
		WHERE r.id = $1
		FOR UPDATE OF r
	`, reservationID).Scan(
		&reservation.EventID,
		&reservation.UserID,
		&reservation.SeatIDs,
		&status,
		&reservation.CreatedAt,
		&reservation.ExpiresAt,
	)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, domain.ErrReservationNotFound
	case err != nil:
		return false, fmt.Errorf("lock payment reservation: %w", err)
	}

	reservation.Status = domain.ReservationStatus(status)

	if reservation.UserID != userID {
		return false, domain.ErrReservationUserMismatch
	}

	now := s.now().UTC().Truncate(time.Microsecond)

	if confirm &&
		reservation.Status == domain.ReservationStatusPending &&
		reservation.ExpiresAt.After(now) {
		expectedSeats := int64(len(reservation.SeatIDs))
		if expectedSeats == 0 {
			return false, fmt.Errorf("reservation %s has no reservation seats", reservationID)
		}

		result, err := tx.Exec(ctx, `
			UPDATE seat_inventory AS inventory
			SET
				status = 'RESERVED',
				held_until = NULL
			FROM reservation_seats AS seats
			WHERE seats.reservation_id = $1
			  AND inventory.event_id = $2
			  AND inventory.seat_id = seats.seat_id
			  AND inventory.status = 'HELD'
			  AND inventory.reservation_id = $1
		`, reservationID, reservation.EventID)
		if err != nil {
			return false, fmt.Errorf("reserve paid reservation seats: %w", err)
		}
		if result.RowsAffected() != expectedSeats {
			return false, fmt.Errorf(
				"reserve paid reservation seats: expected %d rows, updated %d",
				expectedSeats,
				result.RowsAffected(),
			)
		}

		result, err = tx.Exec(ctx, `
			UPDATE reservations
			SET status = 'CONFIRMED'
			WHERE id = $1
			  AND status = 'PENDING'
		`, reservationID)
		if err != nil {
			return false, fmt.Errorf("confirm paid reservation: %w", err)
		}
		if result.RowsAffected() != 1 {
			return false, fmt.Errorf("confirm paid reservation: expected 1 row, updated %d", result.RowsAffected())
		}

		reservation.Status = domain.ReservationStatusConfirmed

		if err := insertReservationOutbox(ctx, tx, reservation, reservationcontract.ReservationConfirmedType, now); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit payment result transaction: %w", err)
	}

	return true, nil
}
