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

func (s *Store) CreateReservation(ctx context.Context, reservation domain.Reservation) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin create reservation transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var inventoryExists bool

	err = tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_inventories
			WHERE event_id = $1
		)
	`, reservation.EventID).Scan(&inventoryExists)
	if err != nil {
		return fmt.Errorf("check event inventory: %w", err)
	}

	if !inventoryExists {
		return domain.ErrInventoryNotFound
	}

	rows, err := tx.Query(ctx, `
		SELECT seat_id, status
		FROM seat_inventory
		WHERE event_id = $1
		  AND seat_id = ANY($2::text[])
		ORDER BY seat_id
		FOR UPDATE
	`, reservation.EventID, reservation.SeatIDs)
	if err != nil {
		return fmt.Errorf("lock requested seats: %w", err)
	}

	foundSeats := 0
	hasUnavailableSeat := false

	for rows.Next() {
		var seatID string
		var status string

		if err := rows.Scan(&seatID, &status); err != nil {
			rows.Close()
			return fmt.Errorf("scan requested seat: %w", err)
		}

		foundSeats++

		if status != string(domain.SeatInventoryStatusAvailable) {
			hasUnavailableSeat = true
		}
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate requested seats: %w", err)
	}

	rows.Close()

	if foundSeats != len(reservation.SeatIDs) {
		return domain.ErrSeatNotFound
	}

	if hasUnavailableSeat {
		return domain.ErrSeatUnavailable
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO reservations (
			id,
			event_id,
			user_id,
			status,
			created_at,
			expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`,
		reservation.ID,
		reservation.EventID,
		reservation.UserID,
		string(reservation.Status),
		reservation.CreatedAt,
		reservation.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("insert reservation: %w", err)
	}

	result, err := tx.Exec(ctx, `
		INSERT INTO reservation_seats (
			reservation_id,
			seat_id,
			position
		)
		SELECT
			$1,
			seat_id,
			position
		FROM unnest($2::text[]) 
			WITH ORDINALITY AS seats(seat_id, position)
	`, reservation.ID, reservation.SeatIDs)
	if err != nil {
		return fmt.Errorf("insert reservation seats: %w", err)
	}
	if result.RowsAffected() != int64(len(reservation.SeatIDs)) {
		return fmt.Errorf(
			"insert reservation seats: expected %d rows, inserted %d",
			len(reservation.SeatIDs),
			result.RowsAffected(),
		)
	}

	result, err = tx.Exec(ctx, `
		UPDATE seat_inventory
		SET
			status = 'HELD',
			reservation_id = $3,
			held_until = $4
		WHERE event_id = $1
		  AND seat_id = ANY($2::text[])
		  AND status = 'AVAILABLE'
	`,
		reservation.EventID,
		reservation.SeatIDs,
		reservation.ID,
		reservation.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("hold reservation seats: %w", err)
	}

	if result.RowsAffected() != int64(len(reservation.SeatIDs)) {
		return fmt.Errorf(
			"hold reservation seats: expected %d rows, updated %d",
			len(reservation.SeatIDs),
			result.RowsAffected(),
		)
	}

	if err := insertReservationOutbox(ctx, tx, reservation, reservationcontract.ReservationCreatedType, reservation.CreatedAt); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create reservation transaction: %w", err)
	}

	return nil
}

func (s *Store) GetReservationByID(ctx context.Context, id string) (domain.Reservation, error) {
	var reservation domain.Reservation
	var status string

	err := s.pool.QueryRow(ctx, `
		SELECT
			r.id,
			r.event_id,
			r.user_id,
			ARRAY(
				SELECT rs.seat_id
				FROM reservation_seats AS rs
				WHERE rs.reservation_id = r.id
				ORDER BY rs.position
			),
			r.status,
			r.created_at,
			r.expires_at
		FROM reservations AS r
		WHERE r.id = $1
	`, id).Scan(
		&reservation.ID,
		&reservation.EventID,
		&reservation.UserID,
		&reservation.SeatIDs,
		&status,
		&reservation.CreatedAt,
		&reservation.ExpiresAt,
	)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.Reservation{}, domain.ErrReservationNotFound

	case err != nil:
		return domain.Reservation{}, fmt.Errorf("find reservation by id: %w", err)
	}

	reservation.Status = domain.ReservationStatus(status)

	return reservation, nil
}

func (s *Store) ListExpiredPendingIDs(ctx context.Context, now time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id
		FROM reservations
		WHERE status = 'PENDING'
		  AND expires_at <= $1
		ORDER BY expires_at, id
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("find expired reservations: %w", err)
	}
	defer rows.Close()

	ids := make([]string, 0)

	for rows.Next() {
		var id string

		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan expired reservation id: %w", err)
		}

		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired reservations: %w", err)
	}

	return ids, nil
}

func (s *Store) ExpireReservation(ctx context.Context, id string, now time.Time) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin expire reservation transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	reservation := domain.Reservation{
		ID:     id,
		Status: domain.ReservationStatusPending,
	}

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
			r.created_at,
			r.expires_at
		FROM reservations AS r
		WHERE r.id = $1
		  AND r.status = 'PENDING'
		  AND r.expires_at <= $2
		FOR UPDATE OF r
	`, id, now).Scan(
		&reservation.EventID,
		&reservation.UserID,
		&reservation.SeatIDs,
		&reservation.CreatedAt,
		&reservation.ExpiresAt,
	)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("lock expired reservation: %w", err)
	}

	expectedSeats := int64(len(reservation.SeatIDs))
	if expectedSeats == 0 {
		return false, fmt.Errorf("reservation %s has no reservation seats", id)
	}

	result, err := tx.Exec(ctx, `
		UPDATE seat_inventory AS inventory
		SET
			status = 'AVAILABLE',
			reservation_id = NULL,
			held_until = NULL
		FROM reservation_seats AS seats
		WHERE seats.reservation_id = $1
		  AND inventory.event_id = $2
		  AND inventory.seat_id = seats.seat_id
		  AND inventory.status = 'HELD'
		  AND inventory.reservation_id = $1
	`, id, reservation.EventID)
	if err != nil {
		return false, fmt.Errorf("release expired reservation seats: %w", err)
	}
	if result.RowsAffected() != expectedSeats {
		return false, fmt.Errorf(
			"release expired reservation seats: expected %d rows, updated %d",
			expectedSeats,
			result.RowsAffected(),
		)
	}

	result, err = tx.Exec(ctx, `
		UPDATE reservations
		SET status = 'EXPIRED'
		WHERE id = $1
		  AND status = 'PENDING'
	`, id)
	if err != nil {
		return false, fmt.Errorf("mark reservation expired: %w", err)
	}

	if result.RowsAffected() != 1 {
		return false, fmt.Errorf("mark reservation expired: expected 1 row, updated %d", result.RowsAffected())
	}
	reservation.Status = domain.ReservationStatusExpired

	if err := insertReservationOutbox(ctx, tx, reservation, reservationcontract.ReservationExpiredType, now); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit expire reservation transaction: %w", err)
	}

	return true, nil
}
