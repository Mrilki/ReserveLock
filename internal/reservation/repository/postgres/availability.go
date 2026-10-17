package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func (s *Store) ListSeatInventory(ctx context.Context, eventID string) ([]domain.SeatInventory, error) {
	var inventoryExists bool

	err := s.pool.QueryRow(
		ctx,
		`
			SELECT EXISTS (
				SELECT 1
				FROM event_inventories
				WHERE event_id = $1
			)
		`,
		eventID,
	).Scan(&inventoryExists)
	if err != nil {
		return nil, fmt.Errorf("check event inventory: %w", err)
	}

	if !inventoryExists {
		return nil, domain.ErrInventoryNotFound
	}

	rows, err := s.pool.Query(
		ctx,
		`
			SELECT
				seat_id,
				status,
				COALESCE(reservation_id, ''),
				held_until
			FROM seat_inventory
			WHERE event_id = $1
			ORDER BY seat_id
		`,
		eventID,
	)
	if err != nil {
		return nil, fmt.Errorf("list seat inventory: %w", err)
	}
	defer rows.Close()

	seats := make([]domain.SeatInventory, 0)

	for rows.Next() {
		var (
			seat      domain.SeatInventory
			status    string
			heldUntil *time.Time
		)

		if err := rows.Scan(
			&seat.SeatID,
			&status,
			&seat.ReservationID,
			&heldUntil,
		); err != nil {
			return nil, fmt.Errorf("scan seat inventory: %w", err)
		}

		seat.EventID = eventID
		seat.Status = domain.SeatInventoryStatus(status)

		if heldUntil != nil {
			seat.HeldUntil = heldUntil.UTC()
		}

		seats = append(seats, seat)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate seat inventory: %w", err)
	}

	return seats, nil
}
