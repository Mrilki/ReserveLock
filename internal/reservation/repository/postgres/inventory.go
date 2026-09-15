package postgres

import (
	"context"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func (s *Store) InitializeInventory(
	ctx context.Context,
	eventID string,
	seats []domain.SeatInventory,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin initialize inventory transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	result, err := tx.Exec(ctx, `
		INSERT INTO event_inventories (event_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
	`, eventID)
	if err != nil {
		return fmt.Errorf("insert event inventory: %w", err)
	}

	if result.RowsAffected() == 0 {
		return domain.ErrInventoryAlreadyExists
	}

	seatIDs := make([]string, 0, len(seats))

	for _, seat := range seats {
		seatIDs = append(seatIDs, seat.SeatID)
	}

	result, err = tx.Exec(ctx, `
		INSERT INTO seat_inventory (event_id, seat_id, status)
		SELECT $1, seat_id, 'AVAILABLE'
		FROM unnest($2::text[]) AS seats(seat_id)
	`, eventID, seatIDs)
	if err != nil {
		return fmt.Errorf("insert seat inventory: %w", err)
	}

	if result.RowsAffected() != int64(len(seats)) {
		return fmt.Errorf(
			"insert seat inventory: expected %d rows, inserted %d",
			len(seatIDs),
			result.RowsAffected(),
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit initialize inventory transaction: %w", err)
	}

	return nil
}
