package postgres

import (
	"context"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func (s *Store) ProcessEventPublished(
	ctx context.Context,
	messageID string,
	eventID string,
	seats []domain.SeatInventory,
) (bool, error) {
	seatIDs := make([]string, 0, len(seats))

	for _, seat := range seats {
		seatIDs = append(seatIDs, seat.SeatID)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin event published transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	result, err := tx.Exec(ctx, `
		INSERT INTO inbox_messages (message_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
	`, messageID)
	if err != nil {
		return false, fmt.Errorf("insert inbox message: %w", err)
	}

	if result.RowsAffected() == 0 {
		return false, nil
	}

	result, err = tx.Exec(ctx, `
		INSERT INTO event_inventories (event_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
	`, eventID)
	if err != nil {
		return false, fmt.Errorf("insert event inventory: %w", err)
	}

	if result.RowsAffected() == 0 {
		return false, domain.ErrInventoryAlreadyExists
	}

	result, err = tx.Exec(ctx, `
		INSERT INTO seat_inventory (event_id, seat_id, status)
		SELECT $1, seat_id, 'AVAILABLE'
		FROM unnest($2::text[]) AS seats(seat_id)
	`, eventID, seatIDs)
	if err != nil {
		return false, fmt.Errorf("insert seat inventory: %w", err)
	}

	if result.RowsAffected() != int64(len(seatIDs)) {
		return false, fmt.Errorf(
			"insert seat inventory: expected %d rows, inserted %d",
			len(seatIDs),
			result.RowsAffected(),
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit event published transaction: %w", err)
	}

	return true, nil
}
