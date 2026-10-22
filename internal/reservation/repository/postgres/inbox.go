package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func insertInboxMessage(
	ctx context.Context,
	tx pgx.Tx,
	messageID string,
) (bool, error) {
	result, err := tx.Exec(ctx, `
		INSERT INTO inbox_messages (message_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
	`, messageID)
	if err != nil {
		return false, fmt.Errorf("insert inbox message: %w", err)
	}

	return result.RowsAffected() == 1, nil
}
