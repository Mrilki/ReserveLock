package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/platform/outbox"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationoutbox "github.com/Mrilki/ReserveLock/internal/reservation/outbox"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func insertReservationOutbox(
	ctx context.Context,
	tx pgx.Tx,
	reservation domain.Reservation,
	eventType string,
	occurredAt time.Time,
) error {
	payload, err := json.Marshal(reservationoutbox.ReservationSnapshot{
		ReservationID: reservation.ID,
		EventID:       reservation.EventID,
		UserID:        reservation.UserID,
		SeatIDs:       reservation.SeatIDs,
		Status:        string(reservation.Status),
		CreatedAt:     reservation.CreatedAt,
		ExpiresAt:     reservation.ExpiresAt,
	})
	if err != nil {
		return fmt.Errorf("encode reservation outbox snapshot: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_messages (
			message_id, event_type, key, payload, occurred_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5)
	`, uuid.NewString(), eventType, reservation.ID, string(payload), occurredAt.UTC())
	if err != nil {
		return fmt.Errorf("insert reservation outbox message: %w", err)
	}

	return nil
}

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) ListPending(ctx context.Context, limit int) ([]outbox.Message, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT message_id, event_type, key, payload, occurred_at
		FROM outbox_messages
		WHERE published_at IS NULL
		ORDER BY occurred_at, message_id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending reservation outbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]outbox.Message, 0)

	for rows.Next() {
		var message outbox.Message

		if err := rows.Scan(
			&message.ID,
			&message.EventType,
			&message.Key,
			&message.Payload,
			&message.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan reservation outbox message: %w", err)
		}

		messages = append(messages, message)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reservation outbox messages: %w", err)
	}

	return messages, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id string, publishedAt time.Time) (bool, error) {
	result, err := r.pool.Exec(ctx, `
		UPDATE outbox_messages
		SET published_at = $2
		WHERE message_id = $1
		  AND published_at IS NULL
	`, id, publishedAt.UTC())
	if err != nil {
		return false, fmt.Errorf("mark reservation outbox message published: %w", err)
	}

	return result.RowsAffected() == 1, nil
}
