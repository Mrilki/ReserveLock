package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	paymentoutbox "github.com/Mrilki/ReserveLock/internal/payment/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func insertPaymentOutbox(
	ctx context.Context,
	tx pgx.Tx,
	payment domain.Payment,
	eventType string,
	occurredAt time.Time,
) error {
	snapshot := paymentoutbox.PaymentSnapshot{
		PaymentID:     payment.ID,
		ReservationID: payment.ReservationID,
		UserID:        payment.UserID,
		Status:        string(payment.Status),
		CreatedAt:     payment.CreatedAt,
		ExpiresAt:     payment.ExpiresAt,
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode payment outbox snapshot: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_messages (
			message_id, event_type, key, payload, occurred_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5)
	`, uuid.NewString(), eventType, payment.ReservationID, string(payload), occurredAt.UTC())
	if err != nil {
		return fmt.Errorf("insert payment outbox message: %w", err)
	}

	return nil
}

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) ListPending(ctx context.Context, limit int) ([]paymentoutbox.Message, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT message_id, event_type, key, payload, occurred_at
		FROM outbox_messages
		WHERE published_at IS NULL
		ORDER BY occurred_at, message_id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending payment outbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]paymentoutbox.Message, 0)
	for rows.Next() {
		var message paymentoutbox.Message
		if err := rows.Scan(
			&message.ID,
			&message.EventType,
			&message.Key,
			&message.Payload,
			&message.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan payment outbox message: %w", err)
		}

		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate payment outbox messages: %w", err)
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
		return false, fmt.Errorf("mark payment outbox message published: %w", err)
	}

	return result.RowsAffected() == 1, nil
}
