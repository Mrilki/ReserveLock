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
