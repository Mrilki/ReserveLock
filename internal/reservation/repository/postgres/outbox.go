package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationoutbox "github.com/Mrilki/ReserveLock/internal/reservation/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
