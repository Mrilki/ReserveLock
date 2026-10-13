package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
)

type Store struct {
	client driver.Conn
}

func NewStore(client driver.Conn) *Store {
	return &Store{client: client}
}

func (s *Store) InsertReservationEvents(ctx context.Context, events []domain.ReservationEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := s.client.PrepareBatch(ctx, `
		INSERT INTO reservation_events (
			message_id,
			event_id,
			reservation_id,
			event_type,
			seat_ids,
			created_at,
			expires_at,
			occurred_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare reservation analytics batch: %w", err)
	}
	defer func() {
		_ = batch.Close()
	}()

	for _, event := range events {
		if err := batch.Append(
			event.MessageID,
			event.EventID,
			event.ReservationID,
			string(event.EventType),
			event.SeatIDs,
			event.CreatedAt.UTC(),
			event.ExpiresAt.UTC(),
			event.OccurredAt.UTC(),
		); err != nil {
			return fmt.Errorf("append reservation analytics message %q: %w", event.MessageID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send reservation analytics batch: %w", err)
	}

	return nil
}

func (s *Store) GetEventSummary(ctx context.Context, eventID string) (domain.EventSummary, error) {
	summary := domain.EventSummary{EventID: eventID}

	err := s.client.QueryRow(ctx, `
		SELECT
			countIf(event_type = 'CREATED'),
			countIf(event_type = 'CONFIRMED'),
			countIf(event_type = 'EXPIRED'),
			sumIf(length(seat_ids), event_type = 'CREATED'),
			sumIf(length(seat_ids), event_type = 'CONFIRMED'),
			sumIf(length(seat_ids), event_type = 'EXPIRED')
		FROM reservation_events FINAL
		WHERE event_id = ?
	`, eventID).Scan(
		&summary.Created,
		&summary.Confirmed,
		&summary.Expired,
		&summary.CreatedSeats,
		&summary.ConfirmedSeats,
		&summary.ExpiredSeats,
	)
	if err != nil {
		return domain.EventSummary{}, fmt.Errorf("query event analytics summary: %w", err)
	}

	return summary, nil
}
