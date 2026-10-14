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
	summary := domain.EventSummary{
		EventID: eventID,
		Daily:   make([]domain.EventDailySummary, 0),
	}

	rows, err := s.client.Query(ctx, `
		SELECT
			toString(toDate(created_at)) AS day,
			countIf(event_type = 'CREATED'),
			countIf(event_type = 'CONFIRMED'),
			countIf(event_type = 'EXPIRED'),
			sumIf(length(seat_ids), event_type = 'CREATED'),
			sumIf(length(seat_ids), event_type = 'CONFIRMED'),
			sumIf(length(seat_ids), event_type = 'EXPIRED'),
			sumIf(
				(toUnixTimestamp64Micro(occurred_at) - toUnixTimestamp64Micro(created_at)) / 1000000.0,
				event_type = 'CONFIRMED'
			),
			sumIf(
				(toUnixTimestamp64Micro(occurred_at) - toUnixTimestamp64Micro(expires_at)) / 1000000.0,
				event_type = 'EXPIRED'
			)
		FROM reservation_events FINAL
		WHERE event_id = ?
		GROUP BY day
		ORDER BY day
	`, eventID)
	if err != nil {
		return domain.EventSummary{}, fmt.Errorf("query event analytics summary: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var confirmationSeconds float64
	var expirationLagSeconds float64

	for rows.Next() {
		var daily domain.EventDailySummary
		var dailyConfirmationSeconds float64
		var dailyExpirationLagSeconds float64
		if err := rows.Scan(
			&daily.Date,
			&daily.Created,
			&daily.Confirmed,
			&daily.Expired,
			&daily.CreatedSeats,
			&daily.ConfirmedSeats,
			&daily.ExpiredSeats,
			&dailyConfirmationSeconds,
			&dailyExpirationLagSeconds,
		); err != nil {
			return domain.EventSummary{}, fmt.Errorf("scan event analytics summary: %w", err)
		}

		summary.Daily = append(summary.Daily, daily)
		summary.Created += daily.Created
		summary.Confirmed += daily.Confirmed
		summary.Expired += daily.Expired
		summary.CreatedSeats += daily.CreatedSeats
		summary.ConfirmedSeats += daily.ConfirmedSeats
		summary.ExpiredSeats += daily.ExpiredSeats

		confirmationSeconds += dailyConfirmationSeconds
		expirationLagSeconds += dailyExpirationLagSeconds
	}

	if err := rows.Err(); err != nil {
		return domain.EventSummary{}, fmt.Errorf("read event analytics summary: %w", err)
	}

	if summary.Confirmed > 0 {
		summary.AvgConfirmationSeconds = confirmationSeconds / float64(summary.Confirmed)
	}
	if summary.Expired > 0 {
		summary.AvgExpirationLagSeconds = expirationLagSeconds / float64(summary.Expired)
	}

	return summary, nil
}
