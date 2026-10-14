package application

import (
	"context"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
)

const reservationEventTimePrecision = time.Microsecond

type Store interface {
	InsertReservationEvents(ctx context.Context, events []domain.ReservationEvent) error
	GetEventSummary(ctx context.Context, eventID string) (domain.EventSummary, error)
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) HandleReservationEvents(ctx context.Context, events []domain.ReservationEvent) error {
	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		if err := validateReservationEvent(event); err != nil {
			return fmt.Errorf("validate analytics message %q: %w", event.MessageID, err)
		}
	}

	return s.store.InsertReservationEvents(ctx, events)
}

func (s *Service) GetEventSummary(ctx context.Context, eventID string) (domain.EventSummary, error) {
	if eventID == "" {
		return domain.EventSummary{}, ErrInvalidEventID
	}

	return s.store.GetEventSummary(ctx, eventID)
}

func validateReservationEvent(event domain.ReservationEvent) error {
	if event.MessageID == "" {
		return ErrInvalidMessageID
	}
	if event.EventID == "" {
		return ErrInvalidEventID
	}
	if event.ReservationID == "" {
		return ErrInvalidReservationID
	}

	switch event.EventType {
	case domain.ReservationEventTypeCreated,
		domain.ReservationEventTypeConfirmed,
		domain.ReservationEventTypeExpired:
	default:
		return ErrInvalidReservationEventType
	}

	if len(event.SeatIDs) == 0 {
		return ErrSeatIDsRequired
	}

	seen := make(map[string]struct{}, len(event.SeatIDs))
	for _, seatID := range event.SeatIDs {
		if seatID == "" {
			return ErrInvalidSeatID
		}
		if _, exists := seen[seatID]; exists {
			return ErrDuplicateSeatID
		}
		seen[seatID] = struct{}{}
	}

	if event.CreatedAt.IsZero() || !event.ExpiresAt.After(event.CreatedAt) {
		return ErrInvalidReservationTimestamps
	}
	if event.OccurredAt.IsZero() {
		return ErrInvalidOccurrenceTime
	}

	createdAt := event.CreatedAt.Truncate(reservationEventTimePrecision)
	occurredAt := event.OccurredAt.Truncate(reservationEventTimePrecision)

	if occurredAt.Before(createdAt) {
		return ErrInvalidOccurrenceTime
	}

	if event.EventType == domain.ReservationEventTypeConfirmed &&
		!event.OccurredAt.Before(event.ExpiresAt) {
		return ErrInvalidOccurrenceTime
	}
	if event.EventType == domain.ReservationEventTypeExpired &&
		event.OccurredAt.Before(event.ExpiresAt) {
		return ErrInvalidOccurrenceTime
	}

	return nil
}
