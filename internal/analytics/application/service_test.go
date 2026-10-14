package application

import (
	"errors"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
)

func TestValidateReservationEventTimestampPrecision(t *testing.T) {
	createdAt := time.Date(2026, 10, 14, 12, 0, 0, 123456789, time.UTC)

	event := domain.ReservationEvent{
		MessageID:     "message-1",
		EventID:       "event-1",
		ReservationID: "reservation-1",
		EventType:     domain.ReservationEventTypeCreated,
		SeatIDs:       []string{"seat-1"},
		CreatedAt:     createdAt,
		ExpiresAt:     createdAt.Add(10 * time.Minute),
		OccurredAt:    createdAt.Truncate(time.Microsecond),
	}

	if err := validateReservationEvent(event); err != nil {
		t.Fatalf("same moment at postgres precision must be accepted: %v", err)
	}

	event.OccurredAt = createdAt.Truncate(time.Microsecond).Add(-time.Microsecond)

	if err := validateReservationEvent(event); !errors.Is(err, ErrInvalidOccurrenceTime) {
		t.Fatalf("an earlier microsecond must be rejected, got %v", err)
	}
}
