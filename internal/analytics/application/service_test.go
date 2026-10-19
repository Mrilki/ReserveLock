package application

import (
	"context"
	"errors"
	"math"
	"reflect"
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

type summaryStore struct {
	summary domain.EventSummary
	eventID string
}

func (s *summaryStore) InsertReservationEvents(context.Context, []domain.ReservationEvent) error {
	panic("InsertReservationEvents is not expected in summary tests")
}

func (s *summaryStore) GetEventSummary(_ context.Context, eventID string) (domain.EventSummary, error) {
	s.eventID = eventID
	return s.summary, nil
}

func TestGetEventSummaryRates(t *testing.T) {
	tests := []struct {
		name         string
		created      uint64
		confirmed    uint64
		expired      uint64
		confirmation float64
		expiration   float64
	}{
		{"completed", 4, 3, 1, 75, 25},
		{"pending remain", 4, 1, 1, 25, 25},
		{"fractional rates", 3, 1, 2, 100.0 / 3, 200.0 / 3},
		{"no facts", 0, 0, 0, 0, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &summaryStore{
				summary: domain.EventSummary{
					EventID:   "event-1",
					Created:   test.created,
					Confirmed: test.confirmed,
					Expired:   test.expired,
					Daily:     []domain.EventDailySummary{},
				},
			}
			if test.confirmed > 0 {
				store.summary.AvgConfirmationSeconds = 40.5
			}
			if test.expired > 0 {
				store.summary.AvgExpirationLagSeconds = 2.25
			}

			summary, err := NewService(store).GetEventSummary(context.Background(), "event-1")
			if err != nil {
				t.Fatal(err)
			}
			if store.eventID != "event-1" {
				t.Fatalf("unexpected event id: %q", store.eventID)
			}

			if math.Abs(summary.ConfirmationRatePercent-test.confirmation) > 1e-9 ||
				math.Abs(summary.ExpirationRatePercent-test.expiration) > 1e-9 {
				t.Fatalf("unexpected rates: %+v", summary)
			}

			summary.ConfirmationRatePercent = 0
			summary.ExpirationRatePercent = 0
			if !reflect.DeepEqual(summary, store.summary) {
				t.Fatalf("store metrics must be preserved: got %+v, want %+v", summary, store.summary)
			}
		})
	}
}
