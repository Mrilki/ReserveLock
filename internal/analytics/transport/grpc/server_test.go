package analyticsgrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type testStore struct {
	summary domain.EventSummary
	err     error
	calls   int
	eventID string
}

func (s *testStore) InsertReservationEvents(context.Context, []domain.ReservationEvent) error {
	panic("InsertReservationEvents is not expected in summary tests")
}

func (s *testStore) GetEventSummary(_ context.Context, eventID string) (domain.EventSummary, error) {
	s.calls++
	s.eventID = eventID
	return s.summary, s.err
}

func TestGetEventSummary(t *testing.T) {
	tests := []struct {
		name     string
		summary  domain.EventSummary
		expected *analyticsv1.EventSummary
	}{
		{
			name: "complete summary",
			summary: domain.EventSummary{
				EventID: "event-1", Created: 5, Confirmed: 2, Expired: 1,
				CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
				AvgConfirmationSeconds: 30.5, AvgExpirationLagSeconds: 2.25,
				Daily: []domain.EventDailySummary{
					{
						Date: "2026-10-14", Created: 5, Confirmed: 2, Expired: 1,
						CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
					},
				},
			},
			expected: &analyticsv1.EventSummary{
				EventId: "event-1", Created: 5, Confirmed: 2, Expired: 1,
				CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
				ConfirmationRatePercent: 40, ExpirationRatePercent: 20,
				AvgConfirmationSeconds: 30.5, AvgExpirationLagSeconds: 2.25,
				Daily: []*analyticsv1.EventDailySummary{
					{
						Date: "2026-10-14", Created: 5, Confirmed: 2, Expired: 1,
						CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
					},
				},
			},
		},
		{
			name:     "no facts",
			summary:  domain.EventSummary{EventID: "event-1"},
			expected: &analyticsv1.EventSummary{EventId: "event-1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &testStore{summary: test.summary}
			server := NewServer(application.NewService(store))

			response, err := server.GetEventSummary(context.Background(), &analyticsv1.GetEventSummaryRequest{
				EventId: "event-1",
			})
			if err != nil {
				t.Fatal(err)
			}

			if !proto.Equal(response.GetSummary(), test.expected) {
				t.Fatalf("unexpected summary: got %v, want %v", response.GetSummary(), test.expected)
			}
			if store.calls != 1 || store.eventID != "event-1" {
				t.Fatalf("unexpected store call: calls=%d eventID=%q", store.calls, store.eventID)
			}
		})
	}
}

func TestGetEventSummaryErrors(t *testing.T) {
	tests := []struct {
		name         string
		eventID      string
		storeErr     error
		expectedCode codes.Code
		expectedMsg  string
		storeCalls   int
	}{
		{"empty event id", "", nil, codes.InvalidArgument, "invalid event id", 0},
		{"store failure", "event-1", errors.New("private clickhouse details"), codes.Internal, "internal error", 1},
		{"deadline", "event-1", context.DeadlineExceeded, codes.DeadlineExceeded, "context deadline exceeded", 1},
		{"canceled", "event-1", context.Canceled, codes.Canceled, "context canceled", 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &testStore{err: test.storeErr}
			server := NewServer(application.NewService(store))

			response, err := server.GetEventSummary(context.Background(), &analyticsv1.GetEventSummaryRequest{
				EventId: test.eventID,
			})

			grpcStatus := status.Convert(err)
			if response != nil || grpcStatus.Code() != test.expectedCode || grpcStatus.Message() != test.expectedMsg {
				t.Fatalf("unexpected response/error: response=%v error=%v", response, err)
			}
			if store.calls != test.storeCalls {
				t.Fatalf("store calls: got %d, want %d", store.calls, test.storeCalls)
			}
		})
	}
}
