package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testStore struct {
	calls    int
	events   []domain.ReservationEvent
	err      error
	onInsert func()
}

func (s *testStore) InsertReservationEvents(_ context.Context, events []domain.ReservationEvent) error {
	s.calls++
	s.events = append([]domain.ReservationEvent(nil), events...)

	if s.onInsert != nil {
		s.onInsert()
	}

	return s.err
}

func (s *testStore) GetEventSummary(_ context.Context, eventID string) (domain.EventSummary, error) {
	return domain.EventSummary{EventID: eventID}, nil
}

type testClient struct {
	commits   int
	failFirst bool
}

func (c *testClient) PollRecords(context.Context, int) kgo.Fetches {
	panic("PollRecords is not expected in processBatch tests")
}

func (c *testClient) AllowRebalance() {}

func (c *testClient) CommitRecords(context.Context, ...*kgo.Record) error {
	c.commits++
	if c.failFirst && c.commits == 1 {
		return errors.New("commit unavailable")
	}
	return nil
}

func TestConsumerProcessesReservationFacts(t *testing.T) {
	tests := []struct {
		eventType string
		expected  domain.ReservationEventType
	}{
		{reservationcontract.ReservationCreatedType, domain.ReservationEventTypeCreated},
		{reservationcontract.ReservationConfirmedType, domain.ReservationEventTypeConfirmed},
		{reservationcontract.ReservationExpiredType, domain.ReservationEventTypeExpired},
	}

	for _, test := range tests {
		t.Run(test.eventType, func(t *testing.T) {
			store := &testStore{}
			client := &testClient{}
			consumer := &Consumer{
				client: client, service: application.NewService(store), logger: zap.NewNop(),
			}

			record := testReservationRecord(t, test.eventType)
			if err := consumer.processBatch(context.Background(), []*kgo.Record{record}); err != nil {
				t.Fatal(err)
			}

			if store.calls != 1 || client.commits != 1 || len(store.events) != 1 {
				t.Fatalf("unexpected calls: inserts=%d commits=%d events=%d", store.calls, client.commits, len(store.events))
			}

			event := store.events[0]
			if event.EventType != test.expected || event.MessageID != "message-1" ||
				event.EventID != "event-1" || event.ReservationID != "reservation-1" ||
				len(event.SeatIDs) != 1 || event.SeatIDs[0] != "seat-1" {
				t.Fatalf("unexpected analytics fact: %+v", event)
			}
		})
	}
}

func TestConsumerRejectsBatchBeforeStorage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*kgo.Record)
	}{
		{"key mismatch", func(record *kgo.Record) { record.Key = []byte("wrong-reservation") }},
		{"message id mismatch", func(record *kgo.Record) { record.Headers[0].Value = []byte("wrong-message") }},
		{"broken protobuf", func(record *kgo.Record) { record.Value = []byte{0xff} }},
		{"status mismatch", func(record *kgo.Record) {
			record.Headers[1].Value = []byte(reservationcontract.ReservationConfirmedType)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &testStore{}
			client := &testClient{}
			consumer := &Consumer{
				client: client, service: application.NewService(store), logger: zap.NewNop(),
			}

			valid := testReservationRecord(t, reservationcontract.ReservationCreatedType)
			invalid := testReservationRecord(t, reservationcontract.ReservationCreatedType)
			test.mutate(invalid)

			err := consumer.processBatch(context.Background(), []*kgo.Record{valid, invalid})
			if err == nil || store.calls != 0 || client.commits != 0 {
				t.Fatalf("invalid batch must not be stored/committed: err=%v inserts=%d commits=%d", err, store.calls, client.commits)
			}
		})
	}
}

func TestConsumerStorageFailureDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &testStore{
		err:      errors.New("clickhouse unavailable"),
		onInsert: cancel,
	}
	client := &testClient{}
	consumer := &Consumer{
		client: client, service: application.NewService(store), logger: zap.NewNop(),
	}

	err := consumer.processBatch(ctx, []*kgo.Record{
		testReservationRecord(t, reservationcontract.ReservationCreatedType),
	})

	if !errors.Is(err, context.Canceled) || store.calls != 1 || client.commits != 0 {
		t.Fatalf("storage failure must not commit: err=%v inserts=%d commits=%d", err, store.calls, client.commits)
	}
}

func TestConsumerCommitRetryDoesNotRepeatInsert(t *testing.T) {
	store := &testStore{}
	client := &testClient{failFirst: true}
	consumer := &Consumer{
		client: client, service: application.NewService(store), logger: zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := consumer.processBatch(ctx, []*kgo.Record{
		testReservationRecord(t, reservationcontract.ReservationCreatedType),
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || client.commits != 2 {
		t.Fatalf("expected one insert and two commit attempts: inserts=%d commits=%d", store.calls, client.commits)
	}
}

func testReservationRecord(t *testing.T, eventType string) *kgo.Record {
	t.Helper()

	createdAt := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	reservation := &reservationv1.Reservation{
		Id:        "reservation-1",
		EventId:   "event-1",
		UserId:    "user-1",
		SeatIds:   []string{"seat-1"},
		CreatedAt: timestamppb.New(createdAt),
		ExpiresAt: timestamppb.New(createdAt.Add(10 * time.Minute)),
	}

	var message proto.Message
	switch eventType {
	case reservationcontract.ReservationCreatedType:
		reservation.Status = reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING
		message = &reservationv1.ReservationCreated{
			MessageId: "message-1", Reservation: reservation, OccurredAt: timestamppb.New(createdAt),
		}
	case reservationcontract.ReservationConfirmedType:
		reservation.Status = reservationv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED
		message = &reservationv1.ReservationConfirmed{
			MessageId: "message-1", Reservation: reservation, OccurredAt: timestamppb.New(createdAt.Add(time.Minute)),
		}
	case reservationcontract.ReservationExpiredType:
		reservation.Status = reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED
		message = &reservationv1.ReservationExpired{
			MessageId: "message-1", Reservation: reservation, OccurredAt: reservation.ExpiresAt,
		}
	default:
		t.Fatalf("unsupported test event type: %s", eventType)
	}

	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	return &kgo.Record{
		Topic: reservationcontract.ReservationLifecycleTopic,
		Key:   []byte(reservation.Id),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "message_id", Value: []byte("message-1")},
			{Key: "event-type", Value: []byte(eventType)},
		},
	}
}
