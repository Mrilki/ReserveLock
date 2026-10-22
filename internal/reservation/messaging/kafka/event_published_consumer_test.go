package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventPublishedConsumerProcessRecord(t *testing.T) {
	want := application.EventPublishedInput{
		MessageID: "message-1",
		EventID:   "event-1",
		SeatIDs:   []string{"seat-1", "seat-2"},
	}

	calls := 0
	var serviceErr error

	service := &serviceStub{
		t: t,
		eventPublished: func(
			ctx context.Context,
			input application.EventPublishedInput,
		) (bool, error) {
			calls++

			if !reflect.DeepEqual(input, want) {
				t.Fatalf("unexpected application input: %+v", input)
			}
			if serviceErr != nil {
				return false, serviceErr
			}
			return true, nil
		},
	}

	payload, err := proto.Marshal(&eventcontentv1.EventPublished{
		MessageId:   want.MessageID,
		EventId:     want.EventID,
		SeatIds:     want.SeatIDs,
		PublishedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("marshal event published: %v", err)
	}

	record := &kgo.Record{
		Topic: eventcontentcontract.EventPublishedTopic,
		Key:   []byte(want.EventID),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "message_id", Value: []byte(want.MessageID)},
			{Key: "event-type", Value: []byte(eventcontentcontract.EventPublishedType)},
		},
	}
	consumer := NewEventPublishedConsumer(nil, service, zap.NewNop())

	processed, err := consumer.handleRecord(context.Background(), record)
	if err != nil || !processed || calls != 1 {
		t.Fatalf("unexpected result: processed=%v calls=%d err=%v", processed, calls, err)
	}

	record.Key = []byte("another-event")

	if _, err := consumer.handleRecord(context.Background(), record); err == nil {
		t.Fatal("expected event identity mismatch")
	}
	if calls != 1 {
		t.Fatal("invalid record must not reach application")
	}

	record.Key = []byte(want.EventID)
	serviceErr = errors.New("application unavailable")

	processed, err = consumer.handleRecord(context.Background(), record)
	if processed || !errors.Is(err, serviceErr) {
		t.Fatalf("expected application failure, got processed=%v err=%v", processed, err)
	}
}
