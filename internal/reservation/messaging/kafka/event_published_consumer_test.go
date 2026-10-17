package kafka

import (
	"context"
	"testing"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventPublishedConsumerProcessRecord(t *testing.T) {
	service := application.NewService(memory.NewStore(), nil)

	payload, err := proto.Marshal(&eventcontentv1.EventPublished{
		MessageId:   "message-1",
		EventId:     "event-1",
		SeatIds:     []string{"seat-1", "seat-2"},
		PublishedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("marshal event published: %v", err)
	}

	consumer := NewEventPublishedConsumer(nil, service, zap.NewNop())

	record := &kgo.Record{
		Topic: eventcontentcontract.EventPublishedTopic,
		Key:   []byte("event-1"),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "message_id", Value: []byte("message-1")},
			{Key: "event-type", Value: []byte(eventcontentcontract.EventPublishedType)},
		},
	}

	processed, err := consumer.handleRecord(context.Background(), record)
	if err != nil {
		t.Fatalf("process record: %v", err)
	}

	if !processed {
		t.Fatal("expected message to be processed")
	}

	record.Key = []byte("another-event")

	if _, err := consumer.handleRecord(context.Background(), record); err == nil {
		t.Fatal("expected event identity mismatch")
	}
}
