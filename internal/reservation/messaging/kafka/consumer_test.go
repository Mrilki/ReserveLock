package kafka

import (
	"context"
	"testing"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestConsumerProcessRecord(t *testing.T) {
	service := application.NewService(memory.NewStore(), nil)

	payload, err := proto.Marshal(&eventcontentv1.EventPublished{
		MessageId: "message-1",
		EventId:   "event-1",
		SeatIds:   []string{"seat-1", "seat-2"},
	})
	if err != nil {
		t.Fatalf("marshal event published: %v", err)
	}

	consumer := &Consumer{
		service: service,
	}

	processed, err := consumer.processRecord(
		context.Background(),
		&kgo.Record{
			Value: payload,
		},
	)
	if err != nil {
		t.Fatalf("process record: %v", err)
	}

	if !processed {
		t.Fatal("expected message to be processed")
	}
}
