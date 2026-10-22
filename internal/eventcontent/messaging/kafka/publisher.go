package kafka

import (
	"context"
	"fmt"

	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"github.com/Mrilki/ReserveLock/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(client *kgo.Client) *Publisher {
	return &Publisher{
		client: client,
	}
}

func (p *Publisher) Publish(ctx context.Context, message outbox.Message) error {
	topic, err := topicForEventType(message.EventType)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic:     topic,
		Key:       []byte(message.Key),
		Value:     message.Payload,
		Timestamp: message.OccurredAt,
		Headers: []kgo.RecordHeader{
			{
				Key:   "message_id",
				Value: []byte(message.ID),
			},
			{
				Key:   "event-type",
				Value: []byte(message.EventType),
			},
		},
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce kafka message %q: %w", message.ID, err)
	}
	return nil
}

func topicForEventType(eventType string) (string, error) {
	switch eventType {
	case eventcontentcontract.EventPublishedType:
		return eventcontentcontract.EventPublishedTopic, nil

	default:
		return "", fmt.Errorf("unsupported outbox event type %q", eventType)
	}
}
