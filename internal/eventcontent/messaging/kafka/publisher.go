package kafka

import (
	"context"
	"fmt"

	eventoutbox "github.com/Mrilki/ReserveLock/internal/eventcontent/outbox"
	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"github.com/twmb/franz-go/pkg/kgo"
)

const eventPublishedTopic = "reservelock.event-content.event-published.v1"

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(client *kgo.Client) *Publisher {
	return &Publisher{
		client: client,
	}
}

func (p *Publisher) Publish(ctx context.Context, message eventoutbox.Message) error {
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
		return fmt.Errorf(
			"produce kafka message %q: %w",
			message.ID,
			err,
		)
	}
	return nil
}

func topicForEventType(eventType string) (string, error) {
	switch eventType {
	case eventcontentcontract.EventPublishedType:
		return eventPublishedTopic, nil

	default:
		return "", fmt.Errorf(
			"unsupported outbox event type %q",
			eventType,
		)
	}
}
