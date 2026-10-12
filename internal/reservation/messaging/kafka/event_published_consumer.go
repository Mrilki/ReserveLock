package kafka

import (
	"context"
	"fmt"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const eventPublishedConsumerGroup = "reservelock.reservation.event-published.v1"

func NewEventPublishedClient(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(eventPublishedConsumerGroup),
		kgo.ConsumeTopics(eventcontentcontract.EventPublishedTopic),
		kgo.DisableAutoCommit(),
	)
}

func NewEventPublishedConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
	consumer := newConsumer(client, service, logger)
	consumer.handleRecord = consumer.processEventPublishedRecord

	return consumer
}

func (c *Consumer) processEventPublishedRecord(ctx context.Context, record *kgo.Record) (bool, error) {
	var message eventcontentv1.EventPublished

	if err := proto.Unmarshal(record.Value, &message); err != nil {
		return false, fmt.Errorf("unmarshal event published: %w", err)
	}

	processed, err := c.service.HandleEventPublished(
		ctx,
		application.EventPublishedInput{
			MessageID: message.GetMessageId(),
			EventID:   message.GetEventId(),
			SeatIDs:   message.GetSeatIds(),
		},
	)
	if err != nil {
		return false, fmt.Errorf("handle event published: %w", err)
	}
	return processed, nil

}
