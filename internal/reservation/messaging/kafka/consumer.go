package kafka

import (
	"context"
	"fmt"
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	eventPublishedConsumerGroup = "reservelock.reservation.event-published.v1"
	retryInterval               = time.Second
)

type Consumer struct {
	client       *kgo.Client
	service      *application.Service
	logger       *zap.Logger
	handleRecord func(context.Context, *kgo.Record) (bool, error)
}

func NewClient(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(eventPublishedConsumerGroup),
		kgo.ConsumeTopics(eventcontentcontract.EventPublishedTopic),
		kgo.DisableAutoCommit(),
	)
}

func NewConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
	consumer := &Consumer{
		client:  client,
		service: service,
		logger:  logger,
	}
	consumer.handleRecord = consumer.processRecord

	return consumer
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.client.PollRecords(ctx, 1)

		if ctx.Err() != nil {
			return
		}

		for _, fetchErr := range fetches.Errors() {
			c.logger.Error(
				"kafka fetch failed",
				zap.String("topic", fetchErr.Topic),
				zap.Int32("partition", fetchErr.Partition),
				zap.Error(fetchErr.Err),
			)
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		record := records[0]

		for {
			processed, err := c.handleRecord(ctx, record)
			if err == nil {
				if !processed {
					c.logger.Debug(
						"duplicate kafka message ignored",
						zap.String("topic", record.Topic),
						zap.Int32("partition", record.Partition),
						zap.Int64("offset", record.Offset),
					)
				}

				break
			}

			if ctx.Err() != nil {
				return
			}

			c.logger.Error(
				"failed to process kafka message",
				zap.String("topic", record.Topic),
				zap.Int32("partition", record.Partition),
				zap.Int64("offset", record.Offset),
				zap.Error(err),
			)

			timer := time.NewTimer(retryInterval)

			select {
			case <-ctx.Done():
				timer.Stop()
				return

			case <-timer.C:
			}
		}

		if err := c.commitRecord(ctx, record); err != nil {
			return
		}

	}

}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) (bool, error) {
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

func (c *Consumer) commitRecord(ctx context.Context, record *kgo.Record) error {
	for {
		err := c.client.CommitRecords(ctx, record)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.logger.Error(
			"failed to commit kafka offset",
			zap.String("topic", record.Topic),
			zap.Int32("partition", record.Partition),
			zap.Int64("offset", record.Offset),
			zap.Error(err),
		)

		timer := time.NewTimer(retryInterval)

		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()

		case <-timer.C:
		}
	}
}
