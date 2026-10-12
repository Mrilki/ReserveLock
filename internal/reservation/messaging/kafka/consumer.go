package kafka

import (
	"context"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const retryInterval = time.Second

type Consumer struct {
	client       *kgo.Client
	service      *application.Service
	logger       *zap.Logger
	handleRecord func(context.Context, *kgo.Record) (bool, error)
}

func newConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
	return &Consumer{
		client:  client,
		service: service,
		logger:  logger,
	}
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
