package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const (
	batchSize        = 1000
	batchWait        = time.Second
	batchTimeout     = 30 * time.Second
	operationTimeout = 5 * time.Second
	retryInterval    = time.Second
)

type kafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, records ...*kgo.Record) error
	AllowRebalance()
}

type Consumer struct {
	client  kafkaClient
	service *application.Service
	logger  *zap.Logger
}

func NewConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
	return &Consumer{client: client, service: service, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		records, err := c.nextBatch(ctx)
		if err == nil && len(records) > 0 {
			err = c.processBatch(ctx, records)
		}

		c.client.AllowRebalance()

		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}

		if len(records) == 0 {
			if err := waitRetry(ctx); err != nil {
				return nil
			}
		}
	}
}

func (c *Consumer) nextBatch(ctx context.Context) ([]*kgo.Record, error) {
	records, err := c.pollRecords(ctx, batchSize)
	if err != nil || len(records) == 0 || len(records) == batchSize {
		return records, err
	}

	collectCtx, cancel := context.WithTimeout(ctx, batchWait)
	defer cancel()

	for len(records) < batchSize {
		next, err := c.pollRecords(collectCtx, batchSize-len(records))
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return records, nil
			}
			return nil, err
		}
		if len(next) == 0 {
			break
		}

		records = append(records, next...)
	}

	return records, nil
}

func (c *Consumer) pollRecords(ctx context.Context, limit int) ([]*kgo.Record, error) {
	fetches := c.client.PollRecords(ctx, limit)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, kgo.ErrClientClosed) {
			return nil, fetchErr.Err
		}

		c.logger.Error("analytics kafka fetch failed",
			zap.String("topic", fetchErr.Topic),
			zap.Int32("partition", fetchErr.Partition),
			zap.Error(fetchErr.Err),
		)
	}

	return fetches.Records(), nil
}

func (c *Consumer) processBatch(ctx context.Context, records []*kgo.Record) error {
	events := make([]domain.ReservationEvent, 0, len(records))

	for _, record := range records {
		if record == nil {
			return fmt.Errorf("nil analytics kafka record")
		}
		event, err := reservationEventFromRecord(record)
		if err != nil {
			return fmt.Errorf("decode analytics record %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		events = append(events, event)
	}

	batchCtx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()

	if err := c.retry(batchCtx, "insert", func(attemptCtx context.Context) error {
		return c.service.HandleReservationEvents(attemptCtx, events)
	}); err != nil {
		return fmt.Errorf("persist analytics batch: %w", err)
	}

	if err := c.retry(batchCtx, "commit", func(attemptCtx context.Context) error {
		return c.client.CommitRecords(attemptCtx, records...)
	}); err != nil {
		return fmt.Errorf("commit analytics batch offsets: %w", err)
	}

	c.logger.Debug("analytics batch processed", zap.Int("records", len(records)))
	return nil
}

func (c *Consumer) retry(ctx context.Context, operation string, run func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		err := run(attemptCtx)
		cancel()

		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s failed: %w; retry budget: %w", operation, err, ctx.Err())
		}

		c.logger.Error("analytics batch operation failed",
			zap.String("operation", operation),
			zap.Error(err),
		)

		if waitErr := waitRetry(ctx); waitErr != nil {
			return fmt.Errorf("%s failed: %w; retry budget: %w", operation, err, waitErr)
		}
	}
}

func waitRetry(ctx context.Context) error {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
