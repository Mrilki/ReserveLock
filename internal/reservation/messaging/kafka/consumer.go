package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

const (
	retryInterval            = time.Second
	recordTimeout            = 30 * time.Second
	operationTimeout         = 5 * time.Second
	consumerRebalanceTimeout = time.Minute
)

type reservationService interface {
	HandleEventPublished(ctx context.Context, input application.EventPublishedInput) (bool, error)
	HandlePaymentSucceeded(ctx context.Context, input application.PaymentResultInput) (bool, error)
	HandlePaymentFailed(ctx context.Context, input application.PaymentResultInput) (bool, error)
}

type Consumer struct {
	client       *kgo.Client
	service      reservationService
	logger       *zap.Logger
	handleRecord func(context.Context, *kgo.Record) (bool, error)
}

func newConsumer(client *kgo.Client, service reservationService, logger *zap.Logger) *Consumer {
	return &Consumer{
		client:  client,
		service: service,
		logger:  logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.AllowRebalance()

	for {
		fetches := c.client.PollRecords(ctx, 1)

		if ctx.Err() != nil {
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			if errors.Is(fetchErr.Err, kgo.ErrClientClosed) {
				return fetchErr.Err
			}

			c.logger.Error("kafka fetch failed",
				zap.String("topic", fetchErr.Topic),
				zap.Int32("partition", fetchErr.Partition),
				zap.Error(fetchErr.Err),
			)
		}

		records := fetches.Records()
		if len(records) == 0 {
			c.client.AllowRebalance()

			if err := waitRetry(ctx); err != nil {
				return nil
			}

			continue
		}

		record := records[0]
		err := c.processAndCommit(ctx, record)

		c.client.AllowRebalance()

		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf(
				"process kafka record %s/%d/%d: %w",
				record.Topic,
				record.Partition,
				record.Offset,
				err,
			)
		}
	}
}

func (c *Consumer) processAndCommit(ctx context.Context, record *kgo.Record) error {
	recordCtx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()

	if err := c.retry(recordCtx, "handle", func(attemptCtx context.Context) error {
		_, err := c.handleRecord(attemptCtx, record)
		return err
	}); err != nil {
		return fmt.Errorf("handle record: %w", err)
	}
	if err := c.retry(recordCtx, "commit", func(attemptCtx context.Context) error {
		return c.client.CommitRecords(attemptCtx, record)
	}); err != nil {
		return fmt.Errorf("commit record: %w", err)
	}
	return nil
}

func (c *Consumer) retry(
	ctx context.Context,
	operation string,
	run func(context.Context) error,
) error {
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

		c.logger.Error("kafka operation failed",
			zap.String("operation", operation),
			zap.Error(err),
		)

		if waitErr := waitRetry(ctx); waitErr != nil {
			return fmt.Errorf(
				"%s failed: %w; retry budget: %w",
				operation,
				err,
				waitErr,
			)
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
