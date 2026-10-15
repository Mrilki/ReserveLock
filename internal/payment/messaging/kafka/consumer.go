package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/Mrilki/ReserveLock/internal/payment/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	consumerGroup            = "reservelock.payment.reservation-lifecycle.v1"
	retryInterval            = time.Second
	recordTimeout            = 30 * time.Second
	operationTimeout         = 5 * time.Second
	consumerRebalanceTimeout = time.Minute
)

type Consumer struct {
	client  *kgo.Client
	service *application.Service
	logger  *zap.Logger
}

func NewClient(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(reservationcontract.ReservationLifecycleTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(consumerRebalanceTimeout),
	)
}

func NewConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
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
		_, err := c.processRecord(attemptCtx, record)
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

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) (bool, error) {
	switch eventType := headerValue(record, "event-type"); eventType {
	case reservationcontract.ReservationCreatedType:
		var message reservationv1.ReservationCreated

		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return false, fmt.Errorf("decode reservation created: %w", err)
		}

		reservation := message.GetReservation()
		if err := validateRecord(
			record,
			message.GetMessageId(),
			reservation,
			reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING,
		); err != nil {
			return false, err
		}
		if err := reservation.GetCreatedAt().CheckValid(); err != nil {
			return false, fmt.Errorf("invalid reservation creation timestamp: %w", err)
		}
		if err := reservation.GetExpiresAt().CheckValid(); err != nil {
			return false, fmt.Errorf("invalid reservation expiration timestamp: %w", err)
		}

		return c.service.HandleReservationCreated(ctx, application.ReservationCreatedInput{
			MessageID:     message.GetMessageId(),
			ReservationID: reservation.GetId(),
			UserID:        reservation.GetUserId(),
			CreatedAt:     reservation.GetCreatedAt().AsTime(),
			ExpiresAt:     reservation.GetExpiresAt().AsTime(),
		})

	case reservationcontract.ReservationExpiredType:
		var message reservationv1.ReservationExpired

		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return false, fmt.Errorf("decode reservation expired: %w", err)
		}
		reservation := message.GetReservation()
		if err := validateRecord(
			record,
			message.GetMessageId(),
			reservation,
			reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED,
		); err != nil {
			return false, err
		}
		return c.service.HandleReservationExpired(ctx, message.GetMessageId(), reservation.GetId())

	case reservationcontract.ReservationConfirmedType:
		return false, nil
	default:
		return false, fmt.Errorf("unsupported reservation event type %q", eventType)
	}

}

func validateRecord(
	record *kgo.Record,
	messageID string,
	reservation *reservationv1.Reservation,
	expectedStatus reservationv1.ReservationStatus,
) error {
	if messageID == "" {
		return application.ErrInvalidMessageID
	}
	if reservation == nil {
		return fmt.Errorf("reservation is missing in event %q", messageID)
	}
	if reservation.GetId() == "" {
		return application.ErrInvalidReservationID
	}

	if headerValue(record, "message_id") != messageID || string(record.Key) != reservation.GetId() {
		return fmt.Errorf("reservation event identity mismatch for message %q", messageID)
	}

	if reservation.GetStatus() != expectedStatus {
		return fmt.Errorf("unexpected reservation status for message %q: %s", messageID, reservation.GetStatus())
	}

	return nil
}

func headerValue(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
