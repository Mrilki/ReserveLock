package kafka

import (
	"context"
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
	consumerGroup = "reservelock.payment.reservation-lifecycle.v1"
	retryInterval = time.Second
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
	)
}

func NewConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
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
			c.logger.Error("payment kafka fetch failed",
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
			_, err := c.processRecord(ctx, record)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}

			c.logger.Error("payment kafka processing failed",
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
	if messageID == "" || reservation == nil || reservation.GetId() == "" {
		return fmt.Errorf("incomplete reservation event")
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

func (c *Consumer) commitRecord(ctx context.Context, record *kgo.Record) error {
	for {
		err := c.client.CommitRecords(ctx, record)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.logger.Error("failed to commit payment kafka offset",
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
