package kafka

import (
	"context"
	"fmt"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const paymentConsumerGroup = "reservelock.reservation.payment-lifecycle.v1"

func NewPaymentClient(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(paymentConsumerGroup),
		kgo.ConsumeTopics(paymentcontract.PaymentLifecycleTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
}

func NewPaymentConsumer(client *kgo.Client, service *application.Service, logger *zap.Logger) *Consumer {
	consumer := newConsumer(client, service, logger)
	consumer.handleRecord = consumer.processPaymentRecord

	return consumer
}

func (c *Consumer) processPaymentRecord(ctx context.Context, record *kgo.Record) (bool, error) {
	eventType := headerValue(record, "event-type")

	var messageID string
	var payment *paymentv1.Payment
	var occurredAt *timestamppb.Timestamp
	var expectedStatus paymentv1.PaymentStatus

	switch eventType {
	case paymentcontract.PaymentSucceededType:
		var message paymentv1.PaymentSucceeded
		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return false, fmt.Errorf("decode payment succeeded: %w", err)
		}

		messageID = message.GetMessageId()
		payment = message.GetPayment()
		occurredAt = message.GetOccurredAt()
		expectedStatus = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED

	case paymentcontract.PaymentFailedType:
		var message paymentv1.PaymentFailed
		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return false, fmt.Errorf("decode payment failed: %w", err)
		}

		messageID = message.GetMessageId()
		payment = message.GetPayment()
		occurredAt = message.GetOccurredAt()
		expectedStatus = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED

	default:
		return false, fmt.Errorf("unsupported payment event type %q", eventType)
	}

	if messageID == "" {
		return false, application.ErrInvalidMessageID
	}
	if payment == nil {
		return false, fmt.Errorf("payment is missing in event %q", messageID)
	}
	if payment.GetId() == "" {
		return false, fmt.Errorf("payment id is missing in event %q", messageID)
	}
	if payment.GetReservationId() == "" {
		return false, application.ErrInvalidReservationID
	}
	if payment.GetUserId() == "" {
		return false, application.ErrInvalidUserID
	}

	if headerValue(record, "message_id") != messageID || string(record.Key) != payment.GetReservationId() {
		return false, fmt.Errorf("payment event identity mismatch for message %q", messageID)
	}
	if payment.GetStatus() != expectedStatus {
		return false, fmt.Errorf("unexpected payment status for message %q: %s", messageID, payment.GetStatus())
	}
	if err := occurredAt.CheckValid(); err != nil {
		return false, fmt.Errorf("invalid payment occurrence timestamp: %w", err)
	}

	input := application.PaymentResultInput{
		MessageID:     messageID,
		ReservationID: payment.GetReservationId(),
		UserID:        payment.GetUserId(),
	}

	if eventType == paymentcontract.PaymentSucceededType {
		return c.service.HandlePaymentSucceeded(ctx, input)
	}

	return c.service.HandlePaymentFailed(ctx, input)
}

func headerValue(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}

	return ""
}
