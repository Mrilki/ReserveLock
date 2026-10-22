package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	paymentoutbox "github.com/Mrilki/ReserveLock/internal/payment/outbox"
	"github.com/Mrilki/ReserveLock/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(client *kgo.Client) *Publisher {
	return &Publisher{client: client}
}

func (p *Publisher) Publish(ctx context.Context, message outbox.Message) error {
	record, err := recordFromMessage(message)
	if err != nil {
		return err
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce payment message %q: %w", message.ID, err)
	}

	return nil
}

func recordFromMessage(message outbox.Message) (*kgo.Record, error) {
	var snapshot paymentoutbox.PaymentSnapshot

	if err := json.Unmarshal(message.Payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode payment outbox snapshot: %w", err)
	}

	if message.ID == "" || message.Key == "" || snapshot.ReservationID != message.Key {
		return nil, fmt.Errorf("invalid payment outbox identity for message %q", message.ID)
	}
	if snapshot.PaymentID == "" || snapshot.UserID == "" {
		return nil, fmt.Errorf("incomplete payment outbox snapshot for message %q", message.ID)
	}
	if snapshot.CreatedAt.IsZero() || !snapshot.ExpiresAt.After(snapshot.CreatedAt) || message.OccurredAt.IsZero() {
		return nil, fmt.Errorf("invalid payment outbox timestamps for message %q", message.ID)
	}

	payment := &paymentv1.Payment{
		Id:            snapshot.PaymentID,
		ReservationId: snapshot.ReservationID,
		UserId:        snapshot.UserID,
		CreatedAt:     timestamppb.New(snapshot.CreatedAt),
		ExpiresAt:     timestamppb.New(snapshot.ExpiresAt),
	}
	occurredAt := timestamppb.New(message.OccurredAt)

	for _, timestamp := range []*timestamppb.Timestamp{payment.CreatedAt, payment.ExpiresAt, occurredAt} {
		if err := timestamp.CheckValid(); err != nil {
			return nil, fmt.Errorf("invalid payment protobuf timestamp: %w", err)
		}
	}

	var event proto.Message
	var expectedStatus domain.PaymentStatus

	switch message.EventType {
	case paymentcontract.PaymentSucceededType:
		expectedStatus = domain.PaymentStatusSucceeded
		payment.Status = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
		event = &paymentv1.PaymentSucceeded{
			MessageId:  message.ID,
			Payment:    payment,
			OccurredAt: occurredAt,
		}

	case paymentcontract.PaymentFailedType:
		expectedStatus = domain.PaymentStatusFailed
		payment.Status = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
		event = &paymentv1.PaymentFailed{
			MessageId:  message.ID,
			Payment:    payment,
			OccurredAt: occurredAt,
		}

	default:
		return nil, fmt.Errorf("unsupported payment event type %q", message.EventType)
	}

	if snapshot.Status != string(expectedStatus) {
		return nil, fmt.Errorf("payment event %q requires status %s, got %q", message.EventType, expectedStatus, snapshot.Status)
	}

	payload, err := proto.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode payment event: %w", err)
	}

	return &kgo.Record{
		Topic:     paymentcontract.PaymentLifecycleTopic,
		Key:       []byte(message.Key),
		Value:     payload,
		Timestamp: message.OccurredAt,
		Headers: []kgo.RecordHeader{
			{Key: "message_id", Value: []byte(message.ID)},
			{Key: "event-type", Value: []byte(message.EventType)},
		},
	}, nil
}
