package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPaymentConsumerProcessRecord(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		otherType string
		status    paymentv1.PaymentStatus
	}{
		{
			name:      "succeeded",
			eventType: paymentcontract.PaymentSucceededType,
			otherType: paymentcontract.PaymentFailedType,
			status:    paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED,
		},
		{
			name:      "failed",
			eventType: paymentcontract.PaymentFailedType,
			otherType: paymentcontract.PaymentSucceededType,
			status:    paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			want := application.PaymentResultInput{
				MessageID:     "payment-message",
				ReservationID: "reservation-1",
				UserID:        "user-1",
			}

			calls := 0
			nextProcessed := true
			var nextErr error

			handle := func(
				kind string,
				input application.PaymentResultInput,
			) (bool, error) {
				calls++

				if kind != test.name {
					t.Fatalf("expected %s handler, got %s", test.name, kind)
				}
				if input != want {
					t.Fatalf("unexpected application input: %+v", input)
				}
				return nextProcessed, nextErr
			}

			service := &serviceStub{
				t: t,
				paymentSucceeded: func(
					ctx context.Context,
					input application.PaymentResultInput,
				) (bool, error) {
					return handle("succeeded", input)
				},
				paymentFailed: func(
					ctx context.Context,
					input application.PaymentResultInput,
				) (bool, error) {
					return handle("failed", input)
				},
			}

			createdAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			payment := &paymentv1.Payment{
				Id:            "payment-1",
				ReservationId: want.ReservationID,
				UserId:        want.UserID,
				Status:        test.status,
				CreatedAt:     timestamppb.New(createdAt),
				ExpiresAt:     timestamppb.New(createdAt.Add(10 * time.Minute)),
			}

			var message proto.Message

			if test.eventType == paymentcontract.PaymentSucceededType {
				message = &paymentv1.PaymentSucceeded{
					MessageId:  want.MessageID,
					Payment:    payment,
					OccurredAt: timestamppb.New(createdAt.Add(time.Second)),
				}
			} else {
				message = &paymentv1.PaymentFailed{
					MessageId:  want.MessageID,
					Payment:    payment,
					OccurredAt: timestamppb.New(createdAt.Add(time.Second)),
				}
			}

			payload, err := proto.Marshal(message)
			if err != nil {
				t.Fatalf("marshal payment: %v", err)
			}

			record := &kgo.Record{
				Topic: paymentcontract.PaymentLifecycleTopic,
				Key:   []byte(want.ReservationID),
				Value: payload,
				Headers: []kgo.RecordHeader{
					{Key: "message_id", Value: []byte(want.MessageID)},
					{Key: "event-type", Value: []byte(test.eventType)},
				},
			}
			consumer := NewPaymentConsumer(nil, service, zap.NewNop())

			processed, err := consumer.handleRecord(ctx, record)
			if err != nil || !processed || calls != 1 {
				t.Fatalf("unexpected result: processed=%v calls=%d err=%v", processed, calls, err)
			}

			nextProcessed = false

			processed, err = consumer.handleRecord(ctx, record)
			if err != nil || processed || calls != 2 {
				t.Fatalf("unexpected ignored result: processed=%v calls=%d err=%v", processed, calls, err)
			}

			record.Key = []byte("another-reservation")

			if _, err := consumer.handleRecord(ctx, record); err == nil {
				t.Fatal("mismatched record key must be rejected")
			}
			if calls != 2 {
				t.Fatal("invalid identity must not reach application")
			}

			record.Key = []byte(want.ReservationID)
			record.Headers[1].Value = []byte(test.otherType)

			if _, err := consumer.handleRecord(ctx, record); err == nil {
				t.Fatal("event type and payment status mismatch must be rejected")
			}
			if calls != 2 {
				t.Fatal("invalid status must not reach application")
			}

			record.Headers[1].Value = []byte(test.eventType)
			nextErr = errors.New("application unavailable")

			processed, err = consumer.handleRecord(ctx, record)
			if processed || !errors.Is(err, nextErr) {
				t.Fatalf("expected application failure, got processed=%v err=%v", processed, err)
			}
		})
	}
}
