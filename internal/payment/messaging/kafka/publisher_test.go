package kafka

import (
	"encoding/json"
	"testing"
	"time"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	paymentoutbox "github.com/Mrilki/ReserveLock/internal/payment/outbox"
	"github.com/Mrilki/ReserveLock/internal/platform/outbox"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecordFromMessageEncodesPaymentEvents(t *testing.T) {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		eventType   string
		status      string
		protoStatus paymentv1.PaymentStatus
	}{
		{"succeeded", paymentcontract.PaymentSucceededType, "SUCCEEDED", paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED},
		{"failed", paymentcontract.PaymentFailedType, "FAILED", paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := paymentoutbox.PaymentSnapshot{
				PaymentID:     "payment-1",
				ReservationID: "reservation-1",
				UserID:        "user-1",
				Status:        test.status,
				CreatedAt:     now,
				ExpiresAt:     now.Add(10 * time.Minute),
			}
			payload, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}

			message := outbox.Message{
				ID:         "message-1",
				EventType:  test.eventType,
				Key:        snapshot.ReservationID,
				Payload:    payload,
				OccurredAt: now.Add(time.Minute),
			}
			record, err := recordFromMessage(message)
			if err != nil {
				t.Fatal(err)
			}

			if record.Topic != paymentcontract.PaymentLifecycleTopic || string(record.Key) != message.Key {
				t.Fatalf("unexpected routing: topic=%s key=%s", record.Topic, record.Key)
			}
			if headerValue(record, "message_id") != message.ID || headerValue(record, "event-type") != message.EventType {
				t.Fatal("record headers do not match the stored message")
			}
			if !record.Timestamp.Equal(message.OccurredAt) {
				t.Fatal("record must preserve occurrence time")
			}

			payment := &paymentv1.Payment{
				Id:            snapshot.PaymentID,
				ReservationId: snapshot.ReservationID,
				UserId:        snapshot.UserID,
				Status:        test.protoStatus,
				CreatedAt:     timestamppb.New(snapshot.CreatedAt),
				ExpiresAt:     timestamppb.New(snapshot.ExpiresAt),
			}

			var want proto.Message
			switch test.eventType {
			case paymentcontract.PaymentSucceededType:
				want = &paymentv1.PaymentSucceeded{
					MessageId:  message.ID,
					Payment:    payment,
					OccurredAt: timestamppb.New(message.OccurredAt),
				}
			case paymentcontract.PaymentFailedType:
				want = &paymentv1.PaymentFailed{
					MessageId:  message.ID,
					Payment:    payment,
					OccurredAt: timestamppb.New(message.OccurredAt),
				}
			}

			decoded := proto.Clone(want)
			proto.Reset(decoded)

			if err := proto.Unmarshal(record.Value, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, want) {
				t.Fatalf("unexpected event: got=%v want=%v", decoded, want)
			}

			snapshot.Status = "PENDING"
			message.Payload, err = json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recordFromMessage(message); err == nil {
				t.Fatal("final payment event must reject a PENDING snapshot")
			}
		})
	}
}
