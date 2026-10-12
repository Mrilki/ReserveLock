package kafka

import (
	"context"
	"testing"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPaymentConsumerProcessRecord(t *testing.T) {
	tests := []struct {
		name              string
		eventType         string
		paymentStatus     paymentv1.PaymentStatus
		reservationStatus domain.ReservationStatus
		seatStatus        domain.SeatInventoryStatus
	}{
		{
			name:              "succeeded",
			eventType:         paymentcontract.PaymentSucceededType,
			paymentStatus:     paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED,
			reservationStatus: domain.ReservationStatusConfirmed,
			seatStatus:        domain.SeatInventoryStatusReserved,
		},
		{
			name:              "failed",
			eventType:         paymentcontract.PaymentFailedType,
			paymentStatus:     paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED,
			reservationStatus: domain.ReservationStatusPending,
			seatStatus:        domain.SeatInventoryStatusHeld,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := memory.NewStore()
			service := application.NewService(store, nil)

			_, err := service.HandleEventPublished(ctx, application.EventPublishedInput{
				MessageID: "inventory-message",
				EventID:   "event-1",
				SeatIDs:   []string{"seat-1"},
			})
			if err != nil {
				t.Fatal(err)
			}

			reservation, err := service.CreateReservation(ctx, application.CreateReservationInput{
				EventID: "event-1",
				UserID:  "user-1",
				SeatIDs: []string{"seat-1"},
			})
			if err != nil {
				t.Fatal(err)
			}

			payment := &paymentv1.Payment{
				Id:            "payment-1",
				ReservationId: reservation.ID,
				UserId:        reservation.UserID,
				Status:        test.paymentStatus,
				CreatedAt:     timestamppb.New(reservation.CreatedAt),
				ExpiresAt:     timestamppb.New(reservation.ExpiresAt),
			}

			var message proto.Message
			switch test.eventType {
			case paymentcontract.PaymentSucceededType:
				message = &paymentv1.PaymentSucceeded{
					MessageId:  "payment-message",
					Payment:    payment,
					OccurredAt: timestamppb.Now(),
				}
			case paymentcontract.PaymentFailedType:
				message = &paymentv1.PaymentFailed{
					MessageId:  "payment-message",
					Payment:    payment,
					OccurredAt: timestamppb.Now(),
				}
			}

			payload, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}

			record := &kgo.Record{
				Topic: paymentcontract.PaymentLifecycleTopic,
				Key:   []byte(reservation.ID),
				Value: payload,
				Headers: []kgo.RecordHeader{
					{Key: "message_id", Value: []byte("payment-message")},
					{Key: "event-type", Value: []byte(test.eventType)},
				},
			}

			consumer := NewPaymentConsumer(nil, service, zap.NewNop())

			processed, err := consumer.handleRecord(ctx, record)
			if err != nil || !processed {
				t.Fatalf("first processing: processed=%v err=%v", processed, err)
			}

			processed, err = consumer.handleRecord(ctx, record)
			if err != nil || processed {
				t.Fatalf("duplicate processing: processed=%v err=%v", processed, err)
			}

			stored, err := service.GetReservation(ctx, reservation.ID)
			if err != nil || stored.Status != test.reservationStatus {
				t.Fatalf("unexpected reservation: reservation=%+v err=%v", stored, err)
			}

			seats, err := store.ListSeatInventory(ctx, reservation.EventID)
			if err != nil || len(seats) != 1 {
				t.Fatalf("unexpected inventory: seats=%+v err=%v", seats, err)
			}
			if seats[0].Status != test.seatStatus {
				t.Fatalf("expected seat status %s, got %s", test.seatStatus, seats[0].Status)
			}
			if seats[0].HeldUntil.IsZero() != (test.seatStatus == domain.SeatInventoryStatusReserved) {
				t.Fatalf("unexpected heldUntil: %v", seats[0].HeldUntil)
			}

			record.Key = []byte("another-reservation")
			if _, err := consumer.handleRecord(ctx, record); err == nil {
				t.Fatal("mismatched record key must be rejected")
			}

			record.Key = []byte(reservation.ID)
			if test.eventType == paymentcontract.PaymentSucceededType {
				record.Headers[1].Value = []byte(paymentcontract.PaymentFailedType)
			} else {
				record.Headers[1].Value = []byte(paymentcontract.PaymentSucceededType)
			}

			if _, err := consumer.handleRecord(ctx, record); err == nil {
				t.Fatal("event type and payment status mismatch must be rejected")
			}
		})
	}
}
