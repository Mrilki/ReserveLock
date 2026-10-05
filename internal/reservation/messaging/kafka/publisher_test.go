package kafka

import (
	"encoding/json"
	"testing"
	"time"

	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	reservationoutbox "github.com/Mrilki/ReserveLock/internal/reservation/outbox"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecordFromMessageEncodesLifecycleEvents(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		eventType   string
		status      string
		protoStatus reservationv1.ReservationStatus
	}{
		{"created", reservationcontract.ReservationCreatedType, "PENDING", reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING},
		{"expired", reservationcontract.ReservationExpiredType, "EXPIRED", reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED},
		{"confirmed", reservationcontract.ReservationConfirmedType, "CONFIRMED", reservationv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := reservationoutbox.ReservationSnapshot{
				ReservationID: "reservation-1",
				EventID:       "event-1",
				UserID:        "user-1",
				SeatIDs:       []string{"seat-2", "seat-1"},
				Status:        tt.status,
				CreatedAt:     now,
				ExpiresAt:     now.Add(10 * time.Minute),
			}

			payload, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}

			message := reservationoutbox.Message{
				ID:         "message-1",
				EventType:  tt.eventType,
				Key:        snapshot.ReservationID,
				Payload:    payload,
				OccurredAt: now.Add(time.Minute),
			}

			record, err := recordFromMessage(message)
			if err != nil {
				t.Fatalf("build record: %v", err)
			}

			if record.Topic != reservationcontract.ReservationLifecycleTopic || string(record.Key) != message.Key {
				t.Fatalf("unexpected routing: topic=%s key=%s", record.Topic, record.Key)
			}
			if !record.Timestamp.Equal(message.OccurredAt) {
				t.Fatal("record must retain occurrence time")
			}
			if len(record.Headers) != 2 ||
				record.Headers[0].Key != "message_id" || string(record.Headers[0].Value) != message.ID ||
				record.Headers[1].Key != "event-type" || string(record.Headers[1].Value) != message.EventType {
				t.Fatalf("unexpected headers: %+v", record.Headers)
			}

			reservation := &reservationv1.Reservation{
				Id:        snapshot.ReservationID,
				EventId:   snapshot.EventID,
				UserId:    snapshot.UserID,
				SeatIds:   snapshot.SeatIDs,
				Status:    tt.protoStatus,
				CreatedAt: timestamppb.New(snapshot.CreatedAt),
				ExpiresAt: timestamppb.New(snapshot.ExpiresAt),
			}
			occurredAt := timestamppb.New(message.OccurredAt)

			var want proto.Message
			switch tt.eventType {
			case reservationcontract.ReservationCreatedType:
				want = &reservationv1.ReservationCreated{MessageId: message.ID, Reservation: reservation, OccurredAt: occurredAt}
			case reservationcontract.ReservationExpiredType:
				want = &reservationv1.ReservationExpired{MessageId: message.ID, Reservation: reservation, OccurredAt: occurredAt}
			case reservationcontract.ReservationConfirmedType:
				want = &reservationv1.ReservationConfirmed{MessageId: message.ID, Reservation: reservation, OccurredAt: occurredAt}
			}

			decoded := proto.Clone(want)
			proto.Reset(decoded)

			if err := proto.Unmarshal(record.Value, decoded); err != nil {
				t.Fatalf("decode protobuf payload: %v", err)
			}
			if !proto.Equal(decoded, want) {
				t.Fatalf("unexpected event: got=%v want=%v", decoded, want)
			}
		})
	}
}

func TestRecordFromMessageRejectsStatusMismatch(t *testing.T) {
	now := time.Now().UTC()
	payload, err := json.Marshal(reservationoutbox.ReservationSnapshot{
		ReservationID: "reservation-1",
		EventID:       "event-1",
		UserID:        "user-1",
		SeatIDs:       []string{"seat-1"},
		Status:        "PENDING",
		CreatedAt:     now,
		ExpiresAt:     now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = recordFromMessage(reservationoutbox.Message{
		ID:         "message-1",
		EventType:  reservationcontract.ReservationExpiredType,
		Key:        "reservation-1",
		Payload:    payload,
		OccurredAt: now,
	})
	if err == nil {
		t.Fatal("EXPIRED event must reject a PENDING snapshot")
	}
}
