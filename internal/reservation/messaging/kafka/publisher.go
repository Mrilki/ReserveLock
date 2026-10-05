package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationoutbox "github.com/Mrilki/ReserveLock/internal/reservation/outbox"
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

func (p *Publisher) Publish(ctx context.Context, message reservationoutbox.Message) error {
	record, err := recordFromMessage(message)
	if err != nil {
		return err
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce reservation message %q: %w", message.ID, err)
	}

	return nil
}

func recordFromMessage(message reservationoutbox.Message) (*kgo.Record, error) {
	var snapshot reservationoutbox.ReservationSnapshot

	if err := json.Unmarshal(message.Payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode reservation outbox snapshot: %w", err)
	}

	if message.ID == "" || message.Key == "" || snapshot.ReservationID != message.Key {
		return nil, fmt.Errorf("invalid reservation outbox identity for message %q", message.ID)
	}

	if snapshot.EventID == "" || snapshot.UserID == "" || len(snapshot.SeatIDs) == 0 {
		return nil, fmt.Errorf("incomplete reservation outbox snapshot for message %q", message.ID)
	}

	if snapshot.CreatedAt.IsZero() || !snapshot.ExpiresAt.After(snapshot.CreatedAt) || message.OccurredAt.IsZero() {
		return nil, fmt.Errorf("invalid reservation outbox timestamps for message %q", message.ID)
	}

	reservation := &reservationv1.Reservation{
		Id:        snapshot.ReservationID,
		EventId:   snapshot.EventID,
		UserId:    snapshot.UserID,
		SeatIds:   snapshot.SeatIDs,
		CreatedAt: timestamppb.New(snapshot.CreatedAt),
		ExpiresAt: timestamppb.New(snapshot.ExpiresAt),
	}
	occurredAt := timestamppb.New(message.OccurredAt)

	for _, timestamp := range []*timestamppb.Timestamp{reservation.CreatedAt, reservation.ExpiresAt, occurredAt} {
		if err := timestamp.CheckValid(); err != nil {
			return nil, fmt.Errorf("invalid reservation protobuf timestamp: %w", err)
		}
	}

	var event proto.Message
	var expectedStatus domain.ReservationStatus

	switch message.EventType {
	case reservationcontract.ReservationCreatedType:
		expectedStatus = domain.ReservationStatusPending
		reservation.Status = reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING
		event = &reservationv1.ReservationCreated{
			MessageId:   message.ID,
			Reservation: reservation,
			OccurredAt:  occurredAt,
		}

	case reservationcontract.ReservationExpiredType:
		expectedStatus = domain.ReservationStatusExpired
		reservation.Status = reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED
		event = &reservationv1.ReservationExpired{
			MessageId:   message.ID,
			Reservation: reservation,
			OccurredAt:  occurredAt,
		}

	case reservationcontract.ReservationConfirmedType:
		expectedStatus = domain.ReservationStatusConfirmed
		reservation.Status = reservationv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED
		event = &reservationv1.ReservationConfirmed{
			MessageId:   message.ID,
			Reservation: reservation,
			OccurredAt:  occurredAt,
		}

	default:
		return nil, fmt.Errorf("unsupported reservation event type %q", message.EventType)
	}

	if snapshot.Status != string(expectedStatus) {
		return nil, fmt.Errorf("reservation event %q requires status %s, got %q", message.EventType, expectedStatus, snapshot.Status)
	}

	payload, err := proto.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode reservation event: %w", err)
	}

	return &kgo.Record{
		Topic:     reservationcontract.ReservationLifecycleTopic,
		Key:       []byte(message.Key),
		Value:     payload,
		Timestamp: message.OccurredAt,
		Headers: []kgo.RecordHeader{
			{Key: "message_id", Value: []byte(message.ID)},
			{Key: "event-type", Value: []byte(message.EventType)},
		},
	}, nil
}
