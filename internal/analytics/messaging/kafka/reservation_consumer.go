package kafka

import (
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	consumerGroup            = "reservelock.analytics.reservation-lifecycle.v1"
	consumerRebalanceTimeout = time.Minute
)

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

func reservationEventFromRecord(record *kgo.Record) (domain.ReservationEvent, error) {
	if record == nil || record.Topic != reservationcontract.ReservationLifecycleTopic {
		return domain.ReservationEvent{}, fmt.Errorf("unexpected reservation analytics topic")
	}

	var messageID string
	var reservation *reservationv1.Reservation
	var occurredAt *timestamppb.Timestamp
	var eventType domain.ReservationEventType
	var expectedStatus reservationv1.ReservationStatus

	switch headerValue(record, "event-type") {
	case reservationcontract.ReservationCreatedType:
		var message reservationv1.ReservationCreated
		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return domain.ReservationEvent{}, fmt.Errorf("decode reservation created: %w", err)
		}
		messageID = message.GetMessageId()
		reservation = message.GetReservation()
		occurredAt = message.GetOccurredAt()
		eventType = domain.ReservationEventTypeCreated
		expectedStatus = reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING

	case reservationcontract.ReservationConfirmedType:
		var message reservationv1.ReservationConfirmed
		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return domain.ReservationEvent{}, fmt.Errorf("decode reservation confirmed: %w", err)
		}
		messageID = message.GetMessageId()
		reservation = message.GetReservation()
		occurredAt = message.GetOccurredAt()
		eventType = domain.ReservationEventTypeConfirmed
		expectedStatus = reservationv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED

	case reservationcontract.ReservationExpiredType:
		var message reservationv1.ReservationExpired
		if err := proto.Unmarshal(record.Value, &message); err != nil {
			return domain.ReservationEvent{}, fmt.Errorf("decode reservation expired: %w", err)
		}
		messageID = message.GetMessageId()
		reservation = message.GetReservation()
		occurredAt = message.GetOccurredAt()
		eventType = domain.ReservationEventTypeExpired
		expectedStatus = reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED

	default:
		return domain.ReservationEvent{}, fmt.Errorf("unsupported reservation analytics event type")
	}

	if messageID == "" || reservation == nil || reservation.GetId() == "" {
		return domain.ReservationEvent{}, fmt.Errorf("incomplete reservation event identity")
	}
	if headerValue(record, "message_id") != messageID || string(record.Key) != reservation.GetId() {
		return domain.ReservationEvent{}, fmt.Errorf("reservation event identity mismatch for message %q", messageID)
	}
	if reservation.GetStatus() != expectedStatus {
		return domain.ReservationEvent{}, fmt.Errorf("unexpected reservation status for message %q: %s", messageID, reservation.GetStatus())
	}

	for _, timestamp := range []*timestamppb.Timestamp{
		reservation.GetCreatedAt(),
		reservation.GetExpiresAt(),
		occurredAt,
	} {
		if err := timestamp.CheckValid(); err != nil {
			return domain.ReservationEvent{}, fmt.Errorf("invalid reservation event timestamp: %w", err)
		}
	}

	return domain.ReservationEvent{
		MessageID:     messageID,
		EventID:       reservation.GetEventId(),
		ReservationID: reservation.GetId(),
		EventType:     eventType,
		SeatIDs:       append([]string(nil), reservation.GetSeatIds()...),
		CreatedAt:     reservation.GetCreatedAt().AsTime().UTC(),
		ExpiresAt:     reservation.GetExpiresAt().AsTime().UTC(),
		OccurredAt:    occurredAt.AsTime().UTC(),
	}, nil
}

func headerValue(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
