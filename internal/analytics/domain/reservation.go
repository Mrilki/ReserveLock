package domain

import "time"

type ReservationEventType string

const (
	ReservationEventTypeCreated   ReservationEventType = "CREATED"
	ReservationEventTypeConfirmed ReservationEventType = "CONFIRMED"
	ReservationEventTypeExpired   ReservationEventType = "EXPIRED"
)

type ReservationEvent struct {
	MessageID     string
	EventID       string
	ReservationID string
	EventType     ReservationEventType
	SeatIDs       []string
	CreatedAt     time.Time
	ExpiresAt     time.Time
	OccurredAt    time.Time
}

type EventSummary struct {
	EventID                 string
	Created                 uint64
	Confirmed               uint64
	Expired                 uint64
	CreatedSeats            uint64
	ConfirmedSeats          uint64
	ExpiredSeats            uint64
	ConfirmationRatePercent float64
	ExpirationRatePercent   float64
	AvgConfirmationSeconds  float64
	AvgExpirationLagSeconds float64
	Daily                   []EventDailySummary
}

type EventDailySummary struct {
	Date           string
	Created        uint64
	Confirmed      uint64
	Expired        uint64
	CreatedSeats   uint64
	ConfirmedSeats uint64
	ExpiredSeats   uint64
}
