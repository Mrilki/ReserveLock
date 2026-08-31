package domain

import "time"

type ReservationStatus string

const (
	ReservationStatusPending   ReservationStatus = "PENDING"
	ReservationStatusConfirmed ReservationStatus = "CONFIRMED"
	ReservationStatusExpired   ReservationStatus = "EXPIRED"
)

type Reservation struct {
	ID        string
	EventID   string
	UserID    string
	SeatIDs   []string
	Status    ReservationStatus
	CreatedAt time.Time
	ExpiresAt time.Time
}
