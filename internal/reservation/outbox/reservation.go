package outbox

import "time"

type ReservationSnapshot struct {
	ReservationID string    `json:"reservationId"`
	EventID       string    `json:"eventId"`
	UserID        string    `json:"userId"`
	SeatIDs       []string  `json:"seatIds"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
}
