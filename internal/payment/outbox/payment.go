package outbox

import "time"

type PaymentSnapshot struct {
	PaymentID     string    `json:"paymentId"`
	ReservationID string    `json:"reservationId"`
	UserID        string    `json:"userId"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
}
