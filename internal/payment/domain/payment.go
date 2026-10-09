package domain

import "time"

type PaymentStatus string

const (
	PaymentStatusPending   PaymentStatus = "PENDING"
	PaymentStatusSucceeded PaymentStatus = "SUCCEEDED"
	PaymentStatusFailed    PaymentStatus = "FAILED"
	PaymentStatusCancelled PaymentStatus = "CANCELLED"
)

type Payment struct {
	ID            string
	ReservationID string
	UserID        string
	Status        PaymentStatus
	CreatedAt     time.Time
	ExpiresAt     time.Time
}
