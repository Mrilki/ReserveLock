package domain

import "time"

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

type Payment struct {
	ID            string
	ReservationID string
	UserID        string
	Status        Status
	CreatedAt     time.Time
	ExpiresAt     time.Time
}
