package domain

import "time"

type SeatInventoryStatus string

const (
	SeatInventoryStatusAvailable SeatInventoryStatus = "AVAILABLE"
	SeatInventoryStatusHeld      SeatInventoryStatus = "HELD"
	SeatInventoryStatusReserved  SeatInventoryStatus = "RESERVED"
)

type SeatInventory struct {
	EventID       string
	SeatID        string
	Status        SeatInventoryStatus
	ReservationID string
	HeldUntil     time.Time
}
