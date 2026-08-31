package domain

import "errors"

var (
	ErrReservationNotFound = errors.New("reservation not found")
	ErrInventoryNotFound   = errors.New("inventory not found")
)
