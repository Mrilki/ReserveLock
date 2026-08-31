package domain

import "errors"

var (
	ErrReservationNotFound    = errors.New("reservation not found")
	ErrInventoryNotFound      = errors.New("inventory not found")
	ErrInventoryAlreadyExists = errors.New("inventory already exists")
	ErrSeatNotFound           = errors.New("seat not found")
	ErrSeatUnavailable        = errors.New("seat unavailable")
)
