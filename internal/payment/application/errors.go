package application

import "errors"

var (
	ErrInvalidReservationEvent = errors.New("invalid reservation event")
	ErrInvalidReservationID    = errors.New("invalid reservation id")
)
