package application

import "errors"

var (
	ErrInvalidEventID       = errors.New("invalid event id")
	ErrSeatIDsRequired      = errors.New("seat ids are required")
	ErrInvalidSeatID        = errors.New("invalid seat id")
	ErrInvalidReservationID = errors.New("invalid reservation id")
)
