package application

import "errors"

var (
	ErrInvalidMessageID             = errors.New("invalid message id")
	ErrInvalidEventID               = errors.New("invalid event id")
	ErrInvalidReservationID         = errors.New("invalid reservation id")
	ErrInvalidReservationEventType  = errors.New("invalid reservation event type")
	ErrSeatIDsRequired              = errors.New("seat ids are required")
	ErrInvalidSeatID                = errors.New("invalid seat id")
	ErrDuplicateSeatID              = errors.New("duplicate seat id")
	ErrInvalidReservationTimestamps = errors.New("invalid reservation timestamps")
	ErrInvalidOccurrenceTime        = errors.New("invalid occurrence time")
)
