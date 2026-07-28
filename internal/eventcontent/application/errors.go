package application

import "errors"

var (
	ErrInvalidVenueID       = errors.New("venue id is required")
	ErrInvalidVenueName     = errors.New("venue name is required")
	ErrInvalidHallID        = errors.New("hall id is required")
	ErrInvalidHallName      = errors.New("hall name is required")
	ErrInvalidSeatMapID     = errors.New("seat map id is required")
	ErrInvalidSeatMapName   = errors.New("seat map name is required")
	ErrSeatMapSeatsRequired = errors.New("seat map must contain at least one seat")
	ErrInvalidSeatRow       = errors.New("seat row is required")
	ErrInvalidSeatNumber    = errors.New("seat number is required")
	ErrDuplicateSeat        = errors.New("duplicate seat")
)
