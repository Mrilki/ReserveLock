package application

import "errors"

var (
	ErrInvalidVenueID   = errors.New("venue id is required")
	ErrInvalidVenueName = errors.New("venue name is required")
	ErrInvalidHallID    = errors.New("hall id is required")
	ErrInvalidHallName  = errors.New("hall name is required")
)
