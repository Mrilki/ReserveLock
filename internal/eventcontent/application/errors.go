package application

import "errors"

var (
	ErrInvalidVenueID   = errors.New("venue id is required")
	ErrInvalidVenueName = errors.New("venue name is required")
)
