package domain

import "errors"

var (
	ErrVenueNotFound   = errors.New("venue not found")
	ErrHallNotFound    = errors.New("hall not found")
	ErrSeatMapNotFound = errors.New("seat map not found")
	ErrEventNotFound   = errors.New("event not found")
	ErrEventNotDraft   = errors.New("event must be in draft status")
)
