package application

import "errors"

var (
	ErrInvalidMessageID             = errors.New("invalid message id")
	ErrInvalidReservationID         = errors.New("invalid reservation id")
	ErrInvalidUserID                = errors.New("invalid user id")
	ErrInvalidReservationTimestamps = errors.New("invalid reservation timestamps")
	ErrInvalidPaymentStatus         = errors.New("invalid payment status")
)
