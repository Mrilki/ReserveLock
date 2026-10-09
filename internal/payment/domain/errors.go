package domain

import "errors"

var (
	ErrPaymentNotFound      = errors.New("payment not found")
	ErrPaymentAlreadyExists = errors.New("payment already exists")
	ErrPaymentNotPending    = errors.New("payment is not pending")
	ErrPaymentExpired       = errors.New("payment has expired")
)
