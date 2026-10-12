package application

import (
	"context"
)

type PaymentResultInput struct {
	MessageID     string
	ReservationID string
	UserID        string
}

func (s *Service) HandlePaymentSucceeded(ctx context.Context, input PaymentResultInput) (bool, error) {
	if err := validatePaymentResultInput(input); err != nil {
		return false, err
	}

	return s.store.ProcessPaymentSucceeded(ctx, input.MessageID, input.ReservationID, input.UserID)
}

func (s *Service) HandlePaymentFailed(ctx context.Context, input PaymentResultInput) (bool, error) {
	if err := validatePaymentResultInput(input); err != nil {
		return false, err
	}

	return s.store.ProcessPaymentFailed(ctx, input.MessageID, input.ReservationID, input.UserID)
}

func validatePaymentResultInput(input PaymentResultInput) error {
	if input.MessageID == "" {
		return ErrInvalidMessageID
	}
	if input.ReservationID == "" {
		return ErrInvalidReservationID
	}
	if input.UserID == "" {
		return ErrInvalidUserID
	}

	return nil
}
