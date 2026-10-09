package application

import (
	"context"
	"time"

	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	"github.com/google/uuid"
)

type Store interface {
	ProcessReservationCreated(ctx context.Context, messageID string, payment domain.Payment) (bool, error)
	ProcessReservationExpired(ctx context.Context, messageID, reservationID string) (bool, error)
	GetPaymentByReservationID(ctx context.Context, reservationID string) (domain.Payment, error)
	CompletePayment(ctx context.Context, reservationID string, paymentStatus domain.PaymentStatus) (domain.Payment, error)
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

type ReservationCreatedInput struct {
	MessageID     string
	ReservationID string
	UserID        string
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type SimulatePaymentInput struct {
	ReservationID string
	Status        domain.PaymentStatus
}

func (s *Service) HandleReservationCreated(ctx context.Context, input ReservationCreatedInput) (bool, error) {
	if input.MessageID == "" {
		return false, ErrInvalidMessageID
	}
	if input.ReservationID == "" {
		return false, ErrInvalidReservationID
	}
	if input.UserID == "" {
		return false, ErrInvalidUserID
	}
	if input.CreatedAt.IsZero() || !input.ExpiresAt.After(input.CreatedAt) {
		return false, ErrInvalidReservationTimestamps
	}

	payment := domain.Payment{
		ID:            uuid.NewString(),
		ReservationID: input.ReservationID,
		UserID:        input.UserID,
		Status:        domain.PaymentStatusPending,
		CreatedAt:     input.CreatedAt.UTC(),
		ExpiresAt:     input.ExpiresAt.UTC(),
	}

	return s.store.ProcessReservationCreated(ctx, input.MessageID, payment)
}

func (s *Service) HandleReservationExpired(ctx context.Context, messageID string, reservationID string) (bool, error) {
	if messageID == "" {
		return false, ErrInvalidMessageID
	}
	if reservationID == "" {
		return false, ErrInvalidReservationID
	}

	return s.store.ProcessReservationExpired(ctx, messageID, reservationID)
}

func (s *Service) GetPayment(ctx context.Context, reservationID string) (domain.Payment, error) {
	if reservationID == "" {
		return domain.Payment{}, ErrInvalidReservationID
	}

	return s.store.GetPaymentByReservationID(ctx, reservationID)
}

func (s *Service) SimulatePayment(ctx context.Context, input SimulatePaymentInput) (domain.Payment, error) {
	if input.ReservationID == "" {
		return domain.Payment{}, ErrInvalidReservationID
	}
	if input.Status != domain.PaymentStatusFailed && input.Status != domain.PaymentStatusSucceeded {
		return domain.Payment{}, ErrInvalidPaymentStatus
	}

	return s.store.CompletePayment(ctx, input.ReservationID, input.Status)
}
