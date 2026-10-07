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

func (s *Service) HandleReservationCreated(ctx context.Context, input ReservationCreatedInput) (bool, error) {
	if input.MessageID == "" || input.ReservationID == "" || input.UserID == "" {
		return false, ErrInvalidReservationEvent
	}

	if input.CreatedAt.IsZero() || !input.ExpiresAt.After(input.CreatedAt) {
		return false, ErrInvalidReservationEvent
	}

	payment := domain.Payment{
		ID:            uuid.NewString(),
		ReservationID: input.ReservationID,
		UserID:        input.UserID,
		Status:        domain.StatusPending,
		CreatedAt:     input.CreatedAt.UTC(),
		ExpiresAt:     input.ExpiresAt.UTC(),
	}

	return s.store.ProcessReservationCreated(ctx, input.MessageID, payment)
}

func (s *Service) HandleReservationExpired(ctx context.Context, messageID string, reservationID string) (bool, error) {
	if messageID == "" || reservationID == "" {
		return false, ErrInvalidReservationEvent
	}

	return s.store.ProcessReservationExpired(ctx, messageID, reservationID)
}

func (s *Service) GetPayment(ctx context.Context, reservationID string) (domain.Payment, error) {
	if reservationID == "" {
		return domain.Payment{}, ErrInvalidReservationID
	}

	return s.store.GetPaymentByReservationID(ctx, reservationID)
}
