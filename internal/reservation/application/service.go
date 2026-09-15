package application

import (
	"context"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type Store interface {
	InitializeInventory(ctx context.Context, eventID string, seats []domain.SeatInventory) error
	CreateReservation(ctx context.Context, reservation domain.Reservation) error
	GetReservationByID(ctx context.Context, id string) (domain.Reservation, error)
	ListExpiredPendingIDs(ctx context.Context, now time.Time, limit int) ([]string, error)
	ExpireReservation(ctx context.Context, id string, now time.Time) (bool, error)
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

type InitializeInventoryInput struct {
	EventID string
	SeatIDs []string
}

func (s *Service) InitializeInventory(ctx context.Context, input InitializeInventoryInput) error {
	if input.EventID == "" {
		return ErrInvalidEventID
	}
	if len(input.SeatIDs) == 0 {
		return ErrSeatIDsRequired
	}

	seats := make([]domain.SeatInventory, 0, len(input.SeatIDs))

	seen := make(map[string]struct{}, len(input.SeatIDs))

	for _, seatID := range input.SeatIDs {
		if seatID == "" {
			return ErrInvalidSeatID
		}

		if _, exists := seen[seatID]; exists {
			return ErrDuplicateSeatID
		}

		seen[seatID] = struct{}{}

		seats = append(seats, domain.SeatInventory{
			EventID: input.EventID,
			SeatID:  seatID,
			Status:  domain.SeatInventoryStatusAvailable,
		})
	}

	return s.store.InitializeInventory(ctx, input.EventID, seats)
}
