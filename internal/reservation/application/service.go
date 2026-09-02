package application

import (
	"context"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type InventoryRepository interface {
	Initialize(
		ctx context.Context,
		eventID string,
		seats []domain.SeatInventory,
	) error
	Hold(
		ctx context.Context,
		eventID string,
		seatIDs []string,
		reservationID string,
		heldUntil time.Time,
	) error

	Release(
		ctx context.Context,
		eventID string,
		seatIDs []string,
		reservationID string,
	) error
}

type ReservationRepository interface {
	Save(ctx context.Context, reservation domain.Reservation) error
	GetByID(ctx context.Context, id string) (domain.Reservation, error)
	ListExpiredPending(ctx context.Context, now time.Time) ([]domain.Reservation, error)
}

type Service struct {
	inventoryRepository   InventoryRepository
	reservationRepository ReservationRepository
}

func NewService(
	inventoryRepository InventoryRepository,
	reservationRepository ReservationRepository,
) *Service {
	return &Service{
		inventoryRepository:   inventoryRepository,
		reservationRepository: reservationRepository,
	}
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

	seats := make(
		[]domain.SeatInventory,
		0,
		len(input.SeatIDs),
	)

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

	return s.inventoryRepository.Initialize(
		ctx,
		input.EventID,
		seats,
	)
}
