package application

import (
	"context"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type InventoryRepository interface {
	Initialize(
		ctx context.Context,
		eventID string,
		seats []domain.SeatInventory,
	) error
}

type ReservationRepository interface {
	Save(ctx context.Context, reservation domain.Reservation) error
	GetByID(ctx context.Context, id string) (domain.Reservation, error)
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

	for _, seatID := range input.SeatIDs {
		if seatID == "" {
			return ErrInvalidSeatID
		}

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

func (s *Service) GetReservation(
	ctx context.Context,
	reservationID string,
) (domain.Reservation, error) {
	if reservationID == "" {
		return domain.Reservation{}, ErrInvalidReservationID
	}

	return s.reservationRepository.GetByID(
		ctx,
		reservationID,
	)
}
