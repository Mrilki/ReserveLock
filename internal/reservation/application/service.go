package application

import (
	"context"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type AvailabilityReader interface {
	ListSeatInventory(ctx context.Context, eventID string) ([]domain.SeatInventory, error)
}

type Store interface {
	ProcessEventPublished(ctx context.Context, messageID string, eventID string, seats []domain.SeatInventory) (bool, error)
	ListSeatInventory(ctx context.Context, eventID string) ([]domain.SeatInventory, error)
	CreateReservation(ctx context.Context, reservation domain.Reservation) error
	GetReservationByID(ctx context.Context, id string) (domain.Reservation, error)
	ListExpiredPendingIDs(ctx context.Context, now time.Time, limit int) ([]string, error)
	ExpireReservation(ctx context.Context, id string, now time.Time) (bool, error)
}

type Service struct {
	store              Store
	availabilityReader AvailabilityReader
}

func NewService(store Store, availabilityReader AvailabilityReader) *Service {
	if availabilityReader == nil {
		availabilityReader = store
	}

	return &Service{
		store:              store,
		availabilityReader: availabilityReader,
	}
}

type EventPublishedInput struct {
	MessageID string
	EventID   string
	SeatIDs   []string
}

func (s *Service) HandleEventPublished(ctx context.Context, input EventPublishedInput) (bool, error) {
	if input.MessageID == "" {
		return false, ErrInvalidMessageID
	}
	if input.EventID == "" {
		return false, ErrInvalidEventID
	}
	if len(input.SeatIDs) == 0 {
		return false, ErrSeatIDsRequired
	}

	seats := make([]domain.SeatInventory, 0, len(input.SeatIDs))
	seen := make(map[string]struct{}, len(input.SeatIDs))

	for _, seatID := range input.SeatIDs {
		if seatID == "" {
			return false, ErrInvalidSeatID
		}

		if _, exists := seen[seatID]; exists {
			return false, ErrDuplicateSeatID
		}

		seen[seatID] = struct{}{}

		seats = append(seats, domain.SeatInventory{
			EventID: input.EventID,
			SeatID:  seatID,
			Status:  domain.SeatInventoryStatusAvailable,
		})
	}

	return s.store.ProcessEventPublished(ctx, input.MessageID, input.EventID, seats)
}
