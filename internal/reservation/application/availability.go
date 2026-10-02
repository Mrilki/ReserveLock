package application

import (
	"context"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func (s *Service) GetAvailability(
	ctx context.Context,
	eventID string,
) ([]domain.SeatInventory, error) {
	if eventID == "" {
		return nil, ErrInvalidEventID
	}
	return s.store.ListSeatInventory(ctx, eventID)
}
