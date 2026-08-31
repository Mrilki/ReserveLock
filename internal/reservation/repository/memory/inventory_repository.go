package memory

import (
	"context"
	"sync"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type InventoryRepository struct {
	mu        sync.RWMutex
	inventory map[string]map[string]domain.SeatInventory
}

func NewInventoryRepository() *InventoryRepository {
	return &InventoryRepository{
		inventory: make(
			map[string]map[string]domain.SeatInventory,
		),
	}
}

func (r *InventoryRepository) Initialize(
	ctx context.Context,
	eventID string,
	seats []domain.SeatInventory,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.inventory[eventID]; exists {
		return domain.ErrInventoryAlreadyExists
	}

	eventInventory := make(
		map[string]domain.SeatInventory,
		len(seats),
	)

	for _, seat := range seats {
		eventInventory[seat.SeatID] = seat
	}

	r.inventory[eventID] = eventInventory

	return nil

}

func (r *InventoryRepository) Hold(
	ctx context.Context,
	eventID string,
	seatIDs []string,
	reservationID string,
	heldUntil time.Time,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventInventory, ok := r.inventory[eventID]
	if !ok {
		return domain.ErrInventoryNotFound
	}

	for _, seatID := range seatIDs {
		seat, ok := eventInventory[seatID]
		if !ok {
			return domain.ErrSeatNotFound
		}

		if seat.Status != domain.SeatInventoryStatusAvailable {
			return domain.ErrSeatUnavailable
		}
	}

	for _, seatID := range seatIDs {
		seat := eventInventory[seatID]

		seat.Status = domain.SeatInventoryStatusHeld
		seat.ReservationID = reservationID
		seat.HeldUntil = heldUntil

		eventInventory[seatID] = seat
	}

	return nil
}

func (r *InventoryRepository) Release(
	ctx context.Context,
	eventID string,
	seatIDs []string,
	reservationID string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventInventory, ok := r.inventory[eventID]
	if !ok {
		return domain.ErrInventoryNotFound
	}

	for _, seatID := range seatIDs {
		seat, ok := eventInventory[seatID]
		if !ok {
			return domain.ErrSeatNotFound
		}

		if seat.ReservationID != reservationID {
			continue
		}

		seat.Status = domain.SeatInventoryStatusAvailable
		seat.ReservationID = ""
		seat.HeldUntil = time.Time{}

		eventInventory[seatID] = seat
	}

	return nil
}
