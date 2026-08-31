package memory

import (
	"context"
	"sync"

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
