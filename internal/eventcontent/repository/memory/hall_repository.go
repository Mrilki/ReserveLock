package memory

import (
	"context"
	"sync"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
)

type HallRepository struct {
	mu    sync.RWMutex
	halls map[string]domain.Hall
}

func NewHallRepository() *HallRepository {
	return &HallRepository{
		halls: make(map[string]domain.Hall),
	}
}

func (r *HallRepository) Create(ctx context.Context, hall domain.Hall) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.halls[hall.ID] = hall
	return nil
}

func (r *HallRepository) GetByID(ctx context.Context, id string) (domain.Hall, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if hall, inMap := r.halls[id]; inMap {
		return hall, nil
	}
	return domain.Hall{}, domain.ErrHallNotFound
}
