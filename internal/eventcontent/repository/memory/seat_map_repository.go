package memory

import (
	"context"
	"sync"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
)

type SeatMapRepository struct {
	mu       sync.RWMutex
	SeatsMap map[string]domain.SeatMap
}

func NewSeatMapRepository() *SeatMapRepository {
	return &SeatMapRepository{
		SeatsMap: make(map[string]domain.SeatMap),
	}
}

func (r *SeatMapRepository) Save(ctx context.Context, seatMap domain.SeatMap) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.SeatsMap[seatMap.ID] = seatMap
	return nil
}

func (r *SeatMapRepository) GetByID(ctx context.Context, id string) (domain.SeatMap, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if seatMap, inMap := r.SeatsMap[id]; inMap {
		return seatMap, nil
	}
	return domain.SeatMap{}, nil
}
