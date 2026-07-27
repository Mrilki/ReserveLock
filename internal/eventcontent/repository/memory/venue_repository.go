package memory

import (
	"context"
	"sync"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
)

type VenueRepository struct {
	mu     sync.RWMutex
	venues map[string]domain.Venue
}

func NewVenueRepository() *VenueRepository {
	return &VenueRepository{
		venues: make(map[string]domain.Venue),
	}
}

func (r *VenueRepository) Save(ctx context.Context, venue domain.Venue) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.venues[venue.ID] = venue
	return nil
}

func (r *VenueRepository) GetByID(ctx context.Context, id string) (domain.Venue, error) {
	r.mu.RLock()
	defer r.mu.Unlock()

	if venue, inMap := r.venues[id]; inMap {
		return venue, nil
	}
	return domain.Venue{}, domain.ErrVenueNotFound
}
