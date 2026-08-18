package memory

import (
	"context"
	"sync"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
)

type EventRepository struct {
	mu     sync.RWMutex
	events map[string]domain.Event
}

func NewEventRepository() *EventRepository {
	return &EventRepository{
		events: make(map[string]domain.Event),
	}
}

func (r *EventRepository) Save(ctx context.Context, event domain.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events[event.ID] = event

	return nil
}

func (r *EventRepository) GetByID(ctx context.Context, id string) (domain.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if event, ok := r.events[id]; ok {
		return event, nil
	}
	return domain.Event{}, domain.ErrEventNotFound
}
