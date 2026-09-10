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

func (r *EventRepository) Create(ctx context.Context, event domain.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events[event.ID] = event

	return nil
}

func (r *EventRepository) Update(ctx context.Context, event domain.Event) error {
	r.mu.Lock()
	r.mu.Unlock()

	if _, exists := r.events[event.ID]; !exists {
		return domain.ErrEventNotFound
	}

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

func (r *EventRepository) List(ctx context.Context) ([]domain.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	events := make([]domain.Event, 0, len(r.events))

	for _, event := range r.events {
		events = append(events, event)
	}

	return events, nil
}
