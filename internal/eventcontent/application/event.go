package application

import (
	"context"
	"sort"
	"time"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"github.com/google/uuid"
)

type CreateEventInput struct {
	VenueID   string
	HallID    string
	SeatMapID string
	Name      string
	StartsAt  time.Time
	CreatedBy string
}

func (s *Service) CreateEvent(ctx context.Context, input CreateEventInput) (domain.Event, error) {
	if input.VenueID == "" {
		return domain.Event{}, ErrInvalidVenueID
	}
	if input.HallID == "" {
		return domain.Event{}, ErrInvalidHallID
	}
	if input.SeatMapID == "" {
		return domain.Event{}, ErrInvalidSeatMapID
	}
	if input.Name == "" {
		return domain.Event{}, ErrInvalidEventName
	}
	if input.StartsAt.IsZero() {
		return domain.Event{}, ErrInvalidEventStartsAt
	}

	venue, err := s.venueRepository.GetByID(ctx, input.VenueID)
	if err != nil {
		return domain.Event{}, err
	}

	hall, err := s.hallRepository.GetByID(ctx, input.HallID)
	if err != nil {
		return domain.Event{}, err
	}

	if venue.ID != hall.VenueID {
		return domain.Event{}, ErrHallVenueMismatch
	}
	seatMap, err := s.seatMapRepository.GetByID(ctx, input.SeatMapID)
	if err != nil {
		return domain.Event{}, err
	}
	if seatMap.HallID != hall.ID {
		return domain.Event{}, ErrSeatMapHallMismatch
	}

	event := domain.Event{
		ID:        uuid.NewString(),
		VenueID:   venue.ID,
		HallID:    hall.ID,
		SeatMapID: seatMap.ID,
		Name:      input.Name,
		StartsAt:  input.StartsAt.UTC(),
		Status:    domain.EventStatusDraft,
		CreatedBy: input.CreatedBy,
	}

	if err := s.eventRepository.Save(ctx, event); err != nil {
		return domain.Event{}, err
	}

	return event, nil
}

func (s *Service) GetEvent(ctx context.Context, eventID string) (domain.Event, error) {
	if eventID == "" {
		return domain.Event{}, ErrInvalidEventID
	}
	return s.eventRepository.GetByID(ctx, eventID)
}

func (s *Service) PublishEvent(ctx context.Context, eventID string) (domain.Event, error) {
	if eventID == "" {
		return domain.Event{}, ErrInvalidEventID
	}

	event, err := s.eventRepository.GetByID(ctx, eventID)
	if err != nil {
		return domain.Event{}, err
	}

	if err := event.Publish(); err != nil {
		return domain.Event{}, err
	}
	if err := s.eventRepository.Save(ctx, event); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}

func (s *Service) ListEvents(ctx context.Context) ([]domain.Event, error) {
	events, err := s.eventRepository.List(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].StartsAt.Before(events[j].StartsAt)
	})

	return events, nil
}
