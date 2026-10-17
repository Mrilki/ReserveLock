package domain

import "time"

type EventStatus string

const (
	EventStatusDraft     EventStatus = "DRAFT"
	EventStatusPublished EventStatus = "PUBLISHED"
)

type Event struct {
	ID        string
	VenueID   string
	HallID    string
	SeatMapID string
	Name      string
	StartsAt  time.Time
	Status    EventStatus
	CreatedBy string
}

func (e *Event) Publish() error {
	if e.Status != EventStatusDraft {
		return ErrEventNotDraft
	}
	e.Status = EventStatusPublished
	return nil
}
