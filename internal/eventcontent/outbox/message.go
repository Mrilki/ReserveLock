package outbox

import (
	"context"
	"time"
)

const EventPublishedType = "reservelock.eventcontent.v1.EventPublished"

type Message struct {
	ID         string
	EventType  string
	Key        string
	Payload    []byte
	OccurredAt time.Time
}

type Repository interface {
	ListPending(ctx context.Context, limit int) ([]Message, error)
	MarkPublished(ctx context.Context, id string, publishedAt time.Time) (bool, error)
}

type Publisher interface {
	Publish(ctx context.Context, message Message) error
}
