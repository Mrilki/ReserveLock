package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"

	"go.uber.org/zap"
)

type repositoryStub struct {
	messages []Message
	marked   []string
	listErr  error
	markErr  error
}

func (r *repositoryStub) ListPending(ctx context.Context, limit int) ([]Message, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}

	return append([]Message(nil), r.messages...), nil
}

func (r *repositoryStub) MarkPublished(ctx context.Context, id string, publishedAt time.Time) (bool, error) {
	if r.markErr != nil {
		return false, r.markErr
	}

	r.marked = append(r.marked, id)
	return true, nil
}

type publisherStub struct {
	messages []Message
	err      error
}

func (p *publisherStub) Publish(ctx context.Context, message Message) error {
	if p.err != nil {
		return p.err
	}

	p.messages = append(p.messages, message)
	return nil
}

func TestWorkerProcessBatchPublishesAndMarksMessages(t *testing.T) {
	messages := []Message{
		{
			ID:         "message-1",
			EventType:  eventcontentcontract.EventPublishedType,
			Key:        "event-1",
			Payload:    []byte("payload-1"),
			OccurredAt: time.Now().UTC(),
		},
		{
			ID:         "message-2",
			EventType:  eventcontentcontract.EventPublishedType,
			Key:        "event-2",
			Payload:    []byte("payload-2"),
			OccurredAt: time.Now().UTC(),
		},
	}

	repository := &repositoryStub{
		messages: messages,
	}

	publisher := &publisherStub{}

	worker := NewWorker(repository, publisher, zap.NewNop())

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("process outbox batch: %v", err)
	}

	if len(publisher.messages) != 2 {
		t.Fatalf("expected 2 published messages, got %d", len(publisher.messages))
	}

	if len(repository.marked) != 2 {
		t.Fatalf("expected 2 marked messages, got %d", len(repository.marked))
	}
}

func TestWorkerProcessBatchDoesNotMarkMessageWhenPublishFails(t *testing.T) {
	repository := &repositoryStub{
		messages: []Message{
			{
				ID:         "message-1",
				EventType:  eventcontentcontract.EventPublishedType,
				Key:        "event-1",
				Payload:    []byte("payload"),
				OccurredAt: time.Now().UTC(),
			},
		},
	}

	publisher := &publisherStub{
		err: errors.New("kafka unavailable"),
	}

	worker := NewWorker(repository, publisher, zap.NewNop())

	err := worker.processBatch(context.Background())
	if err == nil {
		t.Fatal("expected process batch to fail")
	}

	if len(repository.marked) != 0 {
		t.Fatalf("expected no marked messages, got %d", len(repository.marked))
	}
}
