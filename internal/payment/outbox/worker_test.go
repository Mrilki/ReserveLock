package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

type repositoryStub struct {
	messages  []Message
	markCalls int
	marked    int
	markErr   error
}

func (r *repositoryStub) ListPending(ctx context.Context, limit int) ([]Message, error) {
	return append([]Message(nil), r.messages...), nil
}

func (r *repositoryStub) MarkPublished(ctx context.Context, id string, publishedAt time.Time) (bool, error) {
	r.markCalls++
	if r.markErr != nil {
		return false, r.markErr
	}
	r.marked++
	return true, nil
}

type publisherStub struct {
	messages []Message
	err      error
}

func (p *publisherStub) Publish(ctx context.Context, message Message) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestWorkerDeliveryFailuresAndRetry(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected failure")
	repository := &repositoryStub{
		messages: []Message{{ID: "persisted-message-1"}},
	}
	publisher := &publisherStub{err: failure}
	worker := NewWorker(repository, publisher, zap.NewNop())

	if err := worker.processBatch(ctx); !errors.Is(err, failure) {
		t.Fatalf("expected publish failure, got %v", err)
	}
	if repository.markCalls != 0 {
		t.Fatal("failed publication must not mark the message")
	}

	publisher.err = nil
	repository.markErr = failure

	if err := worker.processBatch(ctx); !errors.Is(err, failure) {
		t.Fatalf("expected marking failure, got %v", err)
	}
	if repository.markCalls != 1 || repository.marked != 0 {
		t.Fatal("failed marker must leave the message pending")
	}

	repository.markErr = nil

	if err := worker.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if repository.markCalls != 2 || repository.marked != 1 {
		t.Fatal("expected one successful delivery marker")
	}
	if len(publisher.messages) != 3 {
		t.Fatalf("expected three publication attempts, got %d", len(publisher.messages))
	}
	for _, message := range publisher.messages {
		if message.ID != "persisted-message-1" {
			t.Fatal("every retry must preserve the stored message id")
		}
	}
}
