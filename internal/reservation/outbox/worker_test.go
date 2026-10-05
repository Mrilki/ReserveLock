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
	marked    []string
	markCalls int
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

	r.marked = append(r.marked, id)
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

func TestWorkerProcessBatch(t *testing.T) {
	failure := errors.New("injected failure")

	tests := []struct {
		name          string
		publishErr    error
		markErr       error
		wantErr       bool
		wantMarkCalls int
		wantMarked    int
	}{
		{"success", nil, nil, false, 1, 1},
		{"publish failure", failure, nil, true, 0, 0},
		{"mark failure", nil, failure, true, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &repositoryStub{
				messages: []Message{{ID: "message-1"}},
				markErr:  tt.markErr,
			}
			publisher := &publisherStub{err: tt.publishErr}
			worker := NewWorker(repository, publisher, zap.NewNop())

			err := worker.processBatch(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(publisher.messages) != 1 {
				t.Fatalf("expected one publish attempt, got %d", len(publisher.messages))
			}
			if repository.markCalls != tt.wantMarkCalls || len(repository.marked) != tt.wantMarked {
				t.Fatalf("unexpected marking: calls=%d marked=%d", repository.markCalls, len(repository.marked))
			}
		})
	}
}

func TestWorkerRetriesWithSameMessageID(t *testing.T) {
	repository := &repositoryStub{
		messages: []Message{{ID: "persisted-message-1"}},
		markErr:  errors.New("postgres unavailable after kafka acknowledgement"),
	}
	publisher := &publisherStub{}
	worker := NewWorker(repository, publisher, zap.NewNop())

	if err := worker.processBatch(context.Background()); err == nil {
		t.Fatal("expected marking failure")
	}

	repository.markErr = nil

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("retry batch: %v", err)
	}

	if len(publisher.messages) != 2 {
		t.Fatalf("expected two publish attempts, got %d", len(publisher.messages))
	}
	if publisher.messages[0].ID != "persisted-message-1" || publisher.messages[1].ID != publisher.messages[0].ID {
		t.Fatal("retry must retain the persisted message ID")
	}
	if len(repository.marked) != 1 {
		t.Fatal("expected one successful mark")
	}
}
