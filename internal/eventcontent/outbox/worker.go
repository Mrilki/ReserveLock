package outbox

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	defaultBatchSize = 100
	defaultInterval  = time.Second
)

type Worker struct {
	repository Repository
	publisher  Publisher
	logger     *zap.Logger
}

func NewWorker(repository Repository, publisher Publisher, logger *zap.Logger) *Worker {
	return &Worker{
		repository: repository,
		publisher:  publisher,
		logger:     logger,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(defaultInterval)
	defer ticker.Stop()

	for {
		if err := w.processBatch(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("failed to process outbox batch", zap.Error(err))
		}

		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) error {
	messages, err := w.repository.ListPending(ctx, defaultBatchSize)
	if err != nil {
		return fmt.Errorf("list pending outbox messages: %w", err)
	}

	for _, message := range messages {
		if err := w.publisher.Publish(ctx, message); err != nil {
			return fmt.Errorf("publish outbox message %q: %w", message.ID, err)
		}

		publishedAt := time.Now().UTC()

		marked, err := w.repository.MarkPublished(ctx, message.ID, publishedAt)
		if err != nil {
			return fmt.Errorf("mark outbox message %q published: %w", message.ID, err)
		}

		if !marked {
			w.logger.Warn("outbox message was already marked published", zap.String("message_id", message.ID))
		}
	}

	return nil
}
