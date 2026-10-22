package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/Mrilki/ReserveLock/internal/platform/outbox"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type OutboxRepository struct {
	collection *mongo.Collection
}

func NewOutboxRepository(database *mongo.Database) *OutboxRepository {
	return &OutboxRepository{
		collection: database.Collection(outboxCollectionName),
	}
}

func (r *OutboxRepository) ListPending(ctx context.Context, limit int) ([]outbox.Message, error) {
	cursor, err := r.collection.Find(
		ctx,
		bson.M{
			"published_at": nil,
		},
		options.Find().
			SetSort(bson.D{
				{Key: "occurred_at", Value: 1},
				{Key: "_id", Value: 1},
			}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, fmt.Errorf("find pending outbox messages: %w", err)
	}
	defer func() {
		_ = cursor.Close(ctx)
	}()

	var documents []outboxDocument

	if err := cursor.All(ctx, &documents); err != nil {
		return nil, fmt.Errorf("decode pending outbox messages: %w", err)
	}

	messages := make([]outbox.Message, 0, len(documents))
	for _, document := range documents {
		messages = append(messages, outbox.Message{
			ID:         document.ID,
			EventType:  document.EventType,
			Key:        document.Key,
			Payload:    document.Payload,
			OccurredAt: document.OccurredAt,
		})
	}

	return messages, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id string, publishedAt time.Time) (bool, error) {
	result, err := r.collection.UpdateOne(
		ctx,
		bson.M{
			"_id":          id,
			"published_at": nil,
		},
		bson.M{
			"$set": bson.M{
				"published_at": publishedAt,
			},
		},
	)
	if err != nil {
		return false, fmt.Errorf("mark outbox message published: %w", err)
	}

	return result.ModifiedCount == 1, nil
}

func (r *OutboxRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "published_at", Value: 1},
			{Key: "occurred_at", Value: 1},
			{Key: "_id", Value: 1},
		},
		Options: options.Index().SetName("outbox_pending"),
	})
	if err != nil {
		return fmt.Errorf("create outbox pending index: %w", err)
	}

	return nil
}
