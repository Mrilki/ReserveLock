package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const eventsCollection = "events"

type EventRepository struct {
	collection *mongo.Collection
}

func NewEventRepository(database *mongo.Database) *EventRepository {
	return &EventRepository{
		collection: database.Collection(eventsCollection),
	}
}

func (r *EventRepository) Create(
	ctx context.Context,
	event domain.Event,
) error {
	_, err := r.collection.InsertOne(
		ctx,
		toEventDocument(event),
	)
	if err != nil {
		return fmt.Errorf(
			"insert event: %w",
			err,
		)
	}

	return nil
}

func (r *EventRepository) GetByID(
	ctx context.Context,
	id string,
) (domain.Event, error) {
	var document eventDocument

	err := r.collection.FindOne(
		ctx,
		bson.M{
			"_id": id,
		},
	).Decode(&document)

	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return domain.Event{},
			domain.ErrEventNotFound

	case err != nil:
		return domain.Event{}, fmt.Errorf(
			"find event by id: %w",
			err,
		)
	}

	return document.toDomain(), nil
}

func (r *EventRepository) Update(
	ctx context.Context,
	event domain.Event,
) error {
	result, err := r.collection.ReplaceOne(
		ctx,
		bson.M{
			"_id": event.ID,
		},
		toEventDocument(event),
	)
	if err != nil {
		return fmt.Errorf(
			"replace event: %w",
			err,
		)
	}

	if result.MatchedCount == 0 {
		return domain.ErrEventNotFound
	}

	return nil
}

func (r *EventRepository) List(
	ctx context.Context,
) ([]domain.Event, error) {
	cursor, err := r.collection.Find(
		ctx,
		bson.D{},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"find events: %w",
			err,
		)
	}
	var documents []eventDocument

	if err := cursor.All(
		ctx,
		&documents,
	); err != nil {
		return nil, fmt.Errorf(
			"decode events: %w",
			err,
		)
	}

	events := make([]domain.Event, 0, len(documents))
	for _, event := range documents {
		events = append(events, event.toDomain())
	}
	return events, nil
}
