package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	eventcontentcontract "github.com/Mrilki/ReserveLock/internal/integration/eventcontent"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	eventsCollection     = "events"
	outboxCollectionName = "outbox"
)

type EventRepository struct {
	client           *mongo.Client
	collection       *mongo.Collection
	outboxCollection *mongo.Collection
}

func NewEventRepository(database *mongo.Database) *EventRepository {
	return &EventRepository{
		client:           database.Client(),
		collection:       database.Collection(eventsCollection),
		outboxCollection: database.Collection(outboxCollectionName),
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

func (r *EventRepository) Publish(
	ctx context.Context,
	event domain.Event,
	publication application.EventPublication,
) error {
	message := &eventcontentv1.EventPublished{
		MessageId:   publication.MessageID,
		EventId:     event.ID,
		SeatIds:     publication.SeatIDs,
		PublishedAt: timestamppb.New(publication.OccurredAt),
	}

	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal event published message: %w", err)
	}

	outbox := outboxDocument{
		ID:          publication.MessageID,
		EventType:   eventcontentcontract.EventPublishedType,
		Key:         event.ID,
		Payload:     payload,
		OccurredAt:  publication.OccurredAt,
		PublishedAt: nil,
	}

	session, err := r.client.StartSession()
	if err != nil {
		return fmt.Errorf("start publish event session: %w", err)
	}
	defer session.EndSession(context.Background())

	transactionOptions := options.Transaction().
		SetWriteConcern(writeconcern.Majority())

	_, err = session.WithTransaction(
		ctx,
		func(txCtx context.Context) (any, error) {
			result, err := r.collection.ReplaceOne(
				txCtx,
				bson.M{
					"_id":    event.ID,
					"status": domain.EventStatusDraft,
				},
				toEventDocument(event),
			)
			if err != nil {
				return nil, fmt.Errorf("replace published event: %w", err)
			}

			if result.MatchedCount == 0 {
				var existing eventDocument

				err := r.collection.FindOne(
					txCtx,
					bson.M{"_id": event.ID},
				).Decode(&existing)

				switch {
				case errors.Is(err, mongo.ErrNoDocuments):
					return nil, domain.ErrEventNotFound

				case err != nil:
					return nil, fmt.Errorf(
						"check event after publish conflict: %w",
						err,
					)

				default:
					return nil, domain.ErrEventNotDraft
				}
			}

			if _, err := r.outboxCollection.InsertOne(txCtx, outbox); err != nil {
				return nil, fmt.Errorf("insert outbox message: %w", err)
			}

			return nil, nil
		},
		transactionOptions,
	)
	if err != nil {
		return fmt.Errorf("publish event transaction: %w", err)
	}

	return nil
}

func (r *EventRepository) List(ctx context.Context) ([]domain.Event, error) {
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
