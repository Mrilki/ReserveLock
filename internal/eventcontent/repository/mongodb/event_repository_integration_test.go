package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestEventRepositoryPublishRollsBackWhenOutboxInsertFails(t *testing.T) {
	repository, database := newTestEventRepository(t)

	ctx := context.Background()

	event := newDraftEvent()

	if err := repository.Create(ctx, event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	publication := application.EventPublication{
		MessageID:  uuid.NewString(),
		SeatIDs:    []string{"seat-1", "seat-2"},
		OccurredAt: time.Now().UTC(),
	}

	_, err := database.Collection(outboxCollectionName).InsertOne(
		ctx,
		outboxDocument{
			ID:         publication.MessageID,
			EventType:  "existing-message",
			Key:        event.ID,
			Payload:    []byte("existing"),
			OccurredAt: publication.OccurredAt,
		},
	)
	if err != nil {
		t.Fatalf("insert conflicting outbox message: %v", err)
	}

	publishedEvent := event

	err = repository.Publish(ctx, publishedEvent, publication)
	if err := publishedEvent.Publish(); err != nil {
		t.Fatalf("publish domain event: %v", err)
	}
	if err == nil {
		t.Fatal("expected publish to fail")
	}

	storedEvent, err := repository.GetByID(ctx, event.ID)
	if err != nil {
		t.Fatalf("get event after failed publish: %v", err)
	}

	if storedEvent.Status != domain.EventStatusDraft {
		t.Fatalf(
			"expected event status %q after rollback, got %q",
			domain.EventStatusDraft,
			storedEvent.Status,
		)
	}
}

func TestEventRepositoryPublishAllowsOnlyOneConcurrentPublish(t *testing.T) {
	repository, database := newTestEventRepository(t)

	ctx := context.Background()

	event := newDraftEvent()

	if err := repository.Create(ctx, event); err != nil {
		t.Fatalf("create event: %v", err)
	}

	publishedEvent := event

	if err := publishedEvent.Publish(); err != nil {
		t.Fatalf("publish domain event: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	for i := 0; i < 2; i++ {
		publication := application.EventPublication{
			MessageID:  uuid.NewString(),
			SeatIDs:    []string{"seat-1", "seat-2"},
			OccurredAt: time.Now().UTC(),
		}

		go func(publication application.EventPublication) {
			defer wg.Done()

			<-start

			errs <- repository.Publish(
				ctx,
				publishedEvent,
				publication,
			)
		}(publication)
	}

	close(start)

	wg.Wait()
	close(errs)

	var (
		successCount  int
		notDraftCount int
	)

	for err := range errs {
		switch {
		case err == nil:
			successCount++

		case errors.Is(err, domain.ErrEventNotDraft):
			notDraftCount++

		default:
			t.Fatalf("unexpected publish error: %v", err)
		}
	}

	if successCount != 1 {
		t.Fatalf("expected 1 successful publish, got %d", successCount)
	}

	if notDraftCount != 1 {
		t.Fatalf("expected 1 not-draft error, got %d", notDraftCount)
	}

	storedEvent, err := repository.GetByID(ctx, event.ID)
	if err != nil {
		t.Fatalf("get published event: %v", err)
	}

	if storedEvent.Status != domain.EventStatusPublished {
		t.Fatalf(
			"expected event status %q, got %q",
			domain.EventStatusPublished,
			storedEvent.Status,
		)
	}

	messageCount, err := database.Collection(outboxCollectionName).CountDocuments(
		ctx,
		bson.M{"key": event.ID},
	)
	if err != nil {
		t.Fatalf("count outbox messages: %v", err)
	}

	if messageCount != 1 {
		t.Fatalf(
			"expected 1 outbox message, got %d",
			messageCount,
		)
	}
}

func newTestEventRepository(t *testing.T) (*EventRepository, *mongo.Database) {
	t.Helper()

	uri := os.Getenv("TEST_MONGO_URI")
	if uri == "" {
		t.Skip("TEST_MONGO_URI is not set")
	}

	baseDatabaseName := os.Getenv("TEST_MONGO_DATABASE")
	if baseDatabaseName == "" {
		t.Skip("TEST_MONGO_DATABASE is not set")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect to mongo: %v", err)
	}

	t.Cleanup(func() {
		disconnectCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = client.Disconnect(disconnectCtx)
	})

	pingCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		t.Fatalf("ping mongo: %v", err)
	}

	databaseName := fmt.Sprintf(
		"%s_%s",
		baseDatabaseName,
		strings.ReplaceAll(uuid.NewString(), "-", ""),
	)

	database := client.Database(databaseName)

	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		_ = database.Drop(dropCtx)
	})

	return NewEventRepository(database), database
}

func newDraftEvent() domain.Event {
	return domain.Event{
		ID:        uuid.NewString(),
		VenueID:   uuid.NewString(),
		HallID:    uuid.NewString(),
		SeatMapID: uuid.NewString(),
		Name:      "Integration Test Event",
		StartsAt:  time.Now().UTC().Add(24 * time.Hour),
		Status:    domain.EventStatusDraft,
		CreatedBy: "integration-test",
	}
}
