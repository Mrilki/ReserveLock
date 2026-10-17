package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/google/uuid"
)

func TestProcessEventPublished_DuplicateMessage_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seatID := uuid.NewString()
	messageID := uuid.NewString()

	cleanupEvent(t, pool, eventID)
	cleanupInboxMessages(t, pool, messageID)

	input := application.EventPublishedInput{
		MessageID: messageID,
		EventID:   eventID,
		SeatIDs:   []string{seatID},
	}

	processed, err := service.HandleEventPublished(context.Background(), input)
	if err != nil {
		t.Fatalf("first event published: %v", err)
	}

	if !processed {
		t.Fatal("expected first message to be processed")
	}

	processed, err = service.HandleEventPublished(context.Background(), input)
	if err != nil {
		t.Fatalf("duplicate event published: %v", err)
	}

	if processed {
		t.Fatal("expected duplicate message to be ignored")
	}

	var inboxCount int

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT count(*)
			FROM inbox_messages
			WHERE message_id = $1
		`,
		messageID,
	).Scan(&inboxCount)
	if err != nil {
		t.Fatalf("count inbox messages: %v", err)
	}

	if inboxCount != 1 {
		t.Fatalf("expected 1 inbox message, got %d", inboxCount)
	}

	var seatCount int

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT count(*)
			FROM seat_inventory
			WHERE event_id = $1
		`,
		eventID,
	).Scan(&seatCount)
	if err != nil {
		t.Fatalf("count seat inventory: %v", err)
	}

	if seatCount != 1 {
		t.Fatalf("expected 1 seat inventory row, got %d", seatCount)
	}
}

func TestProcessEventPublished_RollsBackInboxWhenInventoryAlreadyExists_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seatID := uuid.NewString()

	firstMessageID := uuid.NewString()
	secondMessageID := uuid.NewString()

	cleanupEvent(t, pool, eventID)
	cleanupInboxMessages(
		t,
		pool,
		firstMessageID,
		secondMessageID,
	)

	firstProcessed, err := service.HandleEventPublished(
		context.Background(),
		application.EventPublishedInput{
			MessageID: firstMessageID,
			EventID:   eventID,
			SeatIDs:   []string{seatID},
		},
	)
	if err != nil {
		t.Fatalf("first event published: %v", err)
	}

	if !firstProcessed {
		t.Fatal("expected first message to be processed")
	}

	secondProcessed, err := service.HandleEventPublished(
		context.Background(),
		application.EventPublishedInput{
			MessageID: secondMessageID,
			EventID:   eventID,
			SeatIDs:   []string{seatID},
		},
	)

	if !errors.Is(err, domain.ErrInventoryAlreadyExists) {
		t.Fatalf("expected ErrInventoryAlreadyExists, got %v", err)
	}

	if secondProcessed {
		t.Fatal("expected second message not to be processed")
	}

	var secondInboxCount int

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT count(*)
			FROM inbox_messages
			WHERE message_id = $1
		`,
		secondMessageID,
	).Scan(&secondInboxCount)
	if err != nil {
		t.Fatalf("count second inbox message: %v", err)
	}

	if secondInboxCount != 0 {
		t.Fatalf("expected second inbox message to be rolled back, got %d rows", secondInboxCount)
	}

	var firstInboxCount int

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT count(*)
			FROM inbox_messages
			WHERE message_id = $1
		`,
		firstMessageID,
	).Scan(&firstInboxCount)
	if err != nil {
		t.Fatalf("count first inbox message: %v", err)
	}

	if firstInboxCount != 1 {
		t.Fatalf("expected first inbox message to remain, got %d rows", firstInboxCount)
	}
}
