package application_test

import (
	"context"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
)

func TestHandleEventPublished_DuplicateMessage(t *testing.T) {
	service := newTestService()

	input := application.EventPublishedInput{
		MessageID: "message-1",
		EventID:   "event-1",
		SeatIDs:   []string{"seat-1"},
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
}
