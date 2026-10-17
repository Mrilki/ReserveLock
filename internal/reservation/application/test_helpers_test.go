package application_test

import (
	"context"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
	"github.com/google/uuid"
)

func newTestService() *application.Service {
	return application.NewService(memory.NewStore(), nil)
}

func initializeInventory(t *testing.T, service *application.Service, eventID string, seatIDs []string) {
	t.Helper()

	processed, err := service.HandleEventPublished(
		context.Background(),
		application.EventPublishedInput{
			MessageID: uuid.NewString(),
			EventID:   eventID,
			SeatIDs:   seatIDs,
		},
	)
	if err != nil {
		t.Fatalf("handle event published: %v", err)
	}

	if !processed {
		t.Fatal("expected event published message to be processed")
	}
}
