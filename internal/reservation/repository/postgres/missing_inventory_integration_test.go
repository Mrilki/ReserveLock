package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/google/uuid"
)

func TestMissingInventory_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)
	eventID := uuid.NewString()
	cleanupEvent(t, pool, eventID)

	ctx := context.Background()

	_, err := service.GetAvailability(ctx, eventID)
	if !errors.Is(err, domain.ErrInventoryNotFound) {
		t.Fatalf("expected missing inventory from availability, got %v", err)
	}

	_, err = service.CreateReservation(ctx, application.CreateReservationInput{
		EventID: eventID,
		UserID:  "user-1",
		SeatIDs: []string{"seat-1"},
	})
	if !errors.Is(err, domain.ErrInventoryNotFound) {
		t.Fatalf("expected missing inventory from reservation, got %v", err)
	}
}
