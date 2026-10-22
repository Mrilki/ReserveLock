package postgres_test

import (
	"context"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/google/uuid"
)

func TestGetAvailability_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seat1 := uuid.NewString()
	seat2 := uuid.NewString()

	cleanupEvent(t, pool, eventID)

	initializeInventory(
		t,
		pool,
		service,
		eventID,
		[]string{seat1, seat2},
	)

	_, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: eventID,
			UserID:  "user-1",
			SeatIDs: []string{seat1},
		},
	)
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	seats, err := service.GetAvailability(context.Background(), eventID)
	if err != nil {
		t.Fatalf("get availability: %v", err)
	}

	if len(seats) != 2 {
		t.Fatalf("expected 2 seats, got %d", len(seats))
	}

	for i := 1; i < len(seats); i++ {
		if seats[i-1].SeatID > seats[i].SeatID {
			t.Fatalf("inventory must be sorted by seat ID: %+v", seats)
		}
	}

	inventory := make(map[string]domain.SeatInventory, len(seats))

	for _, seat := range seats {
		inventory[seat.SeatID] = seat
	}

	if inventory[seat1].Status != domain.SeatInventoryStatusHeld {
		t.Fatalf("expected first seat HELD, got %s", inventory[seat1].Status)
	}

	if inventory[seat1].HeldUntil.IsZero() {
		t.Fatal("expected held seat to have held until")
	}

	if inventory[seat2].Status != domain.SeatInventoryStatusAvailable {
		t.Fatalf("expected second seat AVAILABLE, got %s", inventory[seat2].Status)
	}

	if !inventory[seat2].HeldUntil.IsZero() {
		t.Fatal("expected available seat to have no held until")
	}
}
