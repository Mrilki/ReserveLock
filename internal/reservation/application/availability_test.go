package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func TestGetAvailability(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1", "seat-2"},
	)

	_, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-1",
			SeatIDs: []string{"seat-1"},
		},
	)
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	seats, err := service.GetAvailability(
		context.Background(),
		"event-1",
	)
	if err != nil {
		t.Fatalf("get availability: %v", err)
	}

	if len(seats) != 2 {
		t.Fatalf(
			"expected 2 seats, got %d",
			len(seats),
		)
	}

	if seats[0].SeatID != "seat-1" ||
		seats[0].Status != domain.SeatInventoryStatusHeld {
		t.Fatalf(
			"expected seat-1 HELD, got %+v",
			seats[0],
		)
	}

	if seats[1].SeatID != "seat-2" ||
		seats[1].Status != domain.SeatInventoryStatusAvailable {
		t.Fatalf(
			"expected seat-2 AVAILABLE, got %+v",
			seats[1],
		)
	}
}

func TestGetAvailability_InventoryNotFound(t *testing.T) {
	service := newTestService()

	_, err := service.GetAvailability(
		context.Background(),
		"missing-event",
	)

	if !errors.Is(err, domain.ErrInventoryNotFound) {
		t.Fatalf(
			"expected ErrInventoryNotFound, got %v",
			err,
		)
	}
}
