package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func TestExpireReservations(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1"},
	)

	reservation, err := service.CreateReservation(
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

	expiredCount, err := service.ExpireReservations(context.Background(), reservation.ExpiresAt)
	if err != nil {
		t.Fatalf("expire reservations: %v", err)
	}

	if expiredCount != 1 {
		t.Fatalf("expected 1 expired reservation, got %d", expiredCount)
	}

	stored, err := service.GetReservation(context.Background(), reservation.ID)
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}

	if stored.Status != domain.ReservationStatusExpired {
		t.Fatalf("expected EXPIRED status, got %s", stored.Status)
	}

	_, err = service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-2",
			SeatIDs: []string{"seat-1"},
		},
	)
	if err != nil {
		t.Fatalf("seat should be available after expiration: %v", err)
	}
}

func TestExpireReservations_DoesNotExpireActiveReservation(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1"},
	)

	reservation, err := service.CreateReservation(
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

	expiredCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt.Add(-time.Second),
	)
	if err != nil {
		t.Fatalf("expire reservations: %v", err)
	}

	if expiredCount != 0 {
		t.Fatalf("expected 0 expired reservations, got %d", expiredCount)
	}

	stored, err := service.GetReservation(context.Background(), reservation.ID)
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}

	if stored.Status != domain.ReservationStatusPending {
		t.Fatalf("expected PENDING status, got %s", stored.Status)
	}

	_, err = service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-2",
			SeatIDs: []string{"seat-1"},
		},
	)

	if !errors.Is(err, domain.ErrSeatUnavailable) {
		t.Fatalf("expected ErrSeatUnavailable, got %v", err)
	}
}

func TestExpireReservations_IsIdempotent(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1"},
	)

	reservation, err := service.CreateReservation(
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

	firstCount, err := service.ExpireReservations(context.Background(), reservation.ExpiresAt)
	if err != nil {
		t.Fatalf("first expiration: %v", err)
	}

	secondCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("second expiration: %v", err)
	}

	if firstCount != 1 {
		t.Fatalf("expected first expiration count 1, got %d", firstCount)
	}

	if secondCount != 0 {
		t.Fatalf("expected second expiration count 0, got %d", secondCount)
	}
}
