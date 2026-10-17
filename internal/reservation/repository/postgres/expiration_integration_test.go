package postgres_test

import (
	"context"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	"github.com/google/uuid"
)

func TestExpireReservations_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seatID := uuid.NewString()

	cleanupEvent(t, pool, eventID)

	initializeInventory(
		t,
		pool,
		service,
		eventID,
		[]string{seatID},
	)

	reservation, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: eventID,
			UserID:  "user-1",
			SeatIDs: []string{seatID},
		},
	)
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	store := reservationpostgres.NewStore(pool)

	expired, err := store.ExpireReservation(context.Background(), reservation.ID, reservation.ExpiresAt)
	if err != nil {
		t.Fatalf("expire reservation: %v", err)
	}
	if !expired {
		t.Fatal("expected reservation to expire")
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
			EventID: eventID,
			UserID:  "user-2",
			SeatIDs: []string{seatID},
		},
	)
	if err != nil {
		t.Fatalf("seat should be reusable after expiration: %v", err)
	}
}
