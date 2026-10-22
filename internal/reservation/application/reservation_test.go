package application_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func TestCreateReservation(t *testing.T) {
	var saved domain.Reservation

	store := &storeStub{
		t: t,
		create: func(ctx context.Context, reservation domain.Reservation) error {
			saved = reservation
			return nil
		},
	}
	service := application.NewService(store, nil)

	input := application.CreateReservationInput{
		EventID: "event-1",
		UserID:  "user-1",
		SeatIDs: []string{"seat-1", "seat-2"},
	}

	got, err := service.CreateReservation(context.Background(), input)
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	if got.ID == "" {
		t.Fatal("reservation ID is empty")
	}
	if got.EventID != input.EventID || got.UserID != input.UserID {
		t.Fatalf("unexpected reservation identity: %+v", got)
	}
	if !slices.Equal(got.SeatIDs, input.SeatIDs) {
		t.Fatalf("unexpected seat IDs: %v", got.SeatIDs)
	}
	if got.Status != domain.ReservationStatusPending {
		t.Fatalf("unexpected status: %s", got.Status)
	}
	if got.CreatedAt.IsZero() || got.CreatedAt.Location() != time.UTC {
		t.Fatalf("unexpected creation time: %v", got.CreatedAt)
	}
	if !got.CreatedAt.Equal(got.CreatedAt.Truncate(time.Microsecond)) {
		t.Fatalf("creation time must have microsecond precision: %v", got.CreatedAt)
	}
	if got.ExpiresAt.Sub(got.CreatedAt) != 10*time.Minute {
		t.Fatalf("unexpected hold duration: %v", got.ExpiresAt.Sub(got.CreatedAt))
	}
	if !reflect.DeepEqual(saved, got) {
		t.Fatalf("saved reservation differs from returned reservation: %+v", saved)
	}

	input.SeatIDs[0] = "changed"

	if got.SeatIDs[0] != "seat-1" {
		t.Fatal("reservation must not share the input seat IDs slice")
	}
}

func TestCreateReservation_InvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		input   application.CreateReservationInput
		wantErr error
	}{
		{
			name:    "missing event",
			input:   application.CreateReservationInput{UserID: "user-1", SeatIDs: []string{"seat-1"}},
			wantErr: application.ErrInvalidEventID,
		},
		{
			name:    "missing user",
			input:   application.CreateReservationInput{EventID: "event-1", SeatIDs: []string{"seat-1"}},
			wantErr: application.ErrInvalidUserID,
		},
		{
			name:    "missing seats",
			input:   application.CreateReservationInput{EventID: "event-1", UserID: "user-1"},
			wantErr: application.ErrSeatIDsRequired,
		},
		{
			name: "empty seat",
			input: application.CreateReservationInput{
				EventID: "event-1",
				UserID:  "user-1",
				SeatIDs: []string{""},
			},
			wantErr: application.ErrInvalidSeatID,
		},
		{
			name: "duplicate seat",
			input: application.CreateReservationInput{
				EventID: "event-1",
				UserID:  "user-1",
				SeatIDs: []string{"seat-1", "seat-1"},
			},
			wantErr: application.ErrDuplicateSeatID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := application.NewService(&storeStub{t: t}, nil)

			_, err := service.CreateReservation(context.Background(), test.input)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("expected %v, got %v", test.wantErr, err)
			}
		})
	}
}

func TestCreateReservation_StoreError(t *testing.T) {
	failure := errors.New("store unavailable")

	store := &storeStub{
		t: t,
		create: func(ctx context.Context, reservation domain.Reservation) error {
			return failure
		},
	}
	service := application.NewService(store, nil)

	reservation, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-1",
			SeatIDs: []string{"seat-1"},
		},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("expected store error, got %v", err)
	}
	if reservation.ID != "" {
		t.Fatal("failed creation must not return a successful reservation")
	}
}
