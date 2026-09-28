package application_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
	"github.com/google/uuid"
)

func newTestService() *application.Service {
	return application.NewService(memory.NewStore())
}

func initializeInventory(
	t *testing.T,
	service *application.Service,
	eventID string,
	seatIDs []string,
) {
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

func TestCreateReservation(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1", "seat-2"},
	)

	reservation, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-1",
			SeatIDs: []string{
				"seat-1",
				"seat-2",
			},
		},
	)
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	if reservation.ID == "" {
		t.Fatal("reservation id is empty")
	}

	if reservation.EventID != "event-1" {
		t.Fatalf(
			"unexpected event id: %s",
			reservation.EventID,
		)
	}

	if reservation.UserID != "user-1" {
		t.Fatalf(
			"unexpected user id: %s",
			reservation.UserID,
		)
	}

	if reservation.Status != domain.ReservationStatusPending {
		t.Fatalf(
			"unexpected reservation status: %s",
			reservation.Status,
		)
	}

	if reservation.CreatedAt.IsZero() {
		t.Fatal("created at is zero")
	}

	if reservation.ExpiresAt.IsZero() {
		t.Fatal("expires at is zero")
	}

	if !reservation.ExpiresAt.After(reservation.CreatedAt) {
		t.Fatal("expires at must be after created at")
	}

	stored, err := service.GetReservation(
		context.Background(),
		reservation.ID,
	)
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}

	if stored.ID != reservation.ID {
		t.Fatalf(
			"unexpected stored reservation id: %s",
			stored.ID,
		)
	}
}

func TestCreateReservation_SeatUnavailable(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1"},
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
		t.Fatalf("first reservation: %v", err)
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
		t.Fatalf(
			"expected ErrSeatUnavailable, got %v",
			err,
		)
	}
}

func TestCreateReservation_ConcurrentSameSeat(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1"},
	)

	const goroutines = 20

	var (
		successCount     atomic.Int32
		unavailableCount atomic.Int32
		wg               sync.WaitGroup
	)

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()

			_, err := service.CreateReservation(
				context.Background(),
				application.CreateReservationInput{
					EventID: "event-1",
					UserID:  "user",
					SeatIDs: []string{"seat-1"},
				},
			)

			switch {
			case err == nil:
				successCount.Add(1)
			case errors.Is(err, domain.ErrSeatUnavailable):
				unavailableCount.Add(1)
			default:
				t.Errorf(
					"unexpected create reservation error: %v",
					err,
				)
			}

		}()
	}

	wg.Wait()

	if got := successCount.Load(); got != 1 {
		t.Fatalf(
			"expected exactly 1 successful reservation, got %d",
			got,
		)
	}

	if got := unavailableCount.Load(); got != goroutines-1 {
		t.Fatalf(
			"expected %d unavailable results, got %d",
			goroutines-1,
			got,
		)
	}
}

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
		t.Fatalf(
			"create reservation: %v",
			err,
		)
	}

	expiredCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt,
	)
	if err != nil {
		t.Fatalf(
			"expire reservations: %v",
			err,
		)
	}

	if expiredCount != 1 {
		t.Fatalf(
			"expected 1 expired reservation, got %d",
			expiredCount,
		)
	}

	stored, err := service.GetReservation(
		context.Background(),
		reservation.ID,
	)
	if err != nil {
		t.Fatalf(
			"get reservation: %v",
			err,
		)
	}

	if stored.Status != domain.ReservationStatusExpired {
		t.Fatalf(
			"expected EXPIRED status, got %s",
			stored.Status,
		)
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
		t.Fatalf(
			"seat should be available after expiration: %v",
			err,
		)
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
		t.Fatalf(
			"create reservation: %v",
			err,
		)
	}

	expiredCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt.Add(-time.Second),
	)
	if err != nil {
		t.Fatalf(
			"expire reservations: %v",
			err,
		)
	}

	if expiredCount != 0 {
		t.Fatalf(
			"expected 0 expired reservations, got %d",
			expiredCount,
		)
	}

	stored, err := service.GetReservation(
		context.Background(),
		reservation.ID,
	)
	if err != nil {
		t.Fatalf(
			"get reservation: %v",
			err,
		)
	}

	if stored.Status != domain.ReservationStatusPending {
		t.Fatalf(
			"expected PENDING status, got %s",
			stored.Status,
		)
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
		t.Fatalf(
			"expected ErrSeatUnavailable, got %v",
			err,
		)
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
		t.Fatalf(
			"create reservation: %v",
			err,
		)
	}

	firstCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt,
	)
	if err != nil {
		t.Fatalf(
			"first expiration: %v",
			err,
		)
	}

	secondCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf(
			"second expiration: %v",
			err,
		)
	}

	if firstCount != 1 {
		t.Fatalf(
			"expected first expiration count 1, got %d",
			firstCount,
		)
	}

	if secondCount != 0 {
		t.Fatalf(
			"expected second expiration count 0, got %d",
			secondCount,
		)
	}
}

func TestCreateReservation_DuplicateSeatID(t *testing.T) {
	service := newTestService()

	initializeInventory(
		t,
		service,
		"event-1",
		[]string{"seat-1"},
	)

	_, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-1",
			SeatIDs: []string{
				"seat-1",
				"seat-1",
			},
		},
	)

	if !errors.Is(err, application.ErrDuplicateSeatID) {
		t.Fatalf(
			"expected ErrDuplicateSeatID, got %v",
			err,
		)
	}
}

func TestCreateReservation_InventoryNotFound(t *testing.T) {
	service := newTestService()

	_, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-without-inventory",
			UserID:  "user-1",
			SeatIDs: []string{"seat-1"},
		},
	)

	if !errors.Is(err, domain.ErrInventoryNotFound) {
		t.Fatalf(
			"expected ErrInventoryNotFound, got %v",
			err,
		)
	}
}

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
