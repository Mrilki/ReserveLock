package application_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/Mrilki/ReserveLock/internal/reservation/repository/memory"
)

type failingReservationRepository struct{}

func (r *failingReservationRepository) Save(
	ctx context.Context,
	reservation domain.Reservation,
) error {
	return errors.New("save failed")
}

func (r *failingReservationRepository) GetByID(
	ctx context.Context,
	id string,
) (domain.Reservation, error) {
	return domain.Reservation{}, domain.ErrReservationNotFound
}

func newTestService() *application.Service {
	inventoryRepository := memory.NewInventoryRepository()
	reservationRepository := memory.NewReservationRepository()

	return application.NewService(
		inventoryRepository,
		reservationRepository,
	)
}

func initializeInventory(
	t *testing.T,
	service *application.Service,
	eventID string,
	seatIDs []string,
) {
	t.Helper()

	err := service.InitializeInventory(
		context.Background(),
		application.InitializeInventoryInput{
			EventID: eventID,
			SeatIDs: seatIDs,
		},
	)
	if err != nil {
		t.Fatalf("initialize inventory: %v", err)
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

func TestCreateReservation_ReleasesSeatsWhenSaveFails(t *testing.T) {
	inventoryRepository := memory.NewInventoryRepository()

	failingService := application.NewService(
		inventoryRepository,
		&failingReservationRepository{},
	)

	initializeInventory(
		t,
		failingService,
		"event-1",
		[]string{"seat-1"},
	)

	_, err := failingService.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-1",
			SeatIDs: []string{"seat-1"},
		},
	)
	if err == nil {
		t.Fatal("expected save error")
	}

	workingService := application.NewService(
		inventoryRepository,
		memory.NewReservationRepository(),
	)

	_, err = workingService.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: "event-1",
			UserID:  "user-2",
			SeatIDs: []string{"seat-1"},
		},
	)
	if err != nil {
		t.Fatalf(
			"seat should have been released after save failure: %v",
			err,
		)
	}
}
