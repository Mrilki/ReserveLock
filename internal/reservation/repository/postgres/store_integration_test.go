package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newPostgresTestService(
	t *testing.T,
) (*pgxpool.Pool, *application.Service) {
	t.Helper()

	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("create postgres pool: %v", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("ping postgres: %v", err)
	}

	t.Cleanup(pool.Close)

	store := reservationpostgres.NewStore(pool)

	return pool, application.NewService(store)
}

func cleanupEvent(
	t *testing.T,
	pool *pgxpool.Pool,
	eventID string,
) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.Background()

		if _, err := pool.Exec(
			ctx,
			`DELETE FROM seat_inventory WHERE event_id = $1`,
			eventID,
		); err != nil {
			t.Errorf("cleanup seat inventory: %v", err)
		}

		if _, err := pool.Exec(
			ctx,
			`DELETE FROM reservations WHERE event_id = $1`,
			eventID,
		); err != nil {
			t.Errorf("cleanup reservations: %v", err)
		}

		if _, err := pool.Exec(
			ctx,
			`DELETE FROM event_inventories WHERE event_id = $1`,
			eventID,
		); err != nil {
			t.Errorf("cleanup event inventory: %v", err)
		}
	})
}

func TestCreateReservation_ConcurrentSameSeat_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seatID := uuid.NewString()

	cleanupEvent(t, pool, eventID)

	err := service.InitializeInventory(
		context.Background(),
		application.InitializeInventoryInput{
			EventID: eventID,
			SeatIDs: []string{seatID},
		},
	)
	if err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	const workers = 20

	start := make(chan struct{})
	results := make(chan error, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			<-start

			_, err := service.CreateReservation(
				context.Background(),
				application.CreateReservationInput{
					EventID: eventID,
					UserID:  fmt.Sprintf("user-%d", index),
					SeatIDs: []string{seatID},
				},
			)

			results <- err
		}(i)
	}

	close(start)

	wg.Wait()
	close(results)

	successCount := 0
	unavailableCount := 0

	for err := range results {
		switch {
		case err == nil:
			successCount++

		case errors.Is(err, domain.ErrSeatUnavailable):
			unavailableCount++

		default:
			t.Fatalf("unexpected reservation error: %v", err)
		}
	}

	if successCount != 1 {
		t.Fatalf("expected 1 successful reservation, got %d", successCount)
	}

	if unavailableCount != workers-1 {
		t.Fatalf(
			"expected %d unavailable errors, got %d",
			workers-1,
			unavailableCount,
		)
	}

	var reservationCount int

	err = pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM reservations WHERE event_id = $1`,
		eventID,
	).Scan(&reservationCount)
	if err != nil {
		t.Fatalf("count reservations: %v", err)
	}

	if reservationCount != 1 {
		t.Fatalf(
			"expected 1 persisted reservation, got %d",
			reservationCount,
		)
	}

	var status string
	var reservationID string

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT status, reservation_id
			FROM seat_inventory
			WHERE event_id = $1
			  AND seat_id = $2
		`,
		eventID,
		seatID,
	).Scan(&status, &reservationID)
	if err != nil {
		t.Fatalf("get seat inventory: %v", err)
	}

	if status != string(domain.SeatInventoryStatusHeld) {
		t.Fatalf("expected HELD seat, got %s", status)
	}

	if reservationID == "" {
		t.Fatal("expected reservation id on held seat")
	}
}

func TestExpireReservations_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seatID := uuid.NewString()

	cleanupEvent(t, pool, eventID)

	err := service.InitializeInventory(
		context.Background(),
		application.InitializeInventoryInput{
			EventID: eventID,
			SeatIDs: []string{seatID},
		},
	)
	if err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

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

	expiredCount, err := service.ExpireReservations(
		context.Background(),
		reservation.ExpiresAt,
	)
	if err != nil {
		t.Fatalf("expire reservations: %v", err)
	}

	if expiredCount != 1 {
		t.Fatalf("expected 1 expired reservation, got %d", expiredCount)
	}

	stored, err := service.GetReservation(
		context.Background(),
		reservation.ID,
	)
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

func TestCreateReservation_MultipleSeatsAtomic_Postgres(t *testing.T) {
	pool, service := newPostgresTestService(t)

	eventID := uuid.NewString()
	seat1 := uuid.NewString()
	seat2 := uuid.NewString()
	seat3 := uuid.NewString()

	cleanupEvent(t, pool, eventID)

	err := service.InitializeInventory(
		context.Background(),
		application.InitializeInventoryInput{
			EventID: eventID,
			SeatIDs: []string{seat1, seat2, seat3},
		},
	)
	if err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	firstReservation, err := service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: eventID,
			UserID:  "user-1",
			SeatIDs: []string{seat1, seat2},
		},
	)
	if err != nil {
		t.Fatalf("create multi-seat reservation: %v", err)
	}

	var heldCount int

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT count(*)
			FROM seat_inventory
			WHERE event_id = $1
			  AND reservation_id = $2
			  AND status = 'HELD'
		`,
		eventID,
		firstReservation.ID,
	).Scan(&heldCount)
	if err != nil {
		t.Fatalf("count held seats: %v", err)
	}

	if heldCount != 2 {
		t.Fatalf("expected 2 held seats, got %d", heldCount)
	}

	_, err = service.CreateReservation(
		context.Background(),
		application.CreateReservationInput{
			EventID: eventID,
			UserID:  "user-2",
			SeatIDs: []string{seat2, seat3},
		},
	)

	if !errors.Is(err, domain.ErrSeatUnavailable) {
		t.Fatalf("expected ErrSeatUnavailable, got %v", err)
	}

	var seat3Status string
	var seat3ReservationIsNull bool

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT status, reservation_id IS NULL
			FROM seat_inventory
			WHERE event_id = $1
			  AND seat_id = $2
		`,
		eventID,
		seat3,
	).Scan(
		&seat3Status,
		&seat3ReservationIsNull,
	)
	if err != nil {
		t.Fatalf("get third seat state: %v", err)
	}

	if seat3Status != string(domain.SeatInventoryStatusAvailable) {
		t.Fatalf("expected third seat AVAILABLE, got %s", seat3Status)
	}

	if !seat3ReservationIsNull {
		t.Fatal("failed reservation must not hold third seat")
	}

	var reservationCount int

	err = pool.QueryRow(
		context.Background(),
		`
			SELECT count(*)
			FROM reservations
			WHERE event_id = $1
		`,
		eventID,
	).Scan(&reservationCount)
	if err != nil {
		t.Fatalf("count reservations: %v", err)
	}

	if reservationCount != 1 {
		t.Fatalf(
			"expected exactly 1 persisted reservation, got %d",
			reservationCount,
		)
	}
}
