package postgres_test

import (
	"context"
	"testing"
	"time"

	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPaymentSucceeded_Idempotency_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service, reservation := newPaymentTestReservation(t, time.Now().UTC())

	firstID := uuid.NewString()
	secondID := uuid.NewString()
	cleanupInboxMessages(t, pool, firstID, secondID)

	calls := []struct {
		messageID string
		processed bool
	}{
		{firstID, true},
		{firstID, false},
		{secondID, true},
	}

	for _, call := range calls {
		processed, err := service.HandlePaymentSucceeded(ctx, paymentResultInput(reservation, call.messageID))
		if err != nil || processed != call.processed {
			t.Fatalf("process payment: processed=%v err=%v", processed, err)
		}
	}

	assertPaymentReservationState(t, pool, reservation, domain.ReservationStatusConfirmed, domain.SeatInventoryStatusReserved)
	assertPaymentInboxCount(t, pool, firstID, 1)
	assertPaymentInboxCount(t, pool, secondID, 1)
	assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 1)

	snapshot, _, _ := readReservationOutbox(t, pool, reservation.ID, reservationcontract.ReservationConfirmedType)
	assertReservationSnapshot(t, snapshot, reservation, domain.ReservationStatusConfirmed)

	expired, err := reservationpostgres.NewStore(pool).ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
	if err != nil || expired {
		t.Fatalf("confirmed reservation must not expire: expired=%v err=%v", expired, err)
	}
}

func TestPaymentSucceeded_OutboxFailureRollsBack_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service, reservation := newPaymentTestReservation(t, time.Now().UTC())

	messageID := uuid.NewString()
	cleanupInboxMessages(t, pool, messageID)
	seedOutboxConflict(t, pool, reservation, reservationcontract.ReservationConfirmedType)

	processed, err := service.HandlePaymentSucceeded(ctx, paymentResultInput(reservation, messageID))
	requireOutboxConflict(t, err)
	if processed {
		t.Fatal("failed transaction must not report successful processing")
	}

	assertPaymentInboxCount(t, pool, messageID, 0)
	assertPaymentReservationState(t, pool, reservation, domain.ReservationStatusPending, domain.SeatInventoryStatusHeld)

	_, err = pool.Exec(ctx, `
		DELETE FROM outbox_messages
		WHERE key = $1 AND event_type = $2
	`, reservation.ID, reservationcontract.ReservationConfirmedType)
	if err != nil {
		t.Fatalf("remove injected conflict: %v", err)
	}

	processed, err = service.HandlePaymentSucceeded(ctx, paymentResultInput(reservation, messageID))
	if err != nil || !processed {
		t.Fatalf("retry after rollback: processed=%v err=%v", processed, err)
	}

	assertPaymentInboxCount(t, pool, messageID, 1)
	assertPaymentReservationState(t, pool, reservation, domain.ReservationStatusConfirmed, domain.SeatInventoryStatusReserved)
	assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 1)
}

func TestPaymentSucceeded_ExpiredReservationKeepsNewHold_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service, reservation := newPaymentTestReservation(t, time.Now().UTC())
	store := reservationpostgres.NewStore(pool)

	expired, err := store.ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
	if err != nil || !expired {
		t.Fatalf("expire original reservation: expired=%v err=%v", expired, err)
	}

	next, err := service.CreateReservation(ctx, application.CreateReservationInput{
		EventID: reservation.EventID,
		UserID:  "next-user",
		SeatIDs: reservation.SeatIDs,
	})
	if err != nil {
		t.Fatalf("create next reservation: %v", err)
	}

	messageID := uuid.NewString()
	cleanupInboxMessages(t, pool, messageID)

	processed, err := service.HandlePaymentSucceeded(ctx, paymentResultInput(reservation, messageID))
	if err != nil || !processed {
		t.Fatalf("handle late payment: processed=%v err=%v", processed, err)
	}

	stored, err := service.GetReservation(ctx, reservation.ID)
	if err != nil || stored.Status != domain.ReservationStatusExpired {
		t.Fatalf("original reservation must remain EXPIRED: reservation=%+v err=%v", stored, err)
	}

	assertPaymentReservationState(t, pool, next, domain.ReservationStatusPending, domain.SeatInventoryStatusHeld)
	assertPaymentInboxCount(t, pool, messageID, 1)
	assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 0)
}

func TestPaymentSucceeded_PastDeadline_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service, reservation := newPaymentTestReservation(t, time.Now().UTC().Add(-11*time.Minute))

	messageID := uuid.NewString()
	cleanupInboxMessages(t, pool, messageID)

	processed, err := service.HandlePaymentSucceeded(ctx, paymentResultInput(reservation, messageID))
	if err != nil || !processed {
		t.Fatalf("handle payment past deadline: processed=%v err=%v", processed, err)
	}

	assertPaymentReservationState(t, pool, reservation, domain.ReservationStatusPending, domain.SeatInventoryStatusHeld)
	assertPaymentInboxCount(t, pool, messageID, 1)
	assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 0)

	expired, err := reservationpostgres.NewStore(pool).ExpireReservation(ctx, reservation.ID, time.Now().UTC())
	if err != nil || !expired {
		t.Fatalf("expire reservation past deadline: expired=%v err=%v", expired, err)
	}
}

func TestPaymentFailed_PreservesHold_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service, reservation := newPaymentTestReservation(t, time.Now().UTC())

	messageID := uuid.NewString()
	cleanupInboxMessages(t, pool, messageID)
	input := paymentResultInput(reservation, messageID)

	processed, err := service.HandlePaymentFailed(ctx, input)
	if err != nil || !processed {
		t.Fatalf("first failed payment: processed=%v err=%v", processed, err)
	}

	processed, err = service.HandlePaymentFailed(ctx, input)
	if err != nil || processed {
		t.Fatalf("duplicate failed payment: processed=%v err=%v", processed, err)
	}

	assertPaymentReservationState(t, pool, reservation, domain.ReservationStatusPending, domain.SeatInventoryStatusHeld)
	assertPaymentInboxCount(t, pool, messageID, 1)
	assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 0)

	expired, err := reservationpostgres.NewStore(pool).ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
	if err != nil || !expired {
		t.Fatalf("failed payment must not prevent expiration: expired=%v err=%v", expired, err)
	}
}

func TestPaymentSucceeded_ConcurrentExpiration_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service, reservation := newPaymentTestReservation(t, time.Now().UTC())

	messageID := uuid.NewString()
	cleanupInboxMessages(t, pool, messageID)

	start := make(chan struct{})
	results := make(chan error, 2)

	go func() {
		<-start
		_, err := service.HandlePaymentSucceeded(ctx, paymentResultInput(reservation, messageID))
		results <- err
	}()

	go func() {
		<-start
		_, err := reservationpostgres.NewStore(pool).ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
		results <- err
	}()

	close(start)

	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent transition: %v", err)
		}
	}

	stored, err := service.GetReservation(ctx, reservation.ID)
	if err != nil {
		t.Fatalf("get final reservation: %v", err)
	}

	switch stored.Status {
	case domain.ReservationStatusConfirmed:
		assertPaymentReservationState(t, pool, reservation, domain.ReservationStatusConfirmed, domain.SeatInventoryStatusReserved)
		assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 1)
		assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationExpiredType, 0)

	case domain.ReservationStatusExpired:
		assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationConfirmedType, 0)
		assertReservationOutboxCount(t, pool, reservation.EventID, reservationcontract.ReservationExpiredType, 1)

		var released int
		err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM seat_inventory
			WHERE event_id = $1
			  AND seat_id = ANY($2::text[])
			  AND status = 'AVAILABLE'
			  AND reservation_id IS NULL
			  AND held_until IS NULL
		`, reservation.EventID, reservation.SeatIDs).Scan(&released)
		if err != nil || released != len(reservation.SeatIDs) {
			t.Fatalf("expired seats must be released: count=%d err=%v", released, err)
		}

	default:
		t.Fatalf("unexpected final status: %s", stored.Status)
	}

	assertPaymentInboxCount(t, pool, messageID, 1)
}

func newPaymentTestReservation(t *testing.T, createdAt time.Time) (*pgxpool.Pool, *application.Service, domain.Reservation) {
	t.Helper()

	pool, service := newPostgresTestService(t)
	eventID := uuid.NewString()
	seatIDs := []string{uuid.NewString(), uuid.NewString()}

	cleanupEvent(t, pool, eventID)
	initializeInventory(t, pool, service, eventID, seatIDs)

	reservation := domain.Reservation{
		ID:        uuid.NewString(),
		EventID:   eventID,
		UserID:    uuid.NewString(),
		SeatIDs:   seatIDs,
		Status:    domain.ReservationStatusPending,
		CreatedAt: createdAt,
		ExpiresAt: createdAt.Add(10 * time.Minute),
	}

	if err := reservationpostgres.NewStore(pool).CreateReservation(context.Background(), reservation); err != nil {
		t.Fatalf("create payment test reservation: %v", err)
	}

	return pool, service, reservation
}

func paymentResultInput(reservation domain.Reservation, messageID string) application.PaymentResultInput {
	return application.PaymentResultInput{
		MessageID:     messageID,
		ReservationID: reservation.ID,
		UserID:        reservation.UserID,
	}
}

func assertPaymentInboxCount(t *testing.T, pool *pgxpool.Pool, messageID string, want int) {
	t.Helper()

	var count int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM inbox_messages WHERE message_id = $1
	`, messageID).Scan(&count)
	if err != nil || count != want {
		t.Fatalf("expected %d inbox rows, got %d: %v", want, count, err)
	}
}

func assertPaymentReservationState(t *testing.T, pool *pgxpool.Pool, reservation domain.Reservation, reservationStatus domain.ReservationStatus, seatStatus domain.SeatInventoryStatus) {
	t.Helper()

	var status string
	err := pool.QueryRow(context.Background(), `
		SELECT status FROM reservations WHERE id = $1
	`, reservation.ID).Scan(&status)
	if err != nil || status != string(reservationStatus) {
		t.Fatalf("expected reservation status %s, got %s: %v", reservationStatus, status, err)
	}

	var count int
	err = pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM seat_inventory
		WHERE event_id = $1
		  AND seat_id = ANY($2::text[])
		  AND status = $3
		  AND reservation_id = $4
		  AND (held_until IS NULL) = $5
	`, reservation.EventID, reservation.SeatIDs, string(seatStatus), reservation.ID,
		seatStatus == domain.SeatInventoryStatusReserved,
	).Scan(&count)
	if err != nil || count != len(reservation.SeatIDs) {
		t.Fatalf("expected %d consistent %s seats, got %d: %v", len(reservation.SeatIDs), seatStatus, count, err)
	}
}
