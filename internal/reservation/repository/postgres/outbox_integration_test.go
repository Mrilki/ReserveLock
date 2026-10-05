package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	reservationcontract "github.com/Mrilki/ReserveLock/internal/integration/reservation"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	reservationoutbox "github.com/Mrilki/ReserveLock/internal/reservation/outbox"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReservationOutbox_Lifecycle_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service := newPostgresTestService(t)
	store := reservationpostgres.NewStore(pool)
	eventID := uuid.NewString()
	seatIDs := []string{uuid.NewString(), uuid.NewString()}
	cleanupEvent(t, pool, eventID)
	initializeInventory(t, pool, service, eventID, seatIDs)

	reservation, err := service.CreateReservation(ctx, application.CreateReservationInput{
		EventID: eventID,
		UserID:  "user-1",
		SeatIDs: seatIDs,
	})
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	created, createdAt, createdID := readReservationOutbox(t, pool, reservation.ID, reservationcontract.ReservationCreatedType)
	assertReservationSnapshot(t, created, reservation, domain.ReservationStatusPending)
	if !createdAt.Equal(reservation.CreatedAt.Truncate(time.Microsecond)) {
		t.Fatalf("unexpected creation occurrence time: %v", createdAt)
	}

	expired, err := store.ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt.Add(-time.Second))
	if err != nil || expired {
		t.Fatalf("early expiration: expired=%v err=%v", expired, err)
	}
	assertReservationOutboxCount(t, pool, eventID, reservationcontract.ReservationExpiredType, 0)

	expired, err = store.ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
	if err != nil || !expired {
		t.Fatalf("expiration: expired=%v err=%v", expired, err)
	}
	expired, err = store.ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
	if err != nil || expired {
		t.Fatalf("repeated expiration must be a no-op: expired=%v err=%v", expired, err)
	}

	snapshot, expiredAt, expiredID := readReservationOutbox(t, pool, reservation.ID, reservationcontract.ReservationExpiredType)
	assertReservationSnapshot(t, snapshot, reservation, domain.ReservationStatusExpired)
	if !expiredAt.Equal(reservation.ExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("unexpected expiration occurrence time: %v", expiredAt)
	}
	if createdID == expiredID {
		t.Fatal("different lifecycle events must have different message IDs")
	}

	created, _, repeatedCreatedID := readReservationOutbox(t, pool, reservation.ID, reservationcontract.ReservationCreatedType)
	assertReservationSnapshot(t, created, reservation, domain.ReservationStatusPending)
	if repeatedCreatedID != createdID {
		t.Fatal("persisted message ID must remain stable")
	}
	assertReservationOutboxCount(t, pool, eventID, reservationcontract.ReservationCreatedType, 1)
	assertReservationOutboxCount(t, pool, eventID, reservationcontract.ReservationExpiredType, 1)

	stored, err := service.GetReservation(ctx, reservation.ID)
	if err != nil || stored.Status != domain.ReservationStatusExpired {
		t.Fatalf("expected EXPIRED reservation: reservation=%+v err=%v", stored, err)
	}

	var available int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM seat_inventory
		WHERE event_id = $1 AND status = 'AVAILABLE'
		  AND reservation_id IS NULL AND held_until IS NULL
	`, eventID).Scan(&available)
	if err != nil || available != len(seatIDs) {
		t.Fatalf("expected all seats released: count=%d err=%v", available, err)
	}
}

func TestCreateReservation_OutboxFailureRollsBack_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service := newPostgresTestService(t)
	store := reservationpostgres.NewStore(pool)
	eventID := uuid.NewString()
	seatID := uuid.NewString()
	cleanupEvent(t, pool, eventID)
	initializeInventory(t, pool, service, eventID, []string{seatID})

	now := time.Now().UTC()
	reservation := domain.Reservation{
		ID:        uuid.NewString(),
		EventID:   eventID,
		UserID:    "user-1",
		SeatIDs:   []string{seatID},
		Status:    domain.ReservationStatusPending,
		CreatedAt: now,
		ExpiresAt: now.Add(10 * time.Minute),
	}
	seedOutboxConflict(t, pool, reservation, reservationcontract.ReservationCreatedType)

	err := store.CreateReservation(ctx, reservation)
	requireOutboxConflict(t, err)

	var reservationCount, relationCount int
	err = pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM reservations WHERE id = $1),
			(SELECT count(*) FROM reservation_seats WHERE reservation_id = $1)
	`, reservation.ID).Scan(&reservationCount, &relationCount)
	if err != nil {
		t.Fatalf("check rolled back reservation: %v", err)
	}
	if reservationCount != 0 || relationCount != 0 {
		t.Fatalf("reservation must roll back: reservations=%d relations=%d", reservationCount, relationCount)
	}

	var status string
	var reservationIsNull, heldUntilIsNull bool
	err = pool.QueryRow(ctx, `
		SELECT status, reservation_id IS NULL, held_until IS NULL
		FROM seat_inventory WHERE event_id = $1 AND seat_id = $2
	`, eventID, seatID).Scan(&status, &reservationIsNull, &heldUntilIsNull)
	if err != nil {
		t.Fatalf("check rolled back seat: %v", err)
	}
	if status != "AVAILABLE" || !reservationIsNull || !heldUntilIsNull {
		t.Fatal("failed outbox insert must roll back seat hold")
	}
	assertReservationOutboxCount(t, pool, eventID, reservationcontract.ReservationCreatedType, 1)
}

func TestExpireReservation_OutboxFailureRollsBack_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service := newPostgresTestService(t)
	store := reservationpostgres.NewStore(pool)
	eventID := uuid.NewString()
	seatID := uuid.NewString()
	cleanupEvent(t, pool, eventID)
	initializeInventory(t, pool, service, eventID, []string{seatID})

	reservation, err := service.CreateReservation(ctx, application.CreateReservationInput{
		EventID: eventID,
		UserID:  "user-1",
		SeatIDs: []string{seatID},
	})
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}
	seedOutboxConflict(t, pool, reservation, reservationcontract.ReservationExpiredType)

	expired, err := store.ExpireReservation(ctx, reservation.ID, reservation.ExpiresAt)
	requireOutboxConflict(t, err)
	if expired {
		t.Fatal("failed transaction must not report successful expiration")
	}

	stored, err := service.GetReservation(ctx, reservation.ID)
	if err != nil || stored.Status != domain.ReservationStatusPending {
		t.Fatalf("reservation must remain PENDING: reservation=%+v err=%v", stored, err)
	}

	var status, owner string
	var heldUntil time.Time
	err = pool.QueryRow(ctx, `
		SELECT status, reservation_id, held_until
		FROM seat_inventory WHERE event_id = $1 AND seat_id = $2
	`, eventID, seatID).Scan(&status, &owner, &heldUntil)
	if err != nil {
		t.Fatalf("check retained seat hold: %v", err)
	}
	if status != "HELD" || owner != reservation.ID || !heldUntil.Equal(reservation.ExpiresAt.Truncate(time.Microsecond)) {
		t.Fatal("failed outbox insert must roll back seat release")
	}
	assertReservationOutboxCount(t, pool, eventID, reservationcontract.ReservationCreatedType, 1)
	assertReservationOutboxCount(t, pool, eventID, reservationcontract.ReservationExpiredType, 1)
}

func readReservationOutbox(t *testing.T, pool *pgxpool.Pool, reservationID string, eventType string) (reservationoutbox.ReservationSnapshot, time.Time, string) {
	t.Helper()
	var messageID string
	var payload []byte
	var occurredAt time.Time
	var pending bool

	err := pool.QueryRow(context.Background(), `
		SELECT message_id, payload, occurred_at, published_at IS NULL
		FROM outbox_messages WHERE key = $1 AND event_type = $2
	`, reservationID, eventType).Scan(&messageID, &payload, &occurredAt, &pending)
	if err != nil {
		t.Fatalf("read reservation outbox: %v", err)
	}
	if messageID == "" || !pending {
		t.Fatal("expected a pending message with a non-empty ID")
	}

	var snapshot reservationoutbox.ReservationSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		t.Fatalf("decode outbox snapshot: %v", err)
	}
	return snapshot, occurredAt, messageID
}

func assertReservationSnapshot(t *testing.T, snapshot reservationoutbox.ReservationSnapshot, reservation domain.Reservation, status domain.ReservationStatus) {
	t.Helper()
	if snapshot.ReservationID != reservation.ID || snapshot.EventID != reservation.EventID || snapshot.UserID != reservation.UserID {
		t.Fatalf("unexpected snapshot identity: %+v", snapshot)
	}
	if snapshot.Status != string(status) || !slices.Equal(snapshot.SeatIDs, reservation.SeatIDs) {
		t.Fatalf("unexpected snapshot state: %+v", snapshot)
	}
	if !snapshot.CreatedAt.Truncate(time.Microsecond).Equal(reservation.CreatedAt.Truncate(time.Microsecond)) ||
		!snapshot.ExpiresAt.Truncate(time.Microsecond).Equal(reservation.ExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("unexpected snapshot timestamps: %+v", snapshot)
	}
}

func assertReservationOutboxCount(t *testing.T, pool *pgxpool.Pool, eventID string, eventType string, want int) {
	t.Helper()
	var count int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbox_messages
		WHERE payload ->> 'eventId' = $1 AND event_type = $2
	`, eventID, eventType).Scan(&count)
	if err != nil {
		t.Fatalf("count reservation outbox messages: %v", err)
	}
	if count != want {
		t.Fatalf("expected %d %s messages, got %d", want, eventType, count)
	}
}

func seedOutboxConflict(t *testing.T, pool *pgxpool.Pool, reservation domain.Reservation, eventType string) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"eventId": reservation.EventID})
	if err != nil {
		t.Fatalf("encode conflict fixture: %v", err)
	}
	_, err = pool.Exec(context.Background(), `
		INSERT INTO outbox_messages (message_id, event_type, key, payload, occurred_at)
		VALUES ($1, $2, $3, $4::jsonb, $5)
	`, uuid.NewString(), eventType, reservation.ID, string(payload), time.Now().UTC())
	if err != nil {
		t.Fatalf("seed outbox conflict: %v", err)
	}
}

func requireOutboxConflict(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "outbox_messages_key_event_type_unique" {
		t.Fatalf("expected injected outbox unique violation, got %v", err)
	}
}

func TestOutboxRepository_PublishState_Postgres(t *testing.T) {
	ctx := context.Background()
	pool, service := newPostgresTestService(t)
	eventID := uuid.NewString()
	seatID := uuid.NewString()

	cleanupEvent(t, pool, eventID)
	initializeInventory(t, pool, service, eventID, []string{seatID})

	reservation, err := service.CreateReservation(ctx, application.CreateReservationInput{
		EventID: eventID,
		UserID:  "user-1",
		SeatIDs: []string{seatID},
	})
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}

	repository := reservationpostgres.NewOutboxRepository(pool)

	messages, err := repository.ListPending(ctx, 100)
	if err != nil {
		t.Fatalf("list pending messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one pending message in isolated test database, got %d", len(messages))
	}

	message := messages[0]
	if message.ID == "" || message.Key != reservation.ID || message.EventType != reservationcontract.ReservationCreatedType {
		t.Fatalf("unexpected pending message: %+v", message)
	}

	var snapshot reservationoutbox.ReservationSnapshot
	if err := json.Unmarshal(message.Payload, &snapshot); err != nil {
		t.Fatalf("decode listed payload: %v", err)
	}
	assertReservationSnapshot(t, snapshot, reservation, domain.ReservationStatusPending)

	publishedAt := time.Now().UTC().Truncate(time.Microsecond)

	marked, err := repository.MarkPublished(ctx, message.ID, publishedAt)
	if err != nil || !marked {
		t.Fatalf("first mark: marked=%v err=%v", marked, err)
	}

	marked, err = repository.MarkPublished(ctx, message.ID, publishedAt.Add(time.Minute))
	if err != nil || marked {
		t.Fatalf("repeated mark must be a no-op: marked=%v err=%v", marked, err)
	}

	messages, err = repository.ListPending(ctx, 100)
	if err != nil || len(messages) != 0 {
		t.Fatalf("published message must leave pending selection: count=%d err=%v", len(messages), err)
	}

	var storedPublishedAt time.Time
	err = pool.QueryRow(ctx, `
		SELECT published_at
		FROM outbox_messages
		WHERE message_id = $1
	`, message.ID).Scan(&storedPublishedAt)
	if err != nil {
		t.Fatalf("read published message: %v", err)
	}
	if !storedPublishedAt.Equal(publishedAt) {
		t.Fatal("repeated mark must not overwrite publication time")
	}
}
