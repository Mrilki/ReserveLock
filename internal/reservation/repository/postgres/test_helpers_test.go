package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	reservationpostgres "github.com/Mrilki/ReserveLock/internal/reservation/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newPostgresTestService(t *testing.T) (*pgxpool.Pool, *application.Service) {
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

	store := reservationpostgres.NewStore(pool, time.Now)

	return pool, application.NewService(store, nil)
}

func cleanupEvent(t *testing.T, pool *pgxpool.Pool, eventID string) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.Background()

		if _, err := pool.Exec(ctx, `
			DELETE FROM outbox_messages
			WHERE payload ->> 'eventId' = $1
		`, eventID); err != nil {
			t.Errorf("cleanup outbox messages: %v", err)
		}

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

func initializeInventory(
	t *testing.T,
	pool *pgxpool.Pool,
	service *application.Service,
	eventID string,
	seatIDs []string,
) {
	t.Helper()

	messageID := uuid.NewString()

	cleanupInboxMessages(t, pool, messageID)

	processed, err := service.HandleEventPublished(
		context.Background(),
		application.EventPublishedInput{
			MessageID: messageID,
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

func cleanupInboxMessages(t *testing.T, pool *pgxpool.Pool, messageIDs ...string) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.Background()

		for _, messageID := range messageIDs {
			if _, err := pool.Exec(
				ctx,
				`DELETE FROM inbox_messages WHERE message_id = $1`,
				messageID,
			); err != nil {
				t.Errorf("cleanup inbox message %q: %v", messageID, err)
			}
		}
	})
}

func newPostgresTestStoreAt(pool *pgxpool.Pool, now time.Time) *reservationpostgres.Store {
	return reservationpostgres.NewStore(pool, func() time.Time {
		return now
	})
}
