package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/payment/application"
	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	paymentpostgres "github.com/Mrilki/ReserveLock/internal/payment/repository/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReservationCreated_DuplicateMessage_Postgres(t *testing.T) {
	service, pool := newPaymentTestService(t)
	input := newReservationCreatedInput()
	cleanupPayment(t, pool, input.ReservationID, input.MessageID)

	processed, err := service.HandleReservationCreated(context.Background(), input)
	if err != nil || !processed {
		t.Fatalf("first processing: processed=%v, err=%v", processed, err)
	}

	first, err := service.GetPayment(context.Background(), input.ReservationID)
	if err != nil {
		t.Fatal(err)
	}

	processed, err = service.HandleReservationCreated(context.Background(), input)
	if err != nil || processed {
		t.Fatalf("duplicate processing: processed=%v, err=%v", processed, err)
	}

	second, err := service.GetPayment(context.Background(), input.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.Status != domain.PaymentStatusPending {
		t.Fatalf("payment changed after duplicate: %+v", second)
	}

	assertPaymentAndInbox(t, pool, input.ReservationID, []string{input.MessageID}, 1)
}

func TestReservationCreated_ConflictRollsBackInbox_Postgres(t *testing.T) {
	service, pool := newPaymentTestService(t)
	input := newReservationCreatedInput()
	secondMessageID := uuid.NewString()
	cleanupPayment(t, pool, input.ReservationID, input.MessageID, secondMessageID)

	processed, err := service.HandleReservationCreated(context.Background(), input)
	if err != nil || !processed {
		t.Fatalf("first processing: processed=%v, err=%v", processed, err)
	}

	secondInput := input
	secondInput.MessageID = secondMessageID

	processed, err = service.HandleReservationCreated(context.Background(), secondInput)
	if !errors.Is(err, domain.ErrPaymentAlreadyExists) || processed {
		t.Fatalf("expected payment conflict, processed=%v, err=%v", processed, err)
	}

	var secondInboxCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM inbox_messages
		WHERE message_id = $1
	`, secondMessageID).Scan(&secondInboxCount); err != nil {
		t.Fatal(err)
	}
	if secondInboxCount != 0 {
		t.Fatalf("conflicting inbox row was not rolled back: %d", secondInboxCount)
	}

	assertPaymentAndInbox(t, pool, input.ReservationID, []string{input.MessageID, secondMessageID}, 1)
}

func TestReservationExpired_CancelsPendingPayment_Postgres(t *testing.T) {
	service, pool := newPaymentTestService(t)
	input := newReservationCreatedInput()
	expiredMessageID := uuid.NewString()
	cleanupPayment(t, pool, input.ReservationID, input.MessageID, expiredMessageID)

	if _, err := service.HandleReservationCreated(context.Background(), input); err != nil {
		t.Fatal(err)
	}

	processed, err := service.HandleReservationExpired(context.Background(), expiredMessageID, input.ReservationID)
	if err != nil || !processed {
		t.Fatalf("expiration: processed=%v, err=%v", processed, err)
	}

	processed, err = service.HandleReservationExpired(context.Background(), expiredMessageID, input.ReservationID)
	if err != nil || processed {
		t.Fatalf("duplicate expiration: processed=%v, err=%v", processed, err)
	}

	processed, err = service.HandleReservationCreated(context.Background(), input)
	if err != nil || processed {
		t.Fatalf("replayed creation: processed=%v, err=%v", processed, err)
	}

	payment, err := service.GetPayment(context.Background(), input.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if payment.Status != domain.PaymentStatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", payment.Status)
	}

	assertPaymentAndInbox(t, pool, input.ReservationID, []string{input.MessageID, expiredMessageID}, 2)
}

func TestReservationExpired_KeepsCompletedPayment_Postgres(t *testing.T) {
	for _, paymentStatus := range []domain.PaymentStatus{domain.PaymentStatusSucceeded, domain.PaymentStatusFailed} {
		t.Run(string(paymentStatus), func(t *testing.T) {
			service, pool := newPaymentTestService(t)
			input := newReservationCreatedInput()
			expiredMessageID := uuid.NewString()
			cleanupPayment(t, pool, input.ReservationID, input.MessageID, expiredMessageID)

			if _, err := service.HandleReservationCreated(context.Background(), input); err != nil {
				t.Fatal(err)
			}

			_, err := pool.Exec(context.Background(), `
				UPDATE payments SET status = $2 WHERE reservation_id = $1
			`, input.ReservationID, string(paymentStatus))
			if err != nil {
				t.Fatal(err)
			}

			processed, err := service.HandleReservationExpired(context.Background(), expiredMessageID, input.ReservationID)
			if err != nil || !processed {
				t.Fatalf("expiration: processed=%v, err=%v", processed, err)
			}

			payment, err := service.GetPayment(context.Background(), input.ReservationID)
			if err != nil {
				t.Fatal(err)
			}
			if payment.Status != paymentStatus {
				t.Fatalf("completed payment changed: expected %s, got %s", paymentStatus, payment.Status)
			}
		})
	}
}

func newPaymentTestService(t *testing.T) (*application.Service, *pgxpool.Pool) {
	t.Helper()

	dsn := os.Getenv("TEST_PAYMENT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_PAYMENT_POSTGRES_DSN is not set")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	return application.NewService(paymentpostgres.NewStore(pool)), pool
}

func newReservationCreatedInput() application.ReservationCreatedInput {
	now := time.Now().UTC()

	return application.ReservationCreatedInput{
		MessageID:     uuid.NewString(),
		ReservationID: uuid.NewString(),
		UserID:        uuid.NewString(),
		CreatedAt:     now,
		ExpiresAt:     now.Add(10 * time.Minute),
	}
}

func cleanupPayment(t *testing.T, pool *pgxpool.Pool, reservationID string, messageIDs ...string) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.Background()
		if _, err := pool.Exec(ctx, `DELETE FROM outbox_messages WHERE key = $1`, reservationID); err != nil {
			t.Errorf("cleanup payment outbox: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM payments WHERE reservation_id = $1`, reservationID); err != nil {
			t.Errorf("cleanup payment: %v", err)
		}

		if _, err := pool.Exec(ctx, `
			DELETE FROM inbox_messages WHERE message_id = ANY($1::text[])
		`, messageIDs); err != nil {
			t.Errorf("cleanup payment inbox: %v", err)
		}
	})
}

func assertPaymentAndInbox(t *testing.T, pool *pgxpool.Pool, reservationID string, messageIDs []string, expectedInbox int) {
	t.Helper()

	var paymentCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM payments WHERE reservation_id = $1
	`, reservationID).Scan(&paymentCount); err != nil {
		t.Fatal(err)
	}
	if paymentCount != 1 {
		t.Fatalf("expected 1 payment, got %d", paymentCount)
	}

	var inboxCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM inbox_messages WHERE message_id = ANY($1::text[])
	`, messageIDs).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != expectedInbox {
		t.Fatalf("expected %d inbox rows, got %d", expectedInbox, inboxCount)
	}
}
