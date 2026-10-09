package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	"github.com/Mrilki/ReserveLock/internal/payment/application"
	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	paymentoutbox "github.com/Mrilki/ReserveLock/internal/payment/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSimulatePayment_ResultAndDuplicate_Postgres(t *testing.T) {
	tests := []struct {
		name      string
		status    domain.PaymentStatus
		eventType string
	}{
		{"succeeded", domain.PaymentStatusSucceeded, paymentcontract.PaymentSucceededType},
		{"failed", domain.PaymentStatusFailed, paymentcontract.PaymentFailedType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			service, pool := newPaymentTestService(t)
			input := newReservationCreatedInput()
			cleanupPayment(t, pool, input.ReservationID, input.MessageID)

			if _, err := service.HandleReservationCreated(ctx, input); err != nil {
				t.Fatal(err)
			}

			command := application.SimulatePaymentInput{
				ReservationID: input.ReservationID,
				Status:        test.status,
			}
			payment, err := service.SimulatePayment(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			if payment.Status != test.status {
				t.Fatalf("expected %s, got %s", test.status, payment.Status)
			}

			var messageID, eventType string
			var payload []byte
			var occurredAt time.Time

			err = pool.QueryRow(ctx, `
				SELECT message_id, event_type, payload, occurred_at
				FROM outbox_messages
				WHERE key = $1
			`, input.ReservationID).Scan(&messageID, &eventType, &payload, &occurredAt)
			if err != nil {
				t.Fatal(err)
			}
			if messageID == "" || eventType != test.eventType || occurredAt.IsZero() {
				t.Fatalf("invalid outbox message: id=%q, type=%q, occurredAt=%v", messageID, eventType, occurredAt)
			}

			var snapshot paymentoutbox.PaymentSnapshot
			if err := json.Unmarshal(payload, &snapshot); err != nil {
				t.Fatal(err)
			}
			if snapshot.PaymentID != payment.ID || snapshot.ReservationID != input.ReservationID ||
				snapshot.UserID != input.UserID || snapshot.Status != string(test.status) {
				t.Fatalf("unexpected snapshot: %+v", snapshot)
			}

			repeated, err := service.SimulatePayment(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			if repeated.ID != payment.ID || repeated.Status != payment.Status {
				t.Fatalf("payment changed after duplicate: %+v", repeated)
			}

			assertPaymentOutboxCount(t, pool, input.ReservationID, 1)

			var repeatedMessageID string
			if err := pool.QueryRow(ctx, `
				SELECT message_id FROM outbox_messages WHERE key = $1
			`, input.ReservationID).Scan(&repeatedMessageID); err != nil {
				t.Fatal(err)
			}
			if repeatedMessageID != messageID {
				t.Fatal("outbox message id changed after duplicate")
			}
		})
	}
}

func TestSimulatePayment_Expired_Postgres(t *testing.T) {
	ctx := context.Background()
	service, pool := newPaymentTestService(t)
	input := newReservationCreatedInput()
	input.CreatedAt = time.Now().UTC().Add(-time.Hour)
	input.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	cleanupPayment(t, pool, input.ReservationID, input.MessageID)

	if _, err := service.HandleReservationCreated(ctx, input); err != nil {
		t.Fatal(err)
	}

	_, err := service.SimulatePayment(ctx, application.SimulatePaymentInput{
		ReservationID: input.ReservationID,
		Status:        domain.PaymentStatusSucceeded,
	})
	if !errors.Is(err, domain.ErrPaymentExpired) {
		t.Fatalf("expected expired payment error, got %v", err)
	}

	payment, err := service.GetPayment(ctx, input.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if payment.Status != domain.PaymentStatusPending {
		t.Fatalf("expired payment changed: %s", payment.Status)
	}
	assertPaymentOutboxCount(t, pool, input.ReservationID, 0)
}

func TestSimulatePayment_OutboxFailureRollsBackStatus_Postgres(t *testing.T) {
	ctx := context.Background()
	service, pool := newPaymentTestService(t)
	input := newReservationCreatedInput()
	cleanupPayment(t, pool, input.ReservationID, input.MessageID)

	if _, err := service.HandleReservationCreated(ctx, input); err != nil {
		t.Fatal(err)
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_messages (
			message_id, event_type, key, payload, occurred_at
		)
		VALUES ($1, $2, $3, '{}'::jsonb, CURRENT_TIMESTAMP)
	`, uuid.NewString(), paymentcontract.PaymentSucceededType, input.ReservationID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.SimulatePayment(ctx, application.SimulatePaymentInput{
		ReservationID: input.ReservationID,
		Status:        domain.PaymentStatusSucceeded,
	})

	var databaseErr *pgconn.PgError
	if !errors.As(err, &databaseErr) || databaseErr.Code != "23505" {
		t.Fatalf("expected outbox unique conflict, got %v", err)
	}

	payment, err := service.GetPayment(ctx, input.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if payment.Status != domain.PaymentStatusPending {
		t.Fatalf("status was not rolled back: %s", payment.Status)
	}
	assertPaymentOutboxCount(t, pool, input.ReservationID, 1)
}

func TestSimulatePayment_ConcurrentOppositeResults_Postgres(t *testing.T) {
	ctx := context.Background()
	service, pool := newPaymentTestService(t)
	input := newReservationCreatedInput()
	cleanupPayment(t, pool, input.ReservationID, input.MessageID)

	if _, err := service.HandleReservationCreated(ctx, input); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)

	for _, paymentStatus := range []domain.PaymentStatus{
		domain.PaymentStatusSucceeded,
		domain.PaymentStatusFailed,
	} {
		go func(paymentStatus domain.PaymentStatus) {
			<-start
			_, err := service.SimulatePayment(ctx, application.SimulatePaymentInput{
				ReservationID: input.ReservationID,
				Status:        paymentStatus,
			})
			results <- err
		}(paymentStatus)
	}
	close(start)

	var succeeded, rejected int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrPaymentNotPending):
			rejected++
		default:
			t.Fatalf("unexpected result: %v", err)
		}
	}

	if succeeded != 1 || rejected != 1 {
		t.Fatalf("expected one success and one rejection, got %d and %d", succeeded, rejected)
	}
	assertPaymentOutboxCount(t, pool, input.ReservationID, 1)
}

func assertPaymentOutboxCount(t *testing.T, pool *pgxpool.Pool, reservationID string, expected int) {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbox_messages WHERE key = $1
	`, reservationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("expected %d outbox rows, got %d", expected, count)
	}
}
