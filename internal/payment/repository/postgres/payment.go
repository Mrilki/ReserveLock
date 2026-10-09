package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	paymentcontract "github.com/Mrilki/ReserveLock/internal/integration/payment"
	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) CompletePayment(
	ctx context.Context,
	reservationID string,
	paymentStatus domain.PaymentStatus,
) (domain.Payment, error) {
	var eventType string
	switch paymentStatus {
	case domain.PaymentStatusSucceeded:
		eventType = paymentcontract.PaymentSucceededType
	case domain.PaymentStatusFailed:
		eventType = paymentcontract.PaymentFailedType
	default:
		return domain.Payment{}, fmt.Errorf("unsupported final payment status %q", paymentStatus)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Payment{}, fmt.Errorf("begin payment completion: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var payment domain.Payment
	var storedStatus string

	err = tx.QueryRow(ctx, `
		SELECT id, reservation_id, user_id, status, created_at, expires_at
		FROM payments
		WHERE reservation_id = $1
		FOR UPDATE
	`, reservationID).Scan(
		&payment.ID,
		&payment.ReservationID,
		&payment.UserID,
		&storedStatus,
		&payment.CreatedAt,
		&payment.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Payment{}, domain.ErrPaymentNotFound
	}
	if err != nil {
		return domain.Payment{}, fmt.Errorf("lock payment for completion: %w", err)
	}

	payment.Status = domain.PaymentStatus(storedStatus)

	if payment.Status == paymentStatus {
		return payment, nil
	}
	if payment.Status != domain.PaymentStatusPending {
		return domain.Payment{}, domain.ErrPaymentNotPending
	}

	now := time.Now().UTC()
	if !now.Before(payment.ExpiresAt) {
		return domain.Payment{}, domain.ErrPaymentExpired
	}

	result, err := tx.Exec(ctx, `
		UPDATE payments
		SET status = $2
		WHERE id = $1
		  AND status = $3
	`, payment.ID, string(paymentStatus), string(domain.PaymentStatusPending))
	if err != nil {
		return domain.Payment{}, fmt.Errorf("update payment status: %w", err)
	}
	if result.RowsAffected() != 1 {
		return domain.Payment{}, domain.ErrPaymentNotPending
	}

	payment.Status = paymentStatus

	if err := insertPaymentOutbox(ctx, tx, payment, eventType, now); err != nil {
		return domain.Payment{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Payment{}, fmt.Errorf("commit payment completion: %w", err)
	}
	return payment, nil
}
