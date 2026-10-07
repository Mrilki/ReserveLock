package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) ProcessReservationCreated(ctx context.Context, messageID string, payment domain.Payment) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin payment creation: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	inserted, err := insertInboxMessage(ctx, tx, messageID)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}

	result, err := tx.Exec(ctx, `
		INSERT INTO payments (
			id, reservation_id, user_id, status, created_at, expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (reservation_id) DO NOTHING
	`, payment.ID, payment.ReservationID, payment.UserID, string(payment.Status), payment.CreatedAt, payment.ExpiresAt)
	if err != nil {
		return false, fmt.Errorf("insert payment: %w", err)
	}

	if result.RowsAffected() == 0 {
		return false, domain.ErrPaymentAlreadyExists
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit payment creation: %w", err)
	}

	return true, nil
}

func (s *Store) ProcessReservationExpired(ctx context.Context, messageID, reservationID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin payment cancellation: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	inserted, err := insertInboxMessage(ctx, tx, messageID)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE payments
		SET status = $2
		WHERE reservation_id = $1
		  AND status = $3
	`, reservationID, string(domain.StatusCancelled), string(domain.StatusPending))
	if err != nil {
		return false, fmt.Errorf("cancel pending payment: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit payment cancellation: %w", err)
	}

	return true, nil
}

func (s *Store) GetPaymentByReservationID(ctx context.Context, reservationID string) (domain.Payment, error) {
	var payment domain.Payment
	var paymentStatus string

	err := s.pool.QueryRow(ctx, `
		SELECT id, reservation_id, user_id, status, created_at, expires_at
		FROM payments
		WHERE reservation_id = $1
	`, reservationID).Scan(
		&payment.ID,
		&payment.ReservationID,
		&payment.UserID,
		&paymentStatus,
		&payment.CreatedAt,
		&payment.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Payment{}, domain.ErrPaymentNotFound
	}
	if err != nil {
		return domain.Payment{}, fmt.Errorf("get payment by reservation id: %w", err)
	}

	payment.Status = domain.Status(paymentStatus)
	return payment, nil
}

func insertInboxMessage(ctx context.Context, tx pgx.Tx, messageID string) (bool, error) {
	result, err := tx.Exec(ctx, `
		INSERT INTO inbox_messages (message_id)
		VALUES ($1)
		ON CONFLICT (message_id) DO NOTHING
	`, messageID)
	if err != nil {
		return false, fmt.Errorf("insert payment inbox message: %w", err)
	}

	return result.RowsAffected() == 1, nil
}
