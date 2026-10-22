package application_test

import (
	"context"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type storeStub struct {
	t *testing.T

	create         func(context.Context, domain.Reservation) error
	eventPublished func(context.Context, string, string, []domain.SeatInventory) (bool, error)
	listExpired    func(context.Context, time.Time, int) ([]string, error)
	expire         func(context.Context, string, time.Time) (bool, error)
}

func (s *storeStub) require(configured bool, method string) {
	s.t.Helper()

	if !configured {
		s.t.Fatalf("unexpected Store.%s call", method)
	}
}

func (s *storeStub) CreateReservation(ctx context.Context, reservation domain.Reservation) error {
	s.require(s.create != nil, "CreateReservation")
	return s.create(ctx, reservation)
}

func (s *storeStub) ProcessEventPublished(
	ctx context.Context,
	messageID string,
	eventID string,
	seats []domain.SeatInventory,
) (bool, error) {
	s.require(s.eventPublished != nil, "ProcessEventPublished")
	return s.eventPublished(ctx, messageID, eventID, seats)
}

func (s *storeStub) ListExpiredPendingIDs(
	ctx context.Context,
	cutoff time.Time,
	limit int,
) ([]string, error) {
	s.require(s.listExpired != nil, "ListExpiredPendingIDs")
	return s.listExpired(ctx, cutoff, limit)
}

func (s *storeStub) ExpireReservation(
	ctx context.Context,
	id string,
	cutoff time.Time,
) (bool, error) {
	s.require(s.expire != nil, "ExpireReservation")
	return s.expire(ctx, id, cutoff)
}

func (s *storeStub) GetReservationByID(
	ctx context.Context,
	id string,
) (domain.Reservation, error) {
	s.require(false, "GetReservationByID")
	return domain.Reservation{}, nil
}

func (s *storeStub) ListSeatInventory(
	ctx context.Context,
	eventID string,
) ([]domain.SeatInventory, error) {
	s.require(false, "ListSeatInventory")
	return nil, nil
}

func (s *storeStub) ProcessPaymentSucceeded(
	ctx context.Context,
	messageID, reservationID, userID string,
) (bool, error) {
	s.require(false, "ProcessPaymentSucceeded")
	return false, nil
}

func (s *storeStub) ProcessPaymentFailed(
	ctx context.Context,
	messageID, reservationID, userID string,
) (bool, error) {
	s.require(false, "ProcessPaymentFailed")
	return false, nil
}
