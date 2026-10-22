package application_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
)

func TestExpireReservations_PartialFailure(t *testing.T) {
	cutoff := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	firstFailure := errors.New("first failure")
	secondFailure := errors.New("second failure")
	ids := []string{"failed-1", "expired", "already-processed", "failed-2"}

	var calls []string

	store := &storeStub{
		t: t,
		listExpired: func(ctx context.Context, gotCutoff time.Time, limit int) ([]string, error) {
			if !gotCutoff.Equal(cutoff) {
				t.Fatalf("unexpected selection cutoff: %v", gotCutoff)
			}
			if limit != 100 {
				t.Fatalf("unexpected batch limit: %d", limit)
			}
			return ids, nil
		},
		expire: func(ctx context.Context, id string, gotCutoff time.Time) (bool, error) {
			if !gotCutoff.Equal(cutoff) {
				t.Fatalf("unexpected expiration cutoff: %v", gotCutoff)
			}

			calls = append(calls, id)

			switch id {
			case "failed-1":
				return false, firstFailure
			case "expired":
				return true, nil
			case "already-processed":
				return false, nil
			case "failed-2":
				return false, secondFailure
			default:
				t.Fatalf("unexpected reservation ID: %s", id)
				return false, nil
			}
		},
	}
	service := application.NewService(store, nil)

	count, err := service.ExpireReservations(context.Background(), cutoff)
	if count != 1 {
		t.Fatalf("expected one expiration, got %d", count)
	}
	if !errors.Is(err, firstFailure) || !errors.Is(err, secondFailure) {
		t.Fatalf("expected both failures, got %v", err)
	}
	if !slices.Equal(calls, ids) {
		t.Fatalf("unexpected processing order: %v", calls)
	}
}

func TestExpireReservations_ListFailure(t *testing.T) {
	failure := errors.New("list unavailable")

	store := &storeStub{
		t: t,
		listExpired: func(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
			return nil, failure
		},
	}
	service := application.NewService(store, nil)

	count, err := service.ExpireReservations(context.Background(), time.Now())
	if count != 0 || !errors.Is(err, failure) {
		t.Fatalf("unexpected result: count=%d err=%v", count, err)
	}
}

func TestExpireReservations_StopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0

	store := &storeStub{
		t: t,
		listExpired: func(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
			return []string{"first", "second"}, nil
		},
		expire: func(ctx context.Context, id string, cutoff time.Time) (bool, error) {
			calls++
			cancel()
			return true, nil
		},
	}
	service := application.NewService(store, nil)

	count, err := service.ExpireReservations(ctx, time.Now())
	if count != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected result: count=%d err=%v", count, err)
	}
	if calls != 1 {
		t.Fatalf("expected one expiration attempt, got %d", calls)
	}
}
