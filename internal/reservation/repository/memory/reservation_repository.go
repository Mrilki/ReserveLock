package memory

import (
	"context"
	"sync"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type ReservationRepository struct {
	mu           sync.RWMutex
	reservations map[string]domain.Reservation
}

func NewReservationRepository() *ReservationRepository {
	return &ReservationRepository{
		reservations: make(map[string]domain.Reservation),
	}
}

func (r *ReservationRepository) Save(
	ctx context.Context,
	reservation domain.Reservation,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.reservations[reservation.ID] = reservation

	return nil
}

func (r *ReservationRepository) GetByID(
	ctx context.Context,
	id string,
) (domain.Reservation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	reservation, ok := r.reservations[id]
	if !ok {
		return domain.Reservation{}, domain.ErrReservationNotFound
	}

	return reservation, nil
}

func (r *ReservationRepository) ListExpiredPending(
	ctx context.Context,
	now time.Time,
) ([]domain.Reservation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	reservations := make(
		[]domain.Reservation,
		0,
	)

	for _, reservation := range r.reservations {
		if reservation.Status != domain.ReservationStatusPending {
			continue
		}

		if reservation.ExpiresAt.After(now) {
			continue
		}

		reservations = append(
			reservations,
			reservation,
		)
	}

	return reservations, nil
}
