package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

type Store struct {
	mu                sync.RWMutex
	inventory         map[string]map[string]domain.SeatInventory
	reservations      map[string]domain.Reservation
	processedMessages map[string]struct{}
}

func NewStore() *Store {
	return &Store{
		inventory:         make(map[string]map[string]domain.SeatInventory),
		reservations:      make(map[string]domain.Reservation),
		processedMessages: make(map[string]struct{}),
	}
}

func (s *Store) ProcessEventPublished(
	ctx context.Context,
	messageID string,
	eventID string,
	seats []domain.SeatInventory,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.processedMessages[messageID]; exists {
		return false, nil
	}

	if _, exists := s.inventory[eventID]; exists {
		return false, domain.ErrInventoryAlreadyExists
	}

	eventInventory := make(map[string]domain.SeatInventory, len(seats))

	for _, seat := range seats {
		eventInventory[seat.SeatID] = seat
	}

	s.inventory[eventID] = eventInventory
	s.processedMessages[messageID] = struct{}{}

	return true, nil
}

func (s *Store) CreateReservation(ctx context.Context, reservation domain.Reservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	eventInventory, ok := s.inventory[reservation.EventID]
	if !ok {
		return domain.ErrInventoryNotFound
	}
	hasUnavailableSeat := false

	for _, seatID := range reservation.SeatIDs {
		seat, ok := eventInventory[seatID]
		if !ok {
			return domain.ErrSeatNotFound
		}

		if seat.Status != domain.SeatInventoryStatusAvailable {
			hasUnavailableSeat = true
		}
	}

	if hasUnavailableSeat {
		return domain.ErrSeatUnavailable
	}

	for _, seatID := range reservation.SeatIDs {
		seat := eventInventory[seatID]
		seat.Status = domain.SeatInventoryStatusHeld
		seat.ReservationID = reservation.ID
		seat.HeldUntil = reservation.ExpiresAt

		eventInventory[seatID] = seat
	}

	reservation.SeatIDs = append([]string(nil), reservation.SeatIDs...)
	s.reservations[reservation.ID] = reservation
	return nil
}

func (s *Store) GetReservationByID(ctx context.Context, id string) (domain.Reservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	reservation, ok := s.reservations[id]
	if !ok {
		return domain.Reservation{}, domain.ErrReservationNotFound
	}
	reservation.SeatIDs = append([]string(nil), reservation.SeatIDs...)

	return reservation, nil
}

func (s *Store) ListExpiredPendingIDs(ctx context.Context, now time.Time, limit int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		return nil, nil
	}

	expired := make([]domain.Reservation, 0)

	for _, reservation := range s.reservations {
		if reservation.Status != domain.ReservationStatusPending {
			continue
		}

		if reservation.ExpiresAt.After(now) {
			continue
		}

		expired = append(expired, reservation)
	}

	sort.Slice(expired, func(i, j int) bool {
		if expired[i].ExpiresAt.Equal(expired[j].ExpiresAt) {
			return expired[i].ID < expired[j].ID
		}

		return expired[i].ExpiresAt.Before(expired[j].ExpiresAt)
	})

	if len(expired) > limit {
		expired = expired[:limit]
	}

	ids := make([]string, 0, len(expired))

	for _, reservation := range expired {
		ids = append(ids, reservation.ID)
	}

	return ids, nil
}

func (s *Store) ExpireReservation(ctx context.Context, id string, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reservation, ok := s.reservations[id]
	if !ok {
		return false, domain.ErrReservationNotFound
	}

	if reservation.Status != domain.ReservationStatusPending {
		return false, nil
	}

	if reservation.ExpiresAt.After(now) {
		return false, nil
	}

	eventInventory, ok := s.inventory[reservation.EventID]
	if !ok {
		return false, domain.ErrInventoryNotFound
	}

	for _, seatID := range reservation.SeatIDs {
		seat, ok := eventInventory[seatID]
		if !ok {
			return false, domain.ErrSeatNotFound
		}

		if seat.Status != domain.SeatInventoryStatusHeld ||
			seat.ReservationID != reservation.ID {
			return false, fmt.Errorf(
				"reservation %s has inconsistent inventory for seat %s",
				reservation.ID,
				seatID,
			)
		}
	}

	for _, seatID := range reservation.SeatIDs {
		seat := eventInventory[seatID]
		seat.Status = domain.SeatInventoryStatusAvailable
		seat.ReservationID = ""
		seat.HeldUntil = time.Time{}

		eventInventory[seatID] = seat
	}

	reservation.Status = domain.ReservationStatusExpired
	s.reservations[id] = reservation

	return true, nil
}

func (s *Store) ListSeatInventory(ctx context.Context, eventID string) ([]domain.SeatInventory, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	eventInventory, exists := s.inventory[eventID]
	if !exists {
		return nil, domain.ErrInventoryNotFound
	}

	seats := make([]domain.SeatInventory, 0, len(eventInventory))

	for _, seat := range eventInventory {
		seats = append(seats, seat)
	}

	sort.Slice(seats, func(i, j int) bool {
		return seats[i].SeatID < seats[j].SeatID
	})

	return seats, nil
}
