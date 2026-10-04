package redis

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type availabilitySource struct {
	calls int
	seats []domain.SeatInventory
	err   error
}

func (s *availabilitySource) ListSeatInventory(ctx context.Context, eventID string) ([]domain.SeatInventory, error) {
	s.calls++
	return s.seats, s.err
}

func newRedisTestClient(t *testing.T) *goredis.Client {
	t.Helper()

	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL is not set")
	}

	client, err := NewClient(redisURL)
	if err != nil {
		t.Fatalf("create redis client: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	return client
}

func cleanupCacheKey(t *testing.T, client *goredis.Client, key string) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := client.Del(ctx, key).Err(); err != nil {
			t.Errorf("cleanup cache key: %v", err)
		}
	})
}

func TestAvailabilityReader_CacheHitAndMiss_Redis(t *testing.T) {
	client := newRedisTestClient(t)
	eventID := uuid.NewString()
	key := availabilityKey(eventID)
	cleanupCacheKey(t, client, key)

	source := &availabilitySource{
		seats: []domain.SeatInventory{
			{EventID: eventID, SeatID: "seat-1", Status: domain.SeatInventoryStatusAvailable},
		},
	}
	reader := NewAvailabilityReader(source, client, zap.NewNop())

	for i := 0; i < 2; i++ {
		seats, err := reader.ListSeatInventory(context.Background(), eventID)
		if err != nil {
			t.Fatalf("get availability: %v", err)
		}
		if len(seats) != 1 || seats[0].SeatID != "seat-1" {
			t.Fatalf("unexpected seats: %+v", seats)
		}
	}

	if source.calls != 1 {
		t.Fatalf("expected 1 source read, got %d", source.calls)
	}

	ttl, err := client.PTTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("get cache TTL: %v", err)
	}
	if ttl <= 0 || ttl > availabilityCacheTTL {
		t.Fatalf("unexpected cache TTL: %v", ttl)
	}

	if err := client.Del(context.Background(), key).Err(); err != nil {
		t.Fatalf("delete cached snapshot: %v", err)
	}

	if _, err := reader.ListSeatInventory(context.Background(), eventID); err != nil {
		t.Fatalf("get availability after cache loss: %v", err)
	}
	if source.calls != 2 {
		t.Fatalf("expected source read after cache loss, got %d reads", source.calls)
	}
}

func TestAvailabilityReader_InvalidOrExpiredSnapshot_Redis(t *testing.T) {
	client := newRedisTestClient(t)

	expired, err := json.Marshal(availabilitySnapshot{
		ValidUntil: time.Now().UTC().Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("encode expired snapshot: %v", err)
	}

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "invalid JSON", payload: []byte("invalid-json")},
		{name: "expired snapshot", payload: expired},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eventID := uuid.NewString()
			key := availabilityKey(eventID)
			cleanupCacheKey(t, client, key)

			if err := client.Set(context.Background(), key, test.payload, time.Hour).Err(); err != nil {
				t.Fatalf("seed cache: %v", err)
			}

			source := &availabilitySource{
				seats: []domain.SeatInventory{
					{EventID: eventID, SeatID: "seat-1", Status: domain.SeatInventoryStatusAvailable},
				},
			}
			reader := NewAvailabilityReader(source, client, zap.NewNop())

			seats, err := reader.ListSeatInventory(context.Background(), eventID)
			if err != nil {
				t.Fatalf("get availability: %v", err)
			}
			if len(seats) != 1 || seats[0].Status != domain.SeatInventoryStatusAvailable {
				t.Fatalf("expected fresh source data, got %+v", seats)
			}
			if source.calls != 1 {
				t.Fatalf("expected 1 source read, got %d", source.calls)
			}
		})
	}
}

func TestAvailabilityReader_DoesNotCacheInventoryNotFound_Redis(t *testing.T) {
	client := newRedisTestClient(t)
	eventID := uuid.NewString()
	key := availabilityKey(eventID)
	cleanupCacheKey(t, client, key)

	source := &availabilitySource{err: domain.ErrInventoryNotFound}
	reader := NewAvailabilityReader(source, client, zap.NewNop())

	_, err := reader.ListSeatInventory(context.Background(), eventID)
	if !errors.Is(err, domain.ErrInventoryNotFound) {
		t.Fatalf("expected ErrInventoryNotFound, got %v", err)
	}

	exists, err := client.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("check cache key: %v", err)
	}
	if exists != 0 {
		t.Fatal("inventory not found must not be cached")
	}

	source.err = nil
	source.seats = []domain.SeatInventory{
		{EventID: eventID, SeatID: "seat-1", Status: domain.SeatInventoryStatusAvailable},
	}

	seats, err := reader.ListSeatInventory(context.Background(), eventID)
	if err != nil {
		t.Fatalf("get initialized inventory: %v", err)
	}
	if len(seats) != 1 || source.calls != 2 {
		t.Fatalf("expected fresh source read, seats=%+v calls=%d", seats, source.calls)
	}
}

func TestAvailabilityReader_RedisFailureFallsBackToSource(t *testing.T) {
	client, err := NewClient("redis://localhost:6379/0")
	if err != nil {
		t.Fatalf("create redis client: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close redis client: %v", err)
	}

	source := &availabilitySource{
		seats: []domain.SeatInventory{
			{EventID: "event-1", SeatID: "seat-1", Status: domain.SeatInventoryStatusHeld},
		},
	}
	reader := NewAvailabilityReader(source, client, zap.NewNop())

	seats, err := reader.ListSeatInventory(context.Background(), "event-1")
	if err != nil {
		t.Fatalf("Redis failure must not fail availability: %v", err)
	}
	if len(seats) != 1 || seats[0].Status != domain.SeatInventoryStatusHeld {
		t.Fatalf("expected source data, got %+v", seats)
	}
	if source.calls != 1 {
		t.Fatalf("expected 1 source read, got %d", source.calls)
	}
}
