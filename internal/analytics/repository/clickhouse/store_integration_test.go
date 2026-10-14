package clickhouse_test

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	analyticsclickhouse "github.com/Mrilki/ReserveLock/internal/analytics/repository/clickhouse"
	"github.com/google/uuid"
)

func TestReservationEvents_ClickHouseReplay(t *testing.T) {
	dsn := os.Getenv("TEST_ANALYTICS_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("TEST_ANALYTICS_CLICKHOUSE_DSN is not set")
	}

	options, err := ch.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(options.Auth.Database, "_test") {
		t.Fatal("clickhouse integration tests require a database ending in _test")
	}

	client, err := analyticsclickhouse.NewClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}

	store := analyticsclickhouse.NewStore(client)
	eventID := uuid.NewString()
	otherEventID := uuid.NewString()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		err := client.Exec(cleanupCtx, `
			ALTER TABLE reservation_events
			DELETE WHERE event_id IN (?, ?)
			SETTINGS mutations_sync = 1
		`, eventID, otherEventID)
		if err != nil {
			t.Errorf("cleanup analytics fixtures: %v", err)
		}
	})

	base := time.Date(2026, 10, 10, 23, 59, 50, 0, time.UTC)

	newCreated := func(createdAt time.Time, seats []string) domain.ReservationEvent {
		return domain.ReservationEvent{
			MessageID:     uuid.NewString(),
			EventID:       eventID,
			ReservationID: uuid.NewString(),
			EventType:     domain.ReservationEventTypeCreated,
			SeatIDs:       seats,
			CreatedAt:     createdAt,
			ExpiresAt:     createdAt.Add(10 * time.Minute),
			OccurredAt:    createdAt,
		}
	}

	createdA := newCreated(base, []string{"seat-1", "seat-2"})
	createdB := newCreated(base.Add(time.Second), []string{"seat-3", "seat-4"})
	createdC := newCreated(base.Add(2*time.Second), []string{"seat-5", "seat-6", "seat-7"})
	createdD := newCreated(base.Add(24*time.Hour), []string{"seat-8"})

	confirmedA := createdA
	confirmedA.MessageID = uuid.NewString()
	confirmedA.EventType = domain.ReservationEventTypeConfirmed
	confirmedA.OccurredAt = createdA.CreatedAt.Add(10250 * time.Millisecond)

	confirmedB := createdB
	confirmedB.MessageID = uuid.NewString()
	confirmedB.EventType = domain.ReservationEventTypeConfirmed
	confirmedB.OccurredAt = createdB.CreatedAt.Add(20500 * time.Millisecond)

	confirmedD := createdD
	confirmedD.MessageID = uuid.NewString()
	confirmedD.EventType = domain.ReservationEventTypeConfirmed
	confirmedD.OccurredAt = createdD.CreatedAt.Add(90750 * time.Millisecond)

	expiredC := createdC
	expiredC.MessageID = uuid.NewString()
	expiredC.EventType = domain.ReservationEventTypeExpired
	expiredC.OccurredAt = createdC.ExpiresAt.Add(2250 * time.Millisecond)

	other := createdA
	other.MessageID = uuid.NewString()
	other.EventID = otherEventID
	other.ReservationID = uuid.NewString()

	batches := [][]domain.ReservationEvent{
		{createdA},
		{createdA, confirmedA, createdB, confirmedB, createdC, expiredC, other},
		{createdD, confirmedD, confirmedA},
		{confirmedA},
	}

	for _, batch := range batches {
		if err := store.InsertReservationEvents(ctx, batch); err != nil {
			t.Fatalf("insert analytics batch: %v", err)
		}
	}

	summary, err := store.GetEventSummary(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}

	expected := domain.EventSummary{
		EventID:                 eventID,
		Created:                 4,
		Confirmed:               3,
		Expired:                 1,
		CreatedSeats:            8,
		ConfirmedSeats:          5,
		ExpiredSeats:            3,
		AvgConfirmationSeconds:  40.5,
		AvgExpirationLagSeconds: 2.25,
		Daily: []domain.EventDailySummary{
			{
				Date: "2026-10-10", Created: 3, Confirmed: 2, Expired: 1,
				CreatedSeats: 7, ConfirmedSeats: 4, ExpiredSeats: 3,
			},
			{
				Date: "2026-10-11", Created: 1, Confirmed: 1,
				CreatedSeats: 1, ConfirmedSeats: 1,
			},
		},
	}
	if !reflect.DeepEqual(summary, expected) {
		t.Fatalf("unexpected summary: got %+v, want %+v", summary, expected)
	}

	var messageCount uint64
	err = client.QueryRow(ctx, `
		SELECT count()
		FROM reservation_events FINAL
		WHERE event_id = ? AND message_id = ?
	`, eventID, createdA.MessageID).Scan(&messageCount)
	if err != nil {
		t.Fatal(err)
	}
	if messageCount != 1 {
		t.Fatalf("expected one logical created message, got %d", messageCount)
	}

	emptyEventID := uuid.NewString()
	empty, err := store.GetEventSummary(ctx, emptyEventID)
	if err != nil {
		t.Fatal(err)
	}

	expectedEmpty := domain.EventSummary{
		EventID: emptyEventID,
		Daily:   []domain.EventDailySummary{},
	}
	if !reflect.DeepEqual(empty, expectedEmpty) {
		t.Fatalf("expected empty summary: got %+v, want %+v", empty, expectedEmpty)
	}
}
