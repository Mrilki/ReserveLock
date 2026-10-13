package clickhouse_test

import (
	"context"
	"os"
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

	createdAt := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)

	created := domain.ReservationEvent{
		MessageID:     uuid.NewString(),
		EventID:       eventID,
		ReservationID: uuid.NewString(),
		EventType:     domain.ReservationEventTypeCreated,
		SeatIDs:       []string{"seat-1", "seat-2"},
		CreatedAt:     createdAt,
		ExpiresAt:     createdAt.Add(10 * time.Minute),
		OccurredAt:    createdAt,
	}

	confirmed := created
	confirmed.MessageID = uuid.NewString()
	confirmed.EventType = domain.ReservationEventTypeConfirmed
	confirmed.OccurredAt = createdAt.Add(30 * time.Second)

	other := created
	other.MessageID = uuid.NewString()
	other.EventID = otherEventID
	other.ReservationID = uuid.NewString()

	batches := [][]domain.ReservationEvent{
		{created},
		{created, confirmed, other},
		{confirmed},
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
		EventID:        eventID,
		Created:        1,
		Confirmed:      1,
		CreatedSeats:   2,
		ConfirmedSeats: 2,
	}
	if summary != expected {
		t.Fatalf("unexpected summary: got %+v, want %+v", summary, expected)
	}

	var messageCount uint64
	err = client.QueryRow(ctx, `
		SELECT count()
		FROM reservation_events FINAL
		WHERE event_id = ? AND message_id = ?
	`, eventID, created.MessageID).Scan(&messageCount)
	if err != nil {
		t.Fatal(err)
	}
	if messageCount != 1 {
		t.Fatalf("expected one logical created message, got %d", messageCount)
	}

	empty, err := store.GetEventSummary(ctx, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if empty.Created != 0 || empty.Confirmed != 0 || empty.Expired != 0 ||
		empty.CreatedSeats != 0 || empty.ConfirmedSeats != 0 || empty.ExpiredSeats != 0 {
		t.Fatalf("expected empty summary, got %+v", empty)
	}
}
