package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
)

func TestHandleEventPublished_BuildsInventory(t *testing.T) {
	failure := errors.New("store unavailable")

	tests := []struct {
		name      string
		processed bool
		err       error
	}{
		{name: "processed", processed: true},
		{name: "ignored", processed: false},
		{name: "failure", err: failure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &storeStub{
				t: t,
				eventPublished: func(
					ctx context.Context,
					messageID string,
					eventID string,
					seats []domain.SeatInventory,
				) (bool, error) {
					if messageID != "message-1" || eventID != "event-1" {
						t.Fatalf("unexpected identity: %s / %s", messageID, eventID)
					}

					want := []domain.SeatInventory{
						{
							EventID: "event-1",
							SeatID:  "seat-1",
							Status:  domain.SeatInventoryStatusAvailable,
						},
						{
							EventID: "event-1",
							SeatID:  "seat-2",
							Status:  domain.SeatInventoryStatusAvailable,
						},
					}
					if !reflect.DeepEqual(seats, want) {
						t.Fatalf("unexpected inventory: %+v", seats)
					}

					return test.processed, test.err
				},
			}
			service := application.NewService(store, nil)

			processed, err := service.HandleEventPublished(
				context.Background(),
				application.EventPublishedInput{
					MessageID: "message-1",
					EventID:   "event-1",
					SeatIDs:   []string{"seat-1", "seat-2"},
				},
			)
			if processed != test.processed || !errors.Is(err, test.err) {
				t.Fatalf("unexpected result: processed=%v err=%v", processed, err)
			}
		})
	}
}
