package publichttp

import (
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
)

const eventContentTimeout = 5 * time.Second

type Handler struct {
	eventContentClient eventcontentv1.EventContentServiceClient
	reservationClient  reservationv1.ReservationServiceClient
}

func NewHandler(
	eventContentClient eventcontentv1.EventContentServiceClient,
	reservationClient reservationv1.ReservationServiceClient,
) *Handler {
	return &Handler{
		eventContentClient: eventContentClient,
		reservationClient:  reservationClient,
	}
}
