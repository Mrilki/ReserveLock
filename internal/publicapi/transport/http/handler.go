package publichttp

import (
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
)

const upstreamTimeout = 5 * time.Second

type Handler struct {
	eventContentClient eventcontentv1.EventContentServiceClient
	reservationClient  reservationv1.ReservationServiceClient
	paymentClient      paymentv1.PaymentServiceClient
}

func NewHandler(
	eventContentClient eventcontentv1.EventContentServiceClient,
	reservationClient reservationv1.ReservationServiceClient,
	paymentClient paymentv1.PaymentServiceClient,
) *Handler {
	return &Handler{
		eventContentClient: eventContentClient,
		reservationClient:  reservationClient,
		paymentClient:      paymentClient,
	}
}
