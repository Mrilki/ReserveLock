package adminhttp

import (
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
)

const eventContentTimeout = 5 * time.Second

type Handler struct {
	eventContentClient eventcontentv1.EventContentServiceClient
}

func NewHandler(eventContentClient eventcontentv1.EventContentServiceClient) *Handler {
	return &Handler{
		eventContentClient: eventContentClient,
	}
}
