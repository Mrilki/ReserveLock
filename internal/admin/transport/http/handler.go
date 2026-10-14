package adminhttp

import (
	"time"

	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
)

const (
	eventContentTimeout = 5 * time.Second
	analyticsTimeout    = 5 * time.Second
)

type Handler struct {
	eventContentClient eventcontentv1.EventContentServiceClient
	analyticsClient    analyticsv1.AnalyticsServiceClient
}

func NewHandler(eventContentClient eventcontentv1.EventContentServiceClient, analyticsClient analyticsv1.AnalyticsServiceClient) *Handler {
	return &Handler{
		eventContentClient: eventContentClient,
		analyticsClient:    analyticsClient,
	}
}
