package publichttp

import (
	"context"
	"net/http"
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
)

type EventResponse struct {
	ID        string `json:"id"`
	VenueID   string `json:"venueId"`
	HallID    string `json:"hallId"`
	SeatMapID string `json:"seatMapId"`
	Name      string `json:"name"`
	StartsAt  string `json:"startsAt"`
	Status    string `json:"status"`
}

type ListEventsResponse struct {
	Events []EventResponse `json:"events"`
}

func (h *Handler) GetEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()

	response, err := h.eventContentClient.GetEvent(ctx, &eventcontentv1.GetEventRequest{
		EventId: eventID,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	event := response.GetEvent()
	if event == nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if event.GetStatus() != eventcontentv1.EventStatus_EVENT_STATUS_PUBLISHED {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}

	writeJSON(w, http.StatusOK, toEventResponse(event))
}

func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()

	response, err := h.eventContentClient.ListEvents(
		ctx,
		&eventcontentv1.ListEventsRequest{},
	)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	events := make([]EventResponse, 0, len(response.GetEvents()))

	for _, event := range response.GetEvents() {
		if event == nil {
			continue
		}

		if event.GetStatus() != eventcontentv1.EventStatus_EVENT_STATUS_PUBLISHED {
			continue
		}

		events = append(events, toEventResponse(event))
	}

	writeJSON(w, http.StatusOK, ListEventsResponse{
		Events: events,
	})
}

func toEventResponse(event *eventcontentv1.Event) EventResponse {
	var startsAt string

	if event.GetStartsAt() != nil {
		startsAt = event.GetStartsAt().AsTime().UTC().Format(time.RFC3339)
	}

	return EventResponse{
		ID:        event.GetId(),
		VenueID:   event.GetVenueId(),
		HallID:    event.GetHallId(),
		SeatMapID: event.GetSeatMapId(),
		Name:      event.GetName(),
		StartsAt:  startsAt,
		Status:    eventStatusToString(event.GetStatus()),
	}
}

func eventStatusToString(status eventcontentv1.EventStatus) string {
	switch status {
	case eventcontentv1.EventStatus_EVENT_STATUS_DRAFT:
		return "DRAFT"
	case eventcontentv1.EventStatus_EVENT_STATUS_PUBLISHED:
		return "PUBLISHED"
	default:
		return "UNSPECIFIED"
	}
}
