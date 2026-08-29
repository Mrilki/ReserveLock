package adminhttp

import (
	"context"
	"net/http"
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CreateEventRequest struct {
	VenueID   string `json:"venueId"`
	HallID    string `json:"hallId"`
	SeatMapID string `json:"seatMapId"`
	Name      string `json:"name"`
	StartsAt  string `json:"startsAt"`
	CreatedBy string `json:"createdBy"`
}

type EventResponse struct {
	ID        string `json:"id"`
	VenueID   string `json:"venueId"`
	HallID    string `json:"hallId"`
	SeatMapID string `json:"seatMapId"`
	Name      string `json:"name"`
	StartsAt  string `json:"startsAt"`
	Status    string `json:"status"`
	CreatedBy string `json:"createdBy"`
}

func (h *Handler) CreateEvent(c *gin.Context) {
	var request CreateEventRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	startsAt, err := time.Parse(time.RFC3339, request.StartsAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid startsAt",
		})
		return
	}

	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		eventContentTimeout,
	)
	defer cancel()

	response, err := h.eventContentClient.CreateEvent(
		ctx,
		&eventcontentv1.CreateEventRequest{
			VenueId:   request.VenueID,
			HallId:    request.HallID,
			SeatMapId: request.SeatMapID,
			Name:      request.Name,
			StartsAt:  timestamppb.New(startsAt),
			CreatedBy: request.CreatedBy,
		},
	)
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	event := response.GetEvent()
	if event == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
		return
	}

	c.JSON(http.StatusCreated, toEventResponse(event))
}

func (h *Handler) PublishEvent(c *gin.Context) {
	eventID := c.Param("id")

	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		eventContentTimeout,
	)
	defer cancel()

	response, err := h.eventContentClient.PublishEvent(
		ctx,
		&eventcontentv1.PublishEventRequest{
			EventId: eventID,
		},
	)
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	event := response.GetEvent()
	if event == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
		return
	}
	c.JSON(http.StatusOK, toEventResponse(event))
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
		CreatedBy: event.GetCreatedBy(),
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
