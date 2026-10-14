package adminhttp

import (
	"context"
	"net/http"

	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
	"github.com/gin-gonic/gin"
)

type EventSummaryResponse struct {
	EventID        string `json:"eventId"`
	Created        uint64 `json:"created"`
	Confirmed      uint64 `json:"confirmed"`
	Expired        uint64 `json:"expired"`
	CreatedSeats   uint64 `json:"createdSeats"`
	ConfirmedSeats uint64 `json:"confirmedSeats"`
	ExpiredSeats   uint64 `json:"expiredSeats"`
}

func (h *Handler) GetEventSummary(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), analyticsTimeout)
	defer cancel()

	response, err := h.analyticsClient.GetEventSummary(ctx, &analyticsv1.GetEventSummaryRequest{
		EventId: c.Param("id"),
	})
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	summary := response.GetSummary()
	if summary == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
		return
	}

	c.JSON(http.StatusOK, toEventSummaryResponse(summary))
}

func toEventSummaryResponse(summary *analyticsv1.EventSummary) EventSummaryResponse {
	return EventSummaryResponse{
		EventID:        summary.GetEventId(),
		Created:        summary.GetCreated(),
		Confirmed:      summary.GetConfirmed(),
		Expired:        summary.GetExpired(),
		CreatedSeats:   summary.GetCreatedSeats(),
		ConfirmedSeats: summary.GetConfirmedSeats(),
		ExpiredSeats:   summary.GetExpiredSeats(),
	}
}
