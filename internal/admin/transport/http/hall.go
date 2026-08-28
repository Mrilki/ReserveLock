package adminhttp

import (
	"context"
	"net/http"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/gin-gonic/gin"
)

type CreateHallRequest struct {
	VenueID   string `json:"venueId"`
	Name      string `json:"name"`
	CreatedBy string `json:"createdBy"`
}

type CreateHallResponse struct {
	ID        string `json:"id"`
	VenueID   string `json:"venueId"`
	Name      string `json:"name"`
	CreatedBy string `json:"createdBy"`
}

func (h *Handler) CreateHall(c *gin.Context) {
	var request CreateHallRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		eventContentTimeout,
	)
	defer cancel()

	response, err := h.eventContentClient.CreateHall(
		ctx,
		&eventcontentv1.CreateHallRequest{
			VenueId:   request.VenueID,
			Name:      request.Name,
			CreatedBy: request.CreatedBy,
		},
	)
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	hall := response.GetHall()
	if hall == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
		return
	}

	c.JSON(http.StatusCreated, CreateHallResponse{
		ID:        hall.GetId(),
		VenueID:   hall.GetVenueId(),
		Name:      hall.GetName(),
		CreatedBy: hall.GetCreatedBy(),
	})
}
