package adminhttp

import (
	"context"
	"net/http"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/gin-gonic/gin"
)

type CreateVenueRequest struct {
	Name      string `json:"name"`
	City      string `json:"city"`
	Address   string `json:"address"`
	CreatedBy string `json:"createdBy"`
}

type CreateVenueResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	City      string `json:"city"`
	Address   string `json:"address"`
	CreatedBy string `json:"createdBy"`
}

func (h *Handler) CreateVenue(c *gin.Context) {
	var request CreateVenueRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), eventContentTimeout)
	defer cancel()

	response, err := h.eventContentClient.CreateVenue(
		ctx,
		&eventcontentv1.CreateVenueRequest{
			Name:      request.Name,
			Address:   request.Address,
			City:      request.City,
			CreatedBy: request.CreatedBy,
		},
	)
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	venue := response.GetVenue()
	if venue == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
		return
	}

	c.JSON(http.StatusCreated, CreateVenueResponse{
		ID:        venue.GetId(),
		Name:      venue.GetName(),
		Address:   venue.GetAddress(),
		City:      venue.GetCity(),
		CreatedBy: venue.GetCreatedBy(),
	})
}
