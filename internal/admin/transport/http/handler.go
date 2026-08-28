package adminhttp

import (
	"net/http"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	eventContentClient eventcontentv1.EventContentServiceClient
}

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

func NewHandler(eventContentClient eventcontentv1.EventContentServiceClient) *Handler {
	return &Handler{
		eventContentClient: eventContentClient,
	}
}

func (h *Handler) CreateVenue(c *gin.Context) {
	var request CreateVenueRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	response, err := h.eventContentClient.CreateVenue(
		c.Request.Context(),
		&eventcontentv1.CreateVenueRequest{
			Name:      request.Name,
			Address:   request.Address,
			City:      request.City,
			CreatedBy: request.CreatedBy,
		})
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	venue := response.GetVenue()

	c.JSON(http.StatusCreated, CreateVenueResponse{
		ID:        venue.Id,
		Name:      venue.Name,
		Address:   venue.Address,
		City:      venue.City,
		CreatedBy: venue.CreatedBy,
	})

}
