package adminhttp

import (
	"context"
	"net/http"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"github.com/gin-gonic/gin"
)

type CreateSeatMapRequest struct {
	HallID    string              `json:"hallId"`
	Name      string              `json:"name"`
	Seats     []CreateSeatRequest `json:"seats"`
	CreatedBy string              `json:"createdBy"`
}

type CreateSeatRequest struct {
	Section string `json:"section"`
	Row     string `json:"row"`
	Number  string `json:"number"`
}

type CreateSeatMapResponse struct {
	ID        string         `json:"id"`
	HallID    string         `json:"hallId"`
	Name      string         `json:"name"`
	Seats     []SeatResponse `json:"seats"`
	CreatedBy string         `json:"createdBy"`
}

type SeatResponse struct {
	ID      string `json:"id"`
	Section string `json:"section"`
	Row     string `json:"row"`
	Number  string `json:"number"`
}

func (h *Handler) CreateSeatMap(c *gin.Context) {
	var request CreateSeatMapRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	seats := make([]*eventcontentv1.SeatInput, 0, len(request.Seats))

	for _, seat := range request.Seats {
		seats = append(seats, &eventcontentv1.SeatInput{
			Section: seat.Section,
			Row:     seat.Row,
			Number:  seat.Number,
		})
	}

	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		eventContentTimeout,
	)
	defer cancel()

	response, err := h.eventContentClient.CreateSeatMap(
		ctx,
		&eventcontentv1.CreateSeatMapRequest{
			HallId:    request.HallID,
			Name:      request.Name,
			Seats:     seats,
			CreatedBy: request.CreatedBy,
		},
	)
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	seatMap := response.GetSeatMap()
	if seatMap == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
		return
	}

	responseSeats := make(
		[]SeatResponse,
		0,
		len(seatMap.GetSeats()),
	)

	for _, seat := range seatMap.GetSeats() {
		responseSeats = append(responseSeats, SeatResponse{
			ID:      seat.GetId(),
			Section: seat.GetSection(),
			Row:     seat.GetRow(),
			Number:  seat.GetNumber(),
		})
	}

	c.JSON(http.StatusCreated, CreateSeatMapResponse{
		ID:        seatMap.GetId(),
		HallID:    seatMap.GetHallId(),
		Name:      seatMap.GetName(),
		Seats:     responseSeats,
		CreatedBy: seatMap.GetCreatedBy(),
	})

}
