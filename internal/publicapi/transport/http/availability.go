package publichttp

import (
	"context"
	"net/http"
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
)

type SeatAvailabilityResponse struct {
	SeatID    string `json:"seatId"`
	Status    string `json:"status"`
	HeldUntil string `json:"heldUntil,omitempty"`
}

type AvailabilityResponse struct {
	EventID string                     `json:"eventId"`
	Seats   []SeatAvailabilityResponse `json:"seats"`
}

func (h *Handler) GetAvailability(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("id")

	ctx, cancel := context.WithTimeout(
		r.Context(),
		eventContentTimeout,
	)
	defer cancel()

	eventResponse, err := h.eventContentClient.GetEvent(
		ctx,
		&eventcontentv1.GetEventRequest{
			EventId: eventID,
		},
	)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	event := eventResponse.GetEvent()

	if event == nil ||
		event.GetStatus() != eventcontentv1.EventStatus_EVENT_STATUS_PUBLISHED {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}

	availability, err := h.reservationClient.GetAvailability(
		ctx,
		&reservationv1.GetAvailabilityRequest{
			EventId: eventID,
		},
	)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	seats := make([]SeatAvailabilityResponse, 0, len(availability.GetSeats()))

	for _, seat := range availability.GetSeats() {
		if seat == nil {
			continue
		}
		seats = append(seats, toSeatAvailabilityResponse(seat))
	}

	writeJSON(w, http.StatusOK, AvailabilityResponse{
		EventID: eventID,
		Seats:   seats,
	})

}

func toSeatAvailabilityResponse(seat *reservationv1.SeatAvailability) SeatAvailabilityResponse {
	var heldUntil string

	if seat.GetHeldUntil() != nil {
		heldUntil = seat.GetHeldUntil().
			AsTime().
			UTC().
			Format(time.RFC3339)
	}

	return SeatAvailabilityResponse{
		SeatID:    seat.GetSeatId(),
		Status:    seatAvailabilityStatusToString(seat.GetStatus()),
		HeldUntil: heldUntil,
	}
}

func seatAvailabilityStatusToString(
	status reservationv1.SeatAvailabilityStatus,
) string {
	switch status {
	case reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_AVAILABLE:
		return "AVAILABLE"

	case reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_HELD:
		return "HELD"

	case reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_RESERVED:
		return "RESERVED"

	default:
		return "UNSPECIFIED"
	}
}
