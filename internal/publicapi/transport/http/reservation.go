package publichttp

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
)

type CreateReservationRequest struct {
	UserID  string   `json:"userId"`
	SeatIDs []string `json:"seatIds"`
}

type ReservationResponse struct {
	ID        string   `json:"id"`
	EventID   string   `json:"eventId"`
	SeatIDs   []string `json:"seatIds"`
	Status    string   `json:"status"`
	CreatedAt string   `json:"createdAt"`
	ExpiresAt string   `json:"expiresAt"`
}

func (h *Handler) CreateReservation(w http.ResponseWriter, r *http.Request) {
	var request CreateReservationRequest

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()

	eventID := r.PathValue("id")

	eventResponse, err := h.eventContentClient.GetEvent(ctx, &eventcontentv1.GetEventRequest{
		EventId: eventID,
	})
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

	response, err := h.reservationClient.CreateReservation(ctx, &reservationv1.CreateReservationRequest{
		EventId: eventID,
		UserId:  request.UserID,
		SeatIds: request.SeatIDs,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	reservation := response.GetReservation()
	if reservation == nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusCreated, toReservationResponse(reservation))
}

func (h *Handler) GetReservation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()

	response, err := h.reservationClient.GetReservation(ctx, &reservationv1.GetReservationRequest{
		ReservationId: r.PathValue("id"),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	reservation := response.GetReservation()
	if reservation == nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toReservationResponse(reservation))
}

func toReservationResponse(reservation *reservationv1.Reservation) ReservationResponse {
	var createdAt string

	if reservation.GetCreatedAt() != nil {
		createdAt = reservation.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}

	var expiresAt string

	if reservation.GetExpiresAt() != nil {
		expiresAt = reservation.GetExpiresAt().AsTime().UTC().Format(time.RFC3339)
	}

	return ReservationResponse{
		ID:        reservation.GetId(),
		EventID:   reservation.GetEventId(),
		SeatIDs:   append([]string(nil), reservation.GetSeatIds()...),
		Status:    reservationStatusToString(reservation.GetStatus()),
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}
}

func reservationStatusToString(status reservationv1.ReservationStatus) string {
	switch status {
	case reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING:
		return "PENDING"
	case reservationv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED:
		return "CONFIRMED"
	case reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED:
		return "EXPIRED"
	default:
		return "UNSPECIFIED"
	}
}
