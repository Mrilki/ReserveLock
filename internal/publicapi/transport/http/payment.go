package publichttp

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
)

type SimulatePaymentRequest struct {
	Status string `json:"status"`
}

type PaymentResponse struct {
	ID            string `json:"id"`
	ReservationID string `json:"reservationId"`
	Status        string `json:"status"`
	CreatedAt     string `json:"createdAt"`
	ExpiresAt     string `json:"expiresAt"`
}

func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()

	response, err := h.paymentClient.GetPayment(ctx, &paymentv1.GetPaymentRequest{
		ReservationId: r.PathValue("id"),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	payment := response.GetPayment()
	if payment == nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toPaymentResponse(payment))
}

func (h *Handler) SimulatePayment(w http.ResponseWriter, r *http.Request) {
	var request SimulatePaymentRequest

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var paymentStatus paymentv1.PaymentStatus

	switch request.Status {
	case "SUCCEEDED":
		paymentStatus = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	case "FAILED":
		paymentStatus = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
	default:
		writeError(w, http.StatusBadRequest, "invalid payment status")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()

	response, err := h.paymentClient.SimulatePayment(ctx, &paymentv1.SimulatePaymentRequest{
		ReservationId: r.PathValue("id"),
		Status:        paymentStatus,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	payment := response.GetPayment()
	if payment == nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toPaymentResponse(payment))
}

func toPaymentResponse(payment *paymentv1.Payment) PaymentResponse {
	var createdAt, expiresAt string

	if payment.GetCreatedAt() != nil {
		createdAt = payment.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	if payment.GetExpiresAt() != nil {
		expiresAt = payment.GetExpiresAt().AsTime().UTC().Format(time.RFC3339)
	}

	return PaymentResponse{
		ID:            payment.GetId(),
		ReservationID: payment.GetReservationId(),
		Status:        paymentStatusToString(payment.GetStatus()),
		CreatedAt:     createdAt,
		ExpiresAt:     expiresAt,
	}
}

func paymentStatusToString(paymentStatus paymentv1.PaymentStatus) string {
	switch paymentStatus {
	case paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING:
		return "PENDING"
	case paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED:
		return "SUCCEEDED"
	case paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED:
		return "FAILED"
	case paymentv1.PaymentStatus_PAYMENT_STATUS_CANCELLED:
		return "CANCELLED"
	default:
		return "UNSPECIFIED"
	}
}
