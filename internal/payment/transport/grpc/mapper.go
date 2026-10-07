package paymentgrpc

import (
	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toProtoPayment(payment domain.Payment) *paymentv1.Payment {
	return &paymentv1.Payment{
		Id:            payment.ID,
		ReservationId: payment.ReservationID,
		UserId:        payment.UserID,
		Status:        toProtoPaymentStatus(payment.Status),
		CreatedAt:     timestamppb.New(payment.CreatedAt),
		ExpiresAt:     timestamppb.New(payment.ExpiresAt),
	}
}

func toProtoPaymentStatus(paymentStatus domain.Status) paymentv1.PaymentStatus {
	switch paymentStatus {
	case domain.StatusPending:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING

	case domain.StatusSucceeded:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED

	case domain.StatusFailed:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED

	case domain.StatusCancelled:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_CANCELLED

	default:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_UNSPECIFIED
	}
}
