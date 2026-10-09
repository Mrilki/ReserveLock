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

func toProtoPaymentStatus(paymentStatus domain.PaymentStatus) paymentv1.PaymentStatus {
	switch paymentStatus {
	case domain.PaymentStatusPending:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING
	case domain.PaymentStatusSucceeded:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	case domain.PaymentStatusFailed:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
	case domain.PaymentStatusCancelled:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_CANCELLED
	default:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_UNSPECIFIED
	}
}

func fromProtoPaymentStatus(paymentStatus paymentv1.PaymentStatus) domain.PaymentStatus {
	switch paymentStatus {
	case paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING:
		return domain.PaymentStatusPending
	case paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED:
		return domain.PaymentStatusSucceeded
	case paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED:
		return domain.PaymentStatusFailed
	case paymentv1.PaymentStatus_PAYMENT_STATUS_CANCELLED:
		return domain.PaymentStatusCancelled
	default:
		return ""
	}
}
