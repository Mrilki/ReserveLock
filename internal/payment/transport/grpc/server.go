package paymentgrpc

import (
	"context"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	"github.com/Mrilki/ReserveLock/internal/payment/application"
)

type server struct {
	paymentv1.UnimplementedPaymentServiceServer
	service *application.Service
}

func NewServer(service *application.Service) paymentv1.PaymentServiceServer {
	return &server{service: service}
}

func (s *server) GetPayment(ctx context.Context, request *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	payment, err := s.service.GetPayment(ctx, request.GetReservationId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &paymentv1.GetPaymentResponse{
		Payment: toProtoPayment(payment),
	}, nil
}
