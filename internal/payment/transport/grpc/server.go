package paymentgrpc

import (
	"context"

	paymentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/payment/v1"
	"github.com/Mrilki/ReserveLock/internal/payment/application"
	"go.uber.org/zap"
)

type server struct {
	paymentv1.UnimplementedPaymentServiceServer
	service *application.Service
	logger  *zap.Logger
}

func NewServer(service *application.Service, logger *zap.Logger) paymentv1.PaymentServiceServer {
	return &server{
		service: service,
		logger:  logger,
	}
}

func (s *server) GetPayment(ctx context.Context, request *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	payment, err := s.service.GetPayment(ctx, request.GetReservationId())
	if err != nil {
		return nil, s.rpcError("GetPayment", err)
	}

	return &paymentv1.GetPaymentResponse{
		Payment: toProtoPayment(payment),
	}, nil
}

func (s *server) SimulatePayment(ctx context.Context, request *paymentv1.SimulatePaymentRequest) (*paymentv1.SimulatePaymentResponse, error) {
	payment, err := s.service.SimulatePayment(ctx, application.SimulatePaymentInput{
		ReservationID: request.GetReservationId(),
		Status:        fromProtoPaymentStatus(request.GetStatus()),
	})
	if err != nil {
		return nil, s.rpcError("SimulatePayment", err)
	}

	return &paymentv1.SimulatePaymentResponse{
		Payment: toProtoPayment(payment),
	}, nil
}
