package paymentgrpc

import (
	"context"
	"errors"

	"github.com/Mrilki/ReserveLock/internal/payment/application"
	"github.com/Mrilki/ReserveLock/internal/payment/domain"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, application.ErrInvalidReservationID),
		errors.Is(err, application.ErrInvalidPaymentStatus):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrPaymentNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrPaymentNotPending),
		errors.Is(err, domain.ErrPaymentExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()

	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func (s *server) rpcError(method string, err error) error {
	rpcErr := toGRPCError(err)

	if status.Code(rpcErr) == codes.Internal {
		s.logger.Error("grpc request failed",
			zap.String("method", method),
			zap.Error(err),
		)
	}

	return rpcErr
}
