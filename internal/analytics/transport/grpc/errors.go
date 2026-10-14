package analyticsgrpc

import (
	"context"
	"errors"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, application.ErrInvalidEventID):
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()

	default:
		return status.Error(codes.Internal, "internal error")
	}
}
