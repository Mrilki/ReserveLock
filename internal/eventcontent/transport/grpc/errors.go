package eventcontentgrpc

import (
	"context"
	"errors"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, application.ErrInvalidVenueID),
		errors.Is(err, application.ErrInvalidVenueName),
		errors.Is(err, application.ErrInvalidHallID),
		errors.Is(err, application.ErrInvalidHallName),
		errors.Is(err, application.ErrInvalidSeatMapID),
		errors.Is(err, application.ErrInvalidSeatMapName),
		errors.Is(err, application.ErrSeatMapSeatsRequired),
		errors.Is(err, application.ErrInvalidSeatRow),
		errors.Is(err, application.ErrInvalidSeatNumber),
		errors.Is(err, application.ErrDuplicateSeat),
		errors.Is(err, application.ErrInvalidEventID),
		errors.Is(err, application.ErrInvalidEventName),
		errors.Is(err, application.ErrInvalidEventStartsAt),
		errors.Is(err, application.ErrHallVenueMismatch),
		errors.Is(err, application.ErrSeatMapHallMismatch):

		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrVenueNotFound),
		errors.Is(err, domain.ErrHallNotFound),
		errors.Is(err, domain.ErrSeatMapNotFound),
		errors.Is(err, domain.ErrEventNotFound):

		return status.Error(codes.NotFound, err.Error())

	case errors.Is(err, domain.ErrEventNotDraft):
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
