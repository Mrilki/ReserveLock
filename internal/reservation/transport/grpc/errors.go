package reservationgrpc

import (
	"errors"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, application.ErrInvalidEventID),
		errors.Is(err, application.ErrInvalidUserID),
		errors.Is(err, application.ErrSeatIDsRequired),
		errors.Is(err, application.ErrInvalidSeatID),
		errors.Is(err, application.ErrDuplicateSeatID),
		errors.Is(err, application.ErrInvalidReservationID):

		return status.Error(
			codes.InvalidArgument,
			err.Error(),
		)

	case errors.Is(err, domain.ErrReservationNotFound),
		errors.Is(err, domain.ErrSeatNotFound):

		return status.Error(
			codes.NotFound,
			err.Error(),
		)

	case errors.Is(err, domain.ErrInventoryNotFound):
		return status.Error(
			codes.Unavailable,
			"event inventory is not ready",
		)

	case errors.Is(err, domain.ErrSeatUnavailable):
		return status.Error(
			codes.FailedPrecondition,
			err.Error(),
		)

	default:
		return status.Error(
			codes.Internal,
			"internal error",
		)
	}
}
