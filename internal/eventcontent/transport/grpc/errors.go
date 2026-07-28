package eventcontentgrpc

import (
	"errors"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
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
		errors.Is(err, application.ErrDuplicateSeat):

		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrVenueNotFound),
		errors.Is(err, domain.ErrHallNotFound),
		errors.Is(err, domain.ErrSeatMapNotFound):

		return status.Error(codes.NotFound, err.Error())

	default:
		return status.Error(codes.Internal, "internal error")
	}
}
