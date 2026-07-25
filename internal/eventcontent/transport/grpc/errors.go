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
	case errors.Is(err, application.ErrInvalidVenueName):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, application.ErrInvalidVenueID):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrVenueNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
