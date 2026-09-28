package reservationgrpc

import (
	"context"

	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
)

type server struct {
	reservationv1.UnimplementedReservationServiceServer
	service *application.Service
}

func NewServer(service *application.Service) reservationv1.ReservationServiceServer {
	return &server{
		service: service,
	}
}

func (s *server) CreateReservation(
	ctx context.Context,
	request *reservationv1.CreateReservationRequest,
) (*reservationv1.CreateReservationResponse, error) {
	reservation, err := s.service.CreateReservation(
		ctx,
		application.CreateReservationInput{
			EventID: request.GetEventId(),
			UserID:  request.GetUserId(),
			SeatIDs: request.GetSeatIds(),
		},
	)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &reservationv1.CreateReservationResponse{
		Reservation: toProtoReservation(reservation),
	}, nil
}

func (s *server) GetReservation(
	ctx context.Context,
	request *reservationv1.GetReservationRequest,
) (*reservationv1.GetReservationResponse, error) {
	reservation, err := s.service.GetReservation(
		ctx,
		request.GetReservationId(),
	)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &reservationv1.GetReservationResponse{
		Reservation: toProtoReservation(reservation),
	}, nil
}
