package reservationgrpc

import (
	"context"

	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	"github.com/Mrilki/ReserveLock/internal/reservation/application"
	"go.uber.org/zap"
)

type server struct {
	reservationv1.UnimplementedReservationServiceServer
	service *application.Service
	logger  *zap.Logger
}

func NewServer(service *application.Service, logger *zap.Logger) reservationv1.ReservationServiceServer {
	return &server{
		service: service,
		logger:  logger,
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
		return nil, s.rpcError("CreateReservation", err)
	}

	return &reservationv1.CreateReservationResponse{
		Reservation: toProtoReservation(reservation),
	}, nil
}

func (s *server) GetReservation(
	ctx context.Context,
	request *reservationv1.GetReservationRequest,
) (*reservationv1.GetReservationResponse, error) {
	reservation, err := s.service.GetReservation(ctx, request.GetReservationId())
	if err != nil {
		return nil, s.rpcError("GetReservation", err)
	}

	return &reservationv1.GetReservationResponse{
		Reservation: toProtoReservation(reservation),
	}, nil
}

func (s *server) GetAvailability(
	ctx context.Context,
	request *reservationv1.GetAvailabilityRequest,
) (*reservationv1.GetAvailabilityResponse, error) {
	seats, err := s.service.GetAvailability(ctx, request.GetEventId())
	if err != nil {
		return nil, s.rpcError("GetAvailability", err)
	}

	responseSeats := make([]*reservationv1.SeatAvailability, 0, len(seats))

	for _, seat := range seats {
		responseSeats = append(responseSeats, toProtoSeatAvailability(seat))
	}

	return &reservationv1.GetAvailabilityResponse{
		EventId: request.GetEventId(),
		Seats:   responseSeats,
	}, nil
}
