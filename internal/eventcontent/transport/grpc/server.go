package eventcontentgrpc

import (
	"context"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
)

type server struct {
	eventcontentv1.UnimplementedEventContentServiceServer
	service *application.Service
}

func NewServer(service *application.Service) eventcontentv1.EventContentServiceServer {
	return &server{
		service: service,
	}
}

func (s *server) CreateVenue(ctx context.Context, request *eventcontentv1.CreateVenueRequest) (*eventcontentv1.CreateVenueResponse, error) {
	venue, err := s.service.CreateVenue(ctx, application.CreateVenueInput{
		Name:      request.Name,
		City:      request.City,
		Address:   request.Address,
		CreatedBy: request.CreatedBy,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.CreateVenueResponse{
		Venue: toProtoVenue(venue),
	}, nil
}

func (s *server) GetVenue(ctx context.Context, request *eventcontentv1.GetVenueRequest) (*eventcontentv1.GetVenueResponse, error) {
	venue, err := s.service.GetVenue(ctx, request.VenueId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.GetVenueResponse{
		Venue: toProtoVenue(venue),
	}, nil
}

func (s *server) CreateHall(ctx context.Context, request *eventcontentv1.CreateHallRequest) (*eventcontentv1.CreateHallResponse, error) {
	hall, err := s.service.CreateHall(ctx, application.CreateHallInput{
		VenueID:   request.VenueId,
		Name:      request.Name,
		CreatedBy: request.CreatedBy,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.CreateHallResponse{
		Hall: toProtoHall(hall),
	}, nil

}

func (s *server) GetHall(ctx context.Context, request *eventcontentv1.GetHallRequest) (*eventcontentv1.GetHallResponse, error) {
	hall, err := s.service.GetHall(ctx, request.HallId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.GetHallResponse{
		Hall: toProtoHall(hall),
	}, nil
}

func (s *server) CreateSeatMap(ctx context.Context, request *eventcontentv1.CreateSeatMapRequest) (*eventcontentv1.CreateSeatMapResponse, error) {
	seats := make([]application.CreateSeatInput, 0, len(request.Seats))

	for _, seat := range request.Seats {
		seats = append(seats, application.CreateSeatInput{
			Section: seat.Section,
			Row:     seat.Row,
			Number:  seat.Number,
		})
	}

	seatMap, err := s.service.CreateSeatMap(ctx, application.CreateSeatMapInput{
		HallID:    request.HallId,
		Name:      request.Name,
		Seats:     seats,
		CreatedBy: request.CreatedBy,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.CreateSeatMapResponse{
		SeatMap: toProtoSeatMap(seatMap),
	}, nil

}

func (s *server) GetSeatMap(ctx context.Context, request *eventcontentv1.GetSeatMapRequest) (*eventcontentv1.GetSeatMapResponse, error) {
	seatMap, err := s.service.GetSeatMap(ctx, request.SeatMapId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.GetSeatMapResponse{
		SeatMap: toProtoSeatMap(seatMap),
	}, nil
}
