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
		VenueId:   request.VenueId,
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
