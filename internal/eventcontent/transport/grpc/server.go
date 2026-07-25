package eventcontentgrpc

import (
	"context"
	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
)

type Server struct {
	eventcontentv1.UnimplementedVenueServiceServer
	service *application.Service
}

func NewServer(service *application.Service) eventcontentv1.VenueServiceServer {
	return &Server{
		service: service,
	}
}

func (s *Server) CreateVenue(ctx context.Context, request *eventcontentv1.CreateVenueRequest) (*eventcontentv1.CreateVenueResponse, error) {
	venue, err := s.service.CreateVenue(ctx, application.CreateVenueInput{
		Name:     request.Name,
		City:     request.City,
		Address:  request.Address,
		CreateBy: request.CreatedBy,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.CreateVenueResponse{
		Venue: toProtoVenue(venue),
	}, nil
}

func (s *Server) GetVenue(ctx context.Context, request *eventcontentv1.GetVenueRequest) (*eventcontentv1.GetVenueResponse, error) {
	venue, err := s.service.GetVenue(ctx, request.VenueId)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &eventcontentv1.GetVenueResponse{
		Venue: toProtoVenue(venue),
	}, nil
}
