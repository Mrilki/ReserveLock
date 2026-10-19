package eventcontentgrpc

import (
	"context"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/application"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"go.uber.org/zap"
)

type server struct {
	eventcontentv1.UnimplementedEventContentServiceServer
	service *application.Service
	logger  *zap.Logger
}

func NewServer(service *application.Service, logger *zap.Logger) eventcontentv1.EventContentServiceServer {
	return &server{
		service: service,
		logger:  logger,
	}
}

func (s *server) CreateVenue(
	ctx context.Context,
	request *eventcontentv1.CreateVenueRequest,
) (*eventcontentv1.CreateVenueResponse, error) {
	venue, err := s.service.CreateVenue(ctx, application.CreateVenueInput{
		Name:      request.GetName(),
		City:      request.GetCity(),
		Address:   request.GetAddress(),
		CreatedBy: request.GetCreatedBy(),
	})
	if err != nil {
		return nil, s.rpcError("CreateVenue", err)
	}
	return &eventcontentv1.CreateVenueResponse{
		Venue: toProtoVenue(venue),
	}, nil
}

func (s *server) GetVenue(
	ctx context.Context,
	request *eventcontentv1.GetVenueRequest,
) (*eventcontentv1.GetVenueResponse, error) {
	venue, err := s.service.GetVenue(ctx, request.GetVenueId())
	if err != nil {
		return nil, s.rpcError("GetVenue", err)
	}
	return &eventcontentv1.GetVenueResponse{
		Venue: toProtoVenue(venue),
	}, nil
}

func (s *server) CreateHall(
	ctx context.Context,
	request *eventcontentv1.CreateHallRequest,
) (*eventcontentv1.CreateHallResponse, error) {
	hall, err := s.service.CreateHall(ctx, application.CreateHallInput{
		VenueID:   request.GetVenueId(),
		Name:      request.GetName(),
		CreatedBy: request.GetCreatedBy(),
	})
	if err != nil {
		return nil, s.rpcError("CreateHall", err)
	}
	return &eventcontentv1.CreateHallResponse{
		Hall: toProtoHall(hall),
	}, nil
}

func (s *server) GetHall(
	ctx context.Context,
	request *eventcontentv1.GetHallRequest,
) (*eventcontentv1.GetHallResponse, error) {
	hall, err := s.service.GetHall(ctx, request.GetHallId())
	if err != nil {
		return nil, s.rpcError("GetHall", err)
	}
	return &eventcontentv1.GetHallResponse{
		Hall: toProtoHall(hall),
	}, nil
}

func (s *server) CreateSeatMap(
	ctx context.Context,
	request *eventcontentv1.CreateSeatMapRequest,
) (*eventcontentv1.CreateSeatMapResponse, error) {
	seats := make([]application.CreateSeatInput, 0, len(request.GetSeats()))

	for _, seat := range request.GetSeats() {
		seats = append(seats, application.CreateSeatInput{
			Section: seat.GetSection(),
			Row:     seat.GetRow(),
			Number:  seat.GetNumber(),
		})
	}

	seatMap, err := s.service.CreateSeatMap(ctx, application.CreateSeatMapInput{
		HallID:    request.GetHallId(),
		Name:      request.GetName(),
		Seats:     seats,
		CreatedBy: request.GetCreatedBy(),
	})
	if err != nil {
		return nil, s.rpcError("CreateSeatMap", err)
	}
	return &eventcontentv1.CreateSeatMapResponse{
		SeatMap: toProtoSeatMap(seatMap),
	}, nil
}

func (s *server) GetSeatMap(
	ctx context.Context,
	request *eventcontentv1.GetSeatMapRequest,
) (*eventcontentv1.GetSeatMapResponse, error) {
	seatMap, err := s.service.GetSeatMap(ctx, request.GetSeatMapId())
	if err != nil {
		return nil, s.rpcError("GetSeatMap", err)
	}
	return &eventcontentv1.GetSeatMapResponse{
		SeatMap: toProtoSeatMap(seatMap),
	}, nil
}

func (s *server) CreateEvent(
	ctx context.Context,
	request *eventcontentv1.CreateEventRequest,
) (*eventcontentv1.CreateEventResponse, error) {
	if err := request.GetStartsAt().CheckValid(); err != nil {
		return nil, s.rpcError("CreateEvent", application.ErrInvalidEventStartsAt)
	}

	event, err := s.service.CreateEvent(ctx, application.CreateEventInput{
		VenueID:   request.GetVenueId(),
		HallID:    request.GetHallId(),
		SeatMapID: request.GetSeatMapId(),
		Name:      request.GetName(),
		StartsAt:  request.GetStartsAt().AsTime(),
		CreatedBy: request.GetCreatedBy(),
	})
	if err != nil {
		return nil, s.rpcError("CreateEvent", err)
	}

	return &eventcontentv1.CreateEventResponse{
		Event: toProtoEvent(event),
	}, nil
}

func (s *server) GetEvent(
	ctx context.Context,
	request *eventcontentv1.GetEventRequest,
) (*eventcontentv1.GetEventResponse, error) {
	event, err := s.service.GetEvent(ctx, request.GetEventId())
	if err != nil {
		return nil, s.rpcError("GetEvent", err)
	}
	return &eventcontentv1.GetEventResponse{
		Event: toProtoEvent(event),
	}, nil
}

func (s *server) PublishEvent(
	ctx context.Context,
	request *eventcontentv1.PublishEventRequest,
) (*eventcontentv1.PublishEventResponse, error) {
	event, err := s.service.PublishEvent(ctx, request.GetEventId())
	if err != nil {
		return nil, s.rpcError("PublishEvent", err)
	}
	return &eventcontentv1.PublishEventResponse{
		Event: toProtoEvent(event),
	}, nil
}

func (s *server) ListEvents(
	ctx context.Context,
	request *eventcontentv1.ListEventsRequest,
) (*eventcontentv1.ListEventsResponse, error) {
	events, err := s.service.ListEvents(ctx)
	if err != nil {
		return nil, s.rpcError("ListEvents", err)
	}

	protoEvents := make([]*eventcontentv1.Event, 0, len(events))
	for _, event := range events {
		protoEvents = append(protoEvents, toProtoEvent(event))
	}
	return &eventcontentv1.ListEventsResponse{
		Events: protoEvents,
	}, nil
}
