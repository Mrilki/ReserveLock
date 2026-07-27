package application

import (
	"context"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	"github.com/google/uuid"
)

type VenueRepository interface {
	Save(ctx context.Context, venue domain.Venue) error
	GetByID(ctx context.Context, id string) (domain.Venue, error)
}

type HallRepository interface {
	Save(ctx context.Context, hall domain.Hall) error
	GetByID(ctx context.Context, id string) (domain.Hall, error)
}

type Service struct {
	venueRepository VenueRepository
	hallRepository  HallRepository
}

type CreateVenueInput struct {
	Name      string
	City      string
	Address   string
	CreatedBy string
}

type CreateHallInput struct {
	VenueId   string
	Name      string
	CreatedBy string
}

func NewService(venueRepository VenueRepository, hallRepository HallRepository) *Service {
	return &Service{
		venueRepository: venueRepository,
		hallRepository:  hallRepository,
	}
}

func (s *Service) CreateVenue(ctx context.Context, input CreateVenueInput) (domain.Venue, error) {
	if input.Name == "" {
		return domain.Venue{}, ErrInvalidVenueName
	}

	venue := domain.Venue{
		ID:        uuid.NewString(),
		Name:      input.Name,
		City:      input.City,
		Address:   input.Address,
		CreatedBy: input.CreatedBy,
	}

	if err := s.venueRepository.Save(ctx, venue); err != nil {
		return domain.Venue{}, err
	}
	return venue, nil

}

func (s *Service) GetVenue(ctx context.Context, venueID string) (domain.Venue, error) {
	if venueID == "" {
		return domain.Venue{}, ErrInvalidVenueID
	}

	return s.venueRepository.GetByID(ctx, venueID)

}

func (s *Service) CreateHall(ctx context.Context, input CreateHallInput) (domain.Hall, error) {
	if input.VenueId == "" {
		return domain.Hall{}, ErrInvalidHallID
	}
	if input.Name == "" {
		return domain.Hall{}, ErrInvalidHallName
	}
	if _, err := s.venueRepository.GetByID(ctx, input.VenueId); err != nil {
		return domain.Hall{}, err
	}
	hall := domain.Hall{
		ID:        uuid.NewString(),
		VenueID:   input.VenueId,
		Name:      input.Name,
		CreatedBy: input.CreatedBy,
	}

	if err := s.hallRepository.Save(ctx, hall); err != nil {
		return domain.Hall{}, err
	}
	return hall, nil
}

func (s *Service) GetHall(ctx context.Context, hallID string) (domain.Hall, error) {
	if hallID == "" {
		return domain.Hall{}, ErrInvalidHallID
	}
	return s.hallRepository.GetByID(ctx, hallID)
}
