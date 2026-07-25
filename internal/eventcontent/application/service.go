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

type Service struct {
	venueRepository VenueRepository
}

type CreateVenueInput struct {
	Name     string
	City     string
	Address  string
	CreateBy string
}

func NewService(venueRepository VenueRepository) *Service {
	return &Service{
		venueRepository: venueRepository,
	}
}

func (s Service) CreateVenue(ctx context.Context, input CreateVenueInput) (domain.Venue, error) {
	if input.Name == "" {
		return domain.Venue{}, ErrInvalidVenueName
	}

	venue := domain.Venue{
		ID:       uuid.NewString(),
		Name:     input.Name,
		City:     input.City,
		Address:  input.Address,
		CreateBy: input.CreateBy,
	}

	if err := s.venueRepository.Save(ctx, venue); err != nil {
		return domain.Venue{}, err
	}
	return venue, nil

}

func (s Service) GetVenue(ctx context.Context, venueID string) (domain.Venue, error) {
	if venueID == "" {
		return domain.Venue{}, ErrInvalidVenueID
	}

	return s.venueRepository.GetByID(ctx, venueID)

}
