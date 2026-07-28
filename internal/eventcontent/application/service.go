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

type SeatMapRepository interface {
	Save(ctx context.Context, seatMap domain.SeatMap) error
	GetByID(ctx context.Context, id string) (domain.SeatMap, error)
}

type Service struct {
	venueRepository   VenueRepository
	hallRepository    HallRepository
	seatMapRepository SeatMapRepository
}

type CreateVenueInput struct {
	Name      string
	City      string
	Address   string
	CreatedBy string
}

type CreateHallInput struct {
	VenueID   string
	Name      string
	CreatedBy string
}

type CreateSeatMapInput struct {
	HallID    string
	Name      string
	Seats     []CreateSeatInput
	CreatedBy string
}

type CreateSeatInput struct {
	Section string
	Row     string
	Number  string
}

func NewService(venueRepository VenueRepository, hallRepository HallRepository, seatMapRepository SeatMapRepository) *Service {
	return &Service{
		venueRepository:   venueRepository,
		hallRepository:    hallRepository,
		seatMapRepository: seatMapRepository,
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
	if input.VenueID == "" {
		return domain.Hall{}, ErrInvalidVenueID
	}
	if input.Name == "" {
		return domain.Hall{}, ErrInvalidHallName
	}
	if _, err := s.venueRepository.GetByID(ctx, input.VenueID); err != nil {
		return domain.Hall{}, err
	}
	hall := domain.Hall{
		ID:        uuid.NewString(),
		VenueID:   input.VenueID,
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

func (s *Service) CreateSeatMap(ctx context.Context, input CreateSeatMapInput) (domain.SeatMap, error) {
	if input.Name == "" {
		return domain.SeatMap{}, ErrInvalidSeatMapName
	}
	if input.HallID == "" {
		return domain.SeatMap{}, ErrInvalidHallID
	}
	if len(input.Seats) == 0 {
		return domain.SeatMap{}, ErrSeatMapSeatsRequired
	}
	if _, err := s.hallRepository.GetByID(ctx, input.HallID); err != nil {
		return domain.SeatMap{}, err
	}

	seats, err := buildSeats(input.Seats)
	if err != nil {
		return domain.SeatMap{}, err
	}

	seatMap := domain.SeatMap{
		ID:        uuid.NewString(),
		HallID:    input.HallID,
		Name:      input.Name,
		Seats:     seats,
		CreatedBy: input.CreatedBy,
	}

	if err := s.seatMapRepository.Save(ctx, seatMap); err != nil {
		return domain.SeatMap{}, err
	}
	return seatMap, nil
}

func (s *Service) GetSeatMap(ctx context.Context, seatMapID string) (domain.SeatMap, error) {
	if seatMapID == "" {
		return domain.SeatMap{}, ErrInvalidSeatMapID
	}
	return s.seatMapRepository.GetByID(ctx, seatMapID)
}

func buildSeats(inputs []CreateSeatInput) ([]domain.Seat, error) {
	type seatKey struct {
		section string
		row     string
		number  string
	}

	seats := make([]domain.Seat, 0, len(inputs))
	seen := make(map[seatKey]struct{}, len(inputs))

	for _, inputSeat := range inputs {
		if inputSeat.Row == "" {
			return nil, ErrInvalidSeatRow
		}
		if inputSeat.Number == "" {
			return nil, ErrInvalidSeatNumber
		}

		key := seatKey{
			section: inputSeat.Section,
			row:     inputSeat.Row,
			number:  inputSeat.Number,
		}

		if _, exists := seen[key]; exists {
			return nil, ErrDuplicateSeat
		}
		seen[key] = struct{}{}

		seats = append(seats, domain.Seat{
			ID:      uuid.NewString(),
			Section: inputSeat.Section,
			Row:     inputSeat.Row,
			Number:  inputSeat.Number,
		})
	}
	return seats, nil
}
