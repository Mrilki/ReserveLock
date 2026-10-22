package mongodb

import (
	"time"

	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
)

type venueDocument struct {
	ID        string `bson:"_id"`
	Name      string `bson:"name"`
	City      string `bson:"city"`
	Address   string `bson:"address"`
	CreatedBy string `bson:"created_by"`
}

func toVenueDocument(venue domain.Venue) venueDocument {
	return venueDocument{
		ID:        venue.ID,
		Name:      venue.Name,
		City:      venue.City,
		Address:   venue.Address,
		CreatedBy: venue.CreatedBy,
	}
}

func (d venueDocument) toDomain() domain.Venue {
	return domain.Venue{
		ID:        d.ID,
		Name:      d.Name,
		City:      d.City,
		Address:   d.Address,
		CreatedBy: d.CreatedBy,
	}
}

type hallDocument struct {
	ID        string `bson:"_id"`
	VenueID   string `bson:"venue_id"`
	Name      string `bson:"name"`
	CreatedBy string `bson:"created_by"`
}

func toHallDocument(hall domain.Hall) hallDocument {
	return hallDocument{
		ID:        hall.ID,
		VenueID:   hall.VenueID,
		Name:      hall.Name,
		CreatedBy: hall.CreatedBy,
	}
}

func (d hallDocument) toDomain() domain.Hall {
	return domain.Hall{
		ID:        d.ID,
		VenueID:   d.VenueID,
		Name:      d.Name,
		CreatedBy: d.CreatedBy,
	}
}

type seatDocument struct {
	ID      string `bson:"id"`
	Section string `bson:"section"`
	Row     string `bson:"row"`
	Number  string `bson:"number"`
}

type seatMapDocument struct {
	ID        string         `bson:"_id"`
	HallID    string         `bson:"hall_id"`
	Name      string         `bson:"name"`
	Seats     []seatDocument `bson:"seats"`
	CreatedBy string         `bson:"created_by"`
}

func toSeatMapDocument(seatMap domain.SeatMap) seatMapDocument {
	seats := make([]seatDocument, 0, len(seatMap.Seats))

	for _, seat := range seatMap.Seats {
		seats = append(seats, seatDocument{
			ID:      seat.ID,
			Section: seat.Section,
			Row:     seat.Row,
			Number:  seat.Number,
		})
	}

	return seatMapDocument{
		ID:        seatMap.ID,
		HallID:    seatMap.HallID,
		Name:      seatMap.Name,
		Seats:     seats,
		CreatedBy: seatMap.CreatedBy,
	}
}

func (d seatMapDocument) toDomain() domain.SeatMap {
	seats := make([]domain.Seat, 0, len(d.Seats))

	for _, seat := range d.Seats {
		seats = append(seats, domain.Seat{
			ID:      seat.ID,
			Section: seat.Section,
			Row:     seat.Row,
			Number:  seat.Number,
		})
	}

	return domain.SeatMap{
		ID:        d.ID,
		HallID:    d.HallID,
		Name:      d.Name,
		Seats:     seats,
		CreatedBy: d.CreatedBy,
	}
}

type eventDocument struct {
	ID        string             `bson:"_id"`
	VenueID   string             `bson:"venue_id"`
	HallID    string             `bson:"hall_id"`
	SeatMapID string             `bson:"seat_map_id"`
	Name      string             `bson:"name"`
	StartsAt  time.Time          `bson:"starts_at"`
	Status    domain.EventStatus `bson:"status"`
	CreatedBy string             `bson:"created_by"`
}

type outboxDocument struct {
	ID          string     `bson:"_id"`
	EventType   string     `bson:"event_type"`
	Key         string     `bson:"key"`
	Payload     []byte     `bson:"payload"`
	OccurredAt  time.Time  `bson:"occurred_at"`
	PublishedAt *time.Time `bson:"published_at"`
}

func toEventDocument(event domain.Event) eventDocument {
	return eventDocument{
		ID:        event.ID,
		VenueID:   event.VenueID,
		HallID:    event.HallID,
		SeatMapID: event.SeatMapID,
		Name:      event.Name,
		StartsAt:  event.StartsAt,
		Status:    event.Status,
		CreatedBy: event.CreatedBy,
	}
}

func (d eventDocument) toDomain() domain.Event {
	return domain.Event{
		ID:        d.ID,
		VenueID:   d.VenueID,
		HallID:    d.HallID,
		SeatMapID: d.SeatMapID,
		Name:      d.Name,
		StartsAt:  d.StartsAt,
		Status:    d.Status,
		CreatedBy: d.CreatedBy,
	}
}
