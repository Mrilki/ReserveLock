package eventcontentgrpc

import (
	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
)

func toProtoVenue(venue domain.Venue) *eventcontentv1.Venue {
	return &eventcontentv1.Venue{
		Id:        venue.ID,
		Name:      venue.Name,
		City:      venue.City,
		Address:   venue.Address,
		CreatedBy: venue.CreatedBy,
	}
}

func toProtoHall(hall domain.Hall) *eventcontentv1.Hall {
	return &eventcontentv1.Hall{
		Id:        hall.ID,
		VenueId:   hall.VenueID,
		Name:      hall.Name,
		CreatedBy: hall.CreatedBy,
	}
}

func toProtoSeat(seat domain.Seat) *eventcontentv1.Seat {
	return &eventcontentv1.Seat{
		Id:      seat.ID,
		Section: seat.Section,
		Row:     seat.Row,
		Number:  seat.Number,
	}
}

func toProtoSeatMap(seatMap domain.SeatMap) *eventcontentv1.SeatMap {
	seats := make([]*eventcontentv1.Seat, 0, len(seatMap.Seats))
	for _, seat := range seatMap.Seats {
		seats = append(seats, toProtoSeat(seat))
	}

	return &eventcontentv1.SeatMap{
		Id:        seatMap.ID,
		HallId:    seatMap.HallID,
		Name:      seatMap.Name,
		Seats:     seats,
		CreatedBy: seatMap.CreatedBy,
	}
}
