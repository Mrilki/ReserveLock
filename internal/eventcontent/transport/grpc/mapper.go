package eventcontentgrpc

import (
	"github.com/Mrilki/ReserveLock/internal/eventcontent/domain"
	eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func toProtoEventStatus(status domain.EventStatus) eventcontentv1.EventStatus {
	switch status {
	case domain.EventStatusDraft:
		return eventcontentv1.EventStatus_EVENT_STATUS_DRAFT
	case domain.EventStatusPublished:
		return eventcontentv1.EventStatus_EVENT_STATUS_PUBLISHED
	default:
		return eventcontentv1.EventStatus_EVENT_STATUS_UNSPECIFIED
	}

}

func toProtoEvent(event domain.Event) *eventcontentv1.Event {
	return &eventcontentv1.Event{
		Id:        event.ID,
		VenueId:   event.VenueID,
		HallId:    event.HallID,
		SeatMapId: event.SeatMapID,
		Name:      event.Name,
		StartsAt:  timestamppb.New(event.StartsAt),
		Status:    toProtoEventStatus(event.Status),
		CreatedBy: event.CreatedBy,
	}
}
