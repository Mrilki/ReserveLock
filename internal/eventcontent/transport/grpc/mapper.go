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
		CreatedBy: venue.CreateBy,
	}
}
