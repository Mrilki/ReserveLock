package analyticsgrpc

import (
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
)

func toProtoEventSummary(summary domain.EventSummary) *analyticsv1.EventSummary {
	return &analyticsv1.EventSummary{
		EventId:        summary.EventID,
		Created:        summary.Created,
		Confirmed:      summary.Confirmed,
		Expired:        summary.Expired,
		CreatedSeats:   summary.CreatedSeats,
		ConfirmedSeats: summary.ConfirmedSeats,
		ExpiredSeats:   summary.ExpiredSeats,
	}
}
