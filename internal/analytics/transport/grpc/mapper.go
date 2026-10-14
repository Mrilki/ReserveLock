package analyticsgrpc

import (
	"github.com/Mrilki/ReserveLock/internal/analytics/domain"
	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
)

func toProtoEventSummary(summary domain.EventSummary) *analyticsv1.EventSummary {
	daily := make([]*analyticsv1.EventDailySummary, 0, len(summary.Daily))
	for _, day := range summary.Daily {
		daily = append(daily, &analyticsv1.EventDailySummary{
			Date:           day.Date,
			Created:        day.Created,
			Confirmed:      day.Confirmed,
			Expired:        day.Expired,
			CreatedSeats:   day.CreatedSeats,
			ConfirmedSeats: day.ConfirmedSeats,
			ExpiredSeats:   day.ExpiredSeats,
		})
	}
	return &analyticsv1.EventSummary{
		EventId:                 summary.EventID,
		Created:                 summary.Created,
		Confirmed:               summary.Confirmed,
		Expired:                 summary.Expired,
		CreatedSeats:            summary.CreatedSeats,
		ConfirmedSeats:          summary.ConfirmedSeats,
		ExpiredSeats:            summary.ExpiredSeats,
		ConfirmationRatePercent: summary.ConfirmationRatePercent,
		ExpirationRatePercent:   summary.ExpirationRatePercent,
		AvgConfirmationSeconds:  summary.AvgConfirmationSeconds,
		AvgExpirationLagSeconds: summary.AvgExpirationLagSeconds,
		Daily:                   daily,
	}
}
