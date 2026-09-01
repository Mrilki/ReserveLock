package reservationgrpc

import (
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toProtoReservationStatus(
	status domain.ReservationStatus,
) reservationv1.ReservationStatus {
	switch status {
	case domain.ReservationStatusPending:
		return reservationv1.ReservationStatus_RESERVATION_STATUS_PENDING

	case domain.ReservationStatusConfirmed:
		return reservationv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED

	case domain.ReservationStatusExpired:
		return reservationv1.ReservationStatus_RESERVATION_STATUS_EXPIRED

	default:
		return reservationv1.ReservationStatus_RESERVATION_STATUS_UNSPECIFIED
	}
}

func toProtoReservation(
	reservation domain.Reservation,
) *reservationv1.Reservation {
	return &reservationv1.Reservation{
		Id:        reservation.ID,
		EventId:   reservation.EventID,
		UserId:    reservation.UserID,
		SeatIds:   append([]string(nil), reservation.SeatIDs...),
		Status:    toProtoReservationStatus(reservation.Status),
		CreatedAt: timestamppb.New(reservation.CreatedAt),
		ExpiresAt: timestamppb.New(reservation.ExpiresAt),
	}
}
