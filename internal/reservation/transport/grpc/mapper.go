package reservationgrpc

import (
	reservationv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/reservation/v1"
	"github.com/Mrilki/ReserveLock/internal/reservation/domain"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toProtoReservationStatus(status domain.ReservationStatus) reservationv1.ReservationStatus {
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

func toProtoReservation(reservation domain.Reservation) *reservationv1.Reservation {
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

func toProtoSeatAvailabilityStatus(status domain.SeatInventoryStatus) reservationv1.SeatAvailabilityStatus {
	switch status {
	case domain.SeatInventoryStatusAvailable:
		return reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_AVAILABLE

	case domain.SeatInventoryStatusHeld:
		return reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_HELD

	case domain.SeatInventoryStatusReserved:
		return reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_RESERVED

	default:
		return reservationv1.SeatAvailabilityStatus_SEAT_AVAILABILITY_STATUS_UNSPECIFIED
	}
}

func toProtoSeatAvailability(seat domain.SeatInventory) *reservationv1.SeatAvailability {
	result := &reservationv1.SeatAvailability{
		SeatId: seat.SeatID,
		Status: toProtoSeatAvailabilityStatus(seat.Status),
	}
	if !seat.HeldUntil.IsZero() {
		result.HeldUntil = timestamppb.New(seat.HeldUntil)
	}

	return result
}
