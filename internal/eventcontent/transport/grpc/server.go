package eventcontentgrpc

import eventcontentv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/eventcontent/v1"

type Server struct {
	eventcontentv1.UnimplementedVenueServiceServer
}

func NewServer() *Server {
	return &Server{}
}
