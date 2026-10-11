package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultEventContentGRPCAddr = "localhost:50051"
	defaultReservationGRPCAddr  = "localhost:50052"
	defaultPaymentGRPCAddr      = "localhost:50053"
)

type Config struct {
	Base                 platformconfig.BaseConfig
	EventContentGRPCAddr string
	ReservationGRPCAddr  string
	PaymentGRPCAddr      string
}

func Load() Config {
	return Config{
		Base: platformconfig.LoadBase("public-api"),
		EventContentGRPCAddr: platformconfig.GetEnv(
			"EVENT_CONTENT_GRPC_ADDR",
			defaultEventContentGRPCAddr,
		),
		ReservationGRPCAddr: platformconfig.GetEnv(
			"RESERVATION_GRPC_ADDR",
			defaultReservationGRPCAddr,
		),
		PaymentGRPCAddr: platformconfig.GetEnv(
			"PAYMENT_GRPC_ADDR",
			defaultPaymentGRPCAddr,
		),
	}
}
