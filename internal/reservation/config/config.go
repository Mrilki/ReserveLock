package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultGRPCAddr    = ":50052"
	defaultPostgresDSN = "postgres://reservelock:reservelock-local@localhost:5433/reservelock_reservation?sslmode=disable"
)

type Config struct {
	Base        platformconfig.BaseConfig
	GRPCAddr    string
	PostgresDSN string
}

func Load() Config {
	return Config{
		Base:        platformconfig.LoadBase("reservation-service"),
		GRPCAddr:    platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
		PostgresDSN: platformconfig.GetEnv("RESERVATION_POSTGRES_DSN", defaultPostgresDSN),
	}
}
