package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultGRPCAddr     = ":50052"
	defaultPostgresDSN  = "postgres://reservelock:reservelock-local@localhost:5433/reservelock_reservation?sslmode=disable"
	defaultKafkaBrokers = "localhost:9092"
	defaultRedisURL     = "redis://localhost:6379/0"
)

type Config struct {
	Base         platformconfig.BaseConfig
	GRPCAddr     string
	PostgresDSN  string
	KafkaBrokers []string
	RedisURL     string
}

func Load() Config {
	return Config{
		Base:         platformconfig.LoadBase("reservation-service"),
		GRPCAddr:     platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
		PostgresDSN:  platformconfig.GetEnv("RESERVATION_POSTGRES_DSN", defaultPostgresDSN),
		KafkaBrokers: platformconfig.GetCSVEnv("KAFKA_BROKERS", defaultKafkaBrokers),
		RedisURL:     platformconfig.GetEnv("RESERVATION_REDIS_URL", defaultRedisURL),
	}
}
