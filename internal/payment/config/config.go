package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultGRPCAddr     = ":50053"
	defaultPostgresDSN  = "postgres://reservelock:reservelock-local@localhost:5433/reservelock_payment?sslmode=disable"
	defaultKafkaBrokers = "localhost:9092"
)

type Config struct {
	Base         platformconfig.BaseConfig
	GRPCAddr     string
	PostgresDSN  string
	KafkaBrokers []string
}

func Load() Config {
	return Config{
		Base:         platformconfig.LoadBase("payment-service"),
		GRPCAddr:     platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
		PostgresDSN:  platformconfig.GetEnv("PAYMENT_POSTGRES_DSN", defaultPostgresDSN),
		KafkaBrokers: platformconfig.GetCSVEnv("KAFKA_BROKERS", defaultKafkaBrokers),
	}
}
