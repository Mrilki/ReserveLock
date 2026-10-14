package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultClickHouseDSN = "clickhouse://reservelock:reservelock-local@localhost:9000/reservelock_analytics"
	defaultKafkaBrokers  = "localhost:9092"
	defaultGRPCAddr      = ":50054"
)

type Config struct {
	Base          platformconfig.BaseConfig
	ClickHouseDSN string
	KafkaBrokers  []string
	GRPCAddr      string
}

func Load() Config {
	return Config{
		Base:          platformconfig.LoadBase("analytics-service"),
		ClickHouseDSN: platformconfig.GetEnv("ANALYTICS_CLICKHOUSE_DSN", defaultClickHouseDSN),
		KafkaBrokers:  platformconfig.GetCSVEnv("KAFKA_BROKERS", defaultKafkaBrokers),
		GRPCAddr:      platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
	}
}
