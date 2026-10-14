package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultEventContentGRPCAddr = "localhost:50051"
	defaultAnalyticsGRPCAddr    = "localhost:50054"
)

type Config struct {
	Base                 platformconfig.BaseConfig
	EventContentGRPCAddr string
	AnalyticsGRPCAddr    string
}

func Load() Config {
	return Config{
		Base:                 platformconfig.LoadBase("admin-service"),
		EventContentGRPCAddr: platformconfig.GetEnv("EVENT_CONTENT_GRPC_ADDR", defaultEventContentGRPCAddr),
		AnalyticsGRPCAddr:    platformconfig.GetEnv("ANALYTICS_GRPC_ADDR", defaultAnalyticsGRPCAddr),
	}
}
