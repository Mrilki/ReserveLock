package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const defaultGRPCAddr = ":50052"

type Config struct {
	Base     platformconfig.BaseConfig
	GRPCAddr string
}

func Load() *Config {
	return &Config{
		Base:     platformconfig.LoadBase("reservation-service"),
		GRPCAddr: platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
	}
}
