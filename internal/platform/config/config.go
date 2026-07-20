package config

import "os"

const (
	defaultHTTPAddr = ":8080"
	defaultAppEnv   = "local"
)

type BaseConfig struct {
	AppName  string
	AppEnv   string
	HTTPAddr string
}

func LoadBase(appName string) BaseConfig {
	return BaseConfig{
		AppName:  GetEnv("APP_NAME", appName),
		AppEnv:   GetEnv("APP_ENV", defaultAppEnv),
		HTTPAddr: GetEnv("HTTP_ADDR", defaultHTTPAddr),
	}
}

func GetEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
