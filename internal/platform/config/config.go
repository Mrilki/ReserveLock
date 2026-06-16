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
		AppName:  getEnv("APP_NAME", appName),
		AppEnv:   getEnv("APP_ENV", defaultAppEnv),
		HTTPAddr: getEnv("HTTP_ADDR", defaultHTTPAddr),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
