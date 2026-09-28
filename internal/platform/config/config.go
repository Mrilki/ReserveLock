package config

import (
	"os"
	"strings"
)

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

func GetCSVEnv(key, fallback string) []string {
	parts := strings.Split(GetEnv(key, fallback), ",")

	values := make([]string, 0, len(parts))

	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}

	return values
}
