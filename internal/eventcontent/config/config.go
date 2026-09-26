package config

import (
	"strings"

	platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"
)

const (
	defaultGRPCAddr      = ":50051"
	defaultMongoURI      = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
	defaultMongoDatabase = "reservelock_event_content"
	defaultKafkaBrokers  = "localhost:9092"
)

type Config struct {
	Base          platformconfig.BaseConfig
	GRPCAddr      string
	MongoURI      string
	MongoDatabase string
	KafkaBrokers  []string
}

func Load() Config {
	return Config{
		Base:          platformconfig.LoadBase("event-content-service"),
		GRPCAddr:      platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
		MongoURI:      platformconfig.GetEnv("EVENT_CONTENT_MONGO_URI", defaultMongoURI),
		MongoDatabase: platformconfig.GetEnv("EVENT_CONTENT_MONGO_DATABASE", defaultMongoDatabase),
		KafkaBrokers:  parseBrokers(platformconfig.GetEnv("KAFKA_BROKERS", defaultKafkaBrokers)),
	}
}

func parseBrokers(value string) []string {
	parts := strings.Split(value, ",")

	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
