package config

import platformconfig "github.com/Mrilki/ReserveLock/internal/platform/config"

const (
	defaultGRPCAddr      = ":50051"
	defaultMongoURI      = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
	defaultMongoDatabase = "reservelock_event_content"
)

type Config struct {
	Base          platformconfig.BaseConfig
	GRPCAddr      string
	MongoURI      string
	MongoDatabase string
}

func Load() Config {
	return Config{
		Base:          platformconfig.LoadBase("event-content-service"),
		GRPCAddr:      platformconfig.GetEnv("GRPC_ADDR", defaultGRPCAddr),
		MongoURI:      platformconfig.GetEnv("EVENT_CONTENT_MONGO_URI", defaultMongoURI),
		MongoDatabase: platformconfig.GetEnv("EVENT_CONTENT_MONGO_DATABASE", defaultMongoDatabase),
	}
}
