package redis

import (
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const redisOperationTimeout = 100 * time.Millisecond

func NewClient(redisURL string) (*goredis.Client, error) {
	options, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	options.DialTimeout = redisOperationTimeout
	options.ReadTimeout = redisOperationTimeout
	options.WriteTimeout = redisOperationTimeout
	options.PoolTimeout = redisOperationTimeout
	options.ContextTimeoutEnabled = true
	options.MaxRetries = -1

	return goredis.NewClient(options), nil
}
