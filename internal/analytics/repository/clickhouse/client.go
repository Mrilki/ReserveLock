package clickhouse

import (
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	clickhouseDialTimeout = 5 * time.Second
	clickhouseReadTimeout = 10 * time.Second
)

func NewClient(dsn string) (driver.Conn, error) {
	options, err := ch.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse analytics clickhouse dsn: %w", err)
	}

	options.DialTimeout = clickhouseDialTimeout
	options.ReadTimeout = clickhouseReadTimeout
	options.MaxOpenConns = 5
	options.MaxIdleConns = 2
	options.Compression = &ch.Compression{Method: ch.CompressionLZ4}

	if options.Settings == nil {
		options.Settings = ch.Settings{}
	}
	options.Settings["async_insert"] = 0

	client, err := ch.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open analytics clickhouse client: %w", err)
	}

	return client, nil
}
