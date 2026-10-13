-- +goose NO TRANSACTION

-- +goose Up
CREATE TABLE IF NOT EXISTS reservation_events (
    message_id String,
    event_id String,
    reservation_id String,
    event_type Enum8('CREATED' = 1, 'CONFIRMED' = 2, 'EXPIRED' = 3),
    seat_ids Array(String),
    created_at DateTime64(6, 'UTC'),
    expires_at DateTime64(6, 'UTC'),
    occurred_at DateTime64(6, 'UTC')
    )
    ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(created_at)
    ORDER BY (event_id, message_id);

-- +goose Down
DROP TABLE IF EXISTS reservation_events;