-- +goose Up

CREATE TABLE inbox_messages
(
    message_id   TEXT PRIMARY KEY CHECK (message_id <> ''),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down

DROP TABLE inbox_messages;