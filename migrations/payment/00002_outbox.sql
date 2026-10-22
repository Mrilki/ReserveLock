-- +goose Up
CREATE TABLE outbox_messages
(
    message_id   TEXT PRIMARY KEY CHECK (message_id <> ''),
    event_type   TEXT NOT NULL CHECK (event_type <> ''),
    key          TEXT NOT NULL CHECK (key <> ''),
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_messages_key_event_type_unique UNIQUE (key, event_type)
);

CREATE INDEX idx_outbox_messages_pending
    ON outbox_messages (occurred_at, message_id)
    WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox_messages;
