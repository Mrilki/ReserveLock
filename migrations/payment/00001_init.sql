-- +goose Up
CREATE TABLE payments
(
    id             TEXT PRIMARY KEY,
    reservation_id TEXT        NOT NULL UNIQUE,
    user_id        TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (
        status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'CANCELLED')
        ),
    created_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > created_at)
);

CREATE TABLE inbox_messages
(
    message_id   TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- +goose Down
DROP TABLE inbox_messages;
DROP TABLE payments;