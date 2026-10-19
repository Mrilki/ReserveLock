-- +goose Up

CREATE TABLE event_inventories
(
    event_id TEXT PRIMARY KEY CHECK (event_id <> '')
);

CREATE TABLE reservations
(
    id         TEXT PRIMARY KEY CHECK (id <> ''),
    event_id   TEXT        NOT NULL REFERENCES event_inventories (event_id),
    user_id    TEXT        NOT NULL CHECK (user_id <> ''),
    status     TEXT        NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'EXPIRED')),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,

    CHECK (expires_at > created_at)
);

CREATE TABLE seat_inventory
(
    event_id       TEXT NOT NULL REFERENCES event_inventories (event_id) ON DELETE CASCADE,
    seat_id        TEXT NOT NULL CHECK (seat_id <> ''),
    status         TEXT NOT NULL CHECK (status IN ('AVAILABLE', 'HELD', 'RESERVED')),
    reservation_id TEXT REFERENCES reservations (id) ON DELETE RESTRICT,
    held_until     TIMESTAMPTZ,
    PRIMARY KEY (event_id, seat_id),
    CHECK (
        (status = 'AVAILABLE' AND reservation_id IS NULL AND held_until IS NULL)
        OR (status = 'HELD' AND reservation_id IS NOT NULL AND held_until IS NOT NULL)
        OR (status = 'RESERVED' AND reservation_id IS NOT NULL AND held_until IS NULL)
    )
);

CREATE TABLE reservation_seats
(
    reservation_id TEXT   NOT NULL REFERENCES reservations (id) ON DELETE CASCADE,
    seat_id        TEXT   NOT NULL CHECK (seat_id <> ''),
    position       BIGINT NOT NULL CHECK (position > 0),
    PRIMARY KEY (reservation_id, seat_id),
    UNIQUE (reservation_id, position)
);

CREATE INDEX idx_reservations_pending_expiration
    ON reservations (expires_at, id)
    WHERE status = 'PENDING';

-- +goose Down

DROP TABLE reservation_seats;
DROP TABLE seat_inventory;
DROP TABLE reservations;
DROP TABLE event_inventories;
