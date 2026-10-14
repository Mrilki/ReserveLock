# ReserveLock

ReserveLock is a Go backend for event seat booking. It handles concurrent reservations, payment results and booking analytics using REST, gRPC and Kafka.

### What it does

- Creates seat reservations with a limited hold time
- Confirms paid reservations and releases expired holds
- Handles concurrent requests for the same seats
- Supports simulated payment outcomes
- Shows seat availability and booking analytics

### Implementation

- Event-content stores event data in MongoDB and publishes events through a Transactional Outbox
- Reservation-service uses PostgreSQL transactions and row locks for seat holds, confirmation and expiration
- A background worker expires reservations and releases their seats
- Payment-service consumes reservation events and publishes payment results through its own Inbox and Outbox
- Kafka consumers commit offsets after persistence; Inbox records prevent repeated state changes
- Redis caches availability, while PostgreSQL remains the source of truth
- Analytics-service writes Kafka events to ClickHouse in batches and calculates booking counts, rates and timings
- `ReplacingMergeTree` with `FINAL` queries handles repeated analytics events

Public REST endpoints use `net/http`. Admin endpoints use Gin. Internal requests use gRPC, and contracts use Protobuf with Buf.

### Testing and deployment

- Unit tests cover application logic, Kafka decoding, outbox workers and API responses
- Integration tests run against PostgreSQL, MongoDB, Redis and ClickHouse
- Services use zap logging and graceful shutdown
- Goose manages SQL migrations
- Docker Compose runs the full local stack with healthchecks and persistent volumes
- GitHub Actions runs contract checks, formatting, vet, tests and builds

### Run

```sh
docker compose --profile app up --build -d
```