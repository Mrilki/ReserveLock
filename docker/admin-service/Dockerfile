# syntax=docker/dockerfile:1
FROM golang:1.26.9-bookworm AS build
WORKDIR /src
ENV GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/admin-service

FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/service /app/service
COPY --from=build /out/healthcheck /app/healthcheck
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/service"]