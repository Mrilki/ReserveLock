# syntax=docker/dockerfile:1
FROM golang:1.26.9-bookworm AS build
WORKDIR /src
ENV GOTOOLCHAIN=local

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOBIN=/out go install github.com/pressly/goose/v3/cmd/goose@v3.27.0

FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/goose /app/goose
COPY migrations /migrations
USER 65532:65532
ENTRYPOINT ["/app/goose"]