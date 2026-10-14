APP_NAME := reservelock

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make fmt            - format Go code"
	@echo "  make vet            - run go vet"
	@echo "  make test           - run tests"
	@echo "  make build          - build all Go packages"
	@echo "  make proto-lint     - lint protobuf files"
	@echo "  make proto-build    - check protobuf files compile"
	@echo "  make proto-generate - generate Go code from protobuf"
	@echo "  make proto          - run all protobuf checks"

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test -count=1 -timeout=5m ./...

.PHONY: build
build:
	go build ./...

.PHONY: proto-lint
proto-lint:
	buf lint

.PHONY: proto-build
proto-build:
	buf build

.PHONY: proto-generate
proto-generate:
	buf generate

.PHONY: proto
proto: proto-lint proto-build proto-generate

