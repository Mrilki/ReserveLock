APP_NAME := reservelock

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make fmt    - format Go code"
	@echo "  make vet    - run go vet"
	@echo "  make test   - run tests"
	@echo "  make build  - build all Go packages"

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

.PHONY: build
build:
	go build ./...