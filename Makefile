.PHONY: help test test-coverage lint lint-fix build clean

# Default target - show help
help:
	@echo "Available targets:"
	@echo "  make test          - Run all tests"
	@echo "  make test-coverage - Run tests with coverage report"
	@echo "  make lint          - Run golangci-lint"
	@echo "  make lint-fix      - Run golangci-lint with auto-fix"
	@echo "  make build         - Build the module"
	@echo "  make clean         - Remove build artifacts and coverage files"

# Run all tests
test:
	go test -race -v ./...

# Run tests with coverage
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Run linter
lint:
	golangci-lint run

# Run linter with auto-fix
lint-fix:
	golangci-lint run --fix

# Build the module
build:
	go build ./...

# Clean build artifacts
clean:
	rm -f coverage.out
	go clean
