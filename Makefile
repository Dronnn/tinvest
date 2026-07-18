# tinvest — build and codegen pipeline.
#
# All tooling runs through `go run <module>@<pinned-version>`; nothing is
# installed system-wide. Requires Go 1.26+ on PATH.

# Pinned tool versions.
BUF_VERSION           := v1.72.0
PROTOC_GEN_GO         := v1.36.11
PROTOC_GEN_GO_GRPC    := v1.6.2
GOLANGCI_LINT_VERSION := v2.12.2

BUF := go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

.DEFAULT_GOAL := build

.PHONY: build test vet lint proto proto-lint tidy clean

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

# Lint via a pinned golangci-lint run (no system install). The first run
# compiles the linter and can take a minute; subsequent runs are cached.
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

# Regenerate internal/pb/investapi from the vendored protos in proto/.
# Plugin versions are pinned inside buf.gen.yaml; buf itself is pinned above.
proto:
	$(BUF) generate

# Optional: protobuf-aware lint of the vendored contracts.
proto-lint:
	$(BUF) lint

tidy:
	go mod tidy

clean:
	rm -rf internal/pb/investapi
