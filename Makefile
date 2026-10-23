GO ?= go
COMMIT ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
VERSION ?= v$(shell cat VERSION)-$(COMMIT)
DIRTY ?= $(if $(shell git status --porcelain --untracked-files=normal 2>/dev/null),-dirty)
BUILD_VERSION = $(VERSION)$(DIRTY)
LDFLAGS = -X github.com/livepeer/node/cmd/version.Version=$(BUILD_VERSION) -X github.com/livepeer/node/cmd/version.Commit=$(COMMIT)

.PHONY: build test
build:
	mkdir -p bin
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/livepeer ./cmd/livepeer
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/livepeer-orchestrator ./cmd/livepeer-orchestrator
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/livepeer-signer ./cmd/livepeer-signer
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/livepeer-chain ./cmd/livepeer-chain
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/livepeer-discovery ./cmd/livepeer-discovery

test:
	$(GO) test -race ./...
