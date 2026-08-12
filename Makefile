# ab0t-events-go — developer tasks
GO ?= go

.DEFAULT_GOAL := check

.PHONY: check build test race cover vet fmt fmt-check tidy lint example clean

## check: the pre-commit bar (fmt-check + vet + build + race)
check: fmt-check vet build race

## build: compile everything
build:
	$(GO) build ./...

## test: run the unit + integration tests
test:
	$(GO) test ./...

## race: run all tests under the race detector
race:
	$(GO) test -race -count=1 ./...

## cover: produce a coverage report (coverage.html)
cover:
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "coverage → coverage.html"

## vet: static checks
vet:
	$(GO) vet ./...

## fmt: format all Go files
fmt:
	gofmt -w .

## fmt-check: fail if anything is unformatted
fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

## tidy: sync go.mod/go.sum
tidy:
	$(GO) mod tidy

## lint: run golangci-lint if installed (optional)
lint:
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed; skipping"

## example: run the quickstart wire-in example
example:
	$(GO) run ./examples/quickstart

## clean: remove build/coverage artifacts
clean:
	rm -f coverage.out coverage.html
	rm -rf bin
