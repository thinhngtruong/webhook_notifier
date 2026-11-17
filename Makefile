# Homebrew Go first: the system Go on this machine is too old.
export PATH := /opt/homebrew/bin:$(PATH)

COMPOSE ?= docker compose
# Integration tests create throwaway databases on the compose Postgres.
TEST_DATABASE_URL ?= postgres://notifier:notifier@localhost:55432/notifier?sslmode=disable

.PHONY: up down db test test-unit lint fmt build bench replay up-metrics

up: ## build and start postgres, notifier x2, producer, partner
	$(COMPOSE) up -d --build --force-recreate

down:
	$(COMPOSE) down -v

db: ## start only postgres and wait until it is healthy
	$(COMPOSE) up -d --wait postgres

build:
	go build ./...

test: db ## unit + integration tests
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -timeout 5m ./...

test-unit: ## unit tests only; integration tests skip without TEST_DATABASE_URL
	go test -race -count=1 -timeout 5m ./...

fmt:
	gofmt -w .

lint:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	go vet ./...

bench:
	@echo "bench: not implemented yet (step 8)"

replay: ## replay dead deliveries, e.g. make replay ARGS="--account=account-123"
	$(COMPOSE) exec -T notifier-1 notifier replay $(ARGS)

up-metrics: ## make up plus Prometheus on http://localhost:9091
	$(COMPOSE) --profile metrics up -d --build --force-recreate
