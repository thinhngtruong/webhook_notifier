# Homebrew Go first: the system Go on this machine is too old.
export PATH := /opt/homebrew/bin:$(PATH)

COMPOSE ?= docker compose
# Integration tests create throwaway databases on the compose Postgres.
TEST_DATABASE_URL ?= postgres://notifier:notifier@localhost:55432/notifier?sslmode=disable

.PHONY: requeue-events test-alerts up down db test test-unit lint fmt build bench replay up-metrics

up: ## build and start postgres, notifier x2, producer, partner
	$(COMPOSE) up -d --build --force-recreate

down:
	$(COMPOSE) down -v

db: ## start only postgres and wait until it is healthy
	$(COMPOSE) up -d --wait postgres

build:
	go build ./...

PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0

ALERTMANAGER_IMAGE ?= prom/alertmanager:v0.28.1
AMTOOL = docker run --rm -v "$(CURDIR)/deploy:/deploy:ro" --entrypoint amtool $(ALERTMANAGER_IMAGE)

test-alerts: ## unit-test alert rules (promtool) and Alertmanager routing (amtool)
	docker run --rm -v "$(CURDIR)/deploy:/deploy:ro" --entrypoint promtool $(PROMETHEUS_IMAGE) check rules /deploy/alerts.yml
	docker run --rm -v "$(CURDIR)/deploy:/deploy:ro" -w /deploy --entrypoint promtool $(PROMETHEUS_IMAGE) test rules alerts_test.yml
	$(AMTOOL) check-config /deploy/alertmanager.yml
	@got=$$($(AMTOOL) config routes test --config.file=/deploy/alertmanager.yml severity=page slo=freshness); \
	  test "$$got" = "pager" || { echo "page routed to '$$got', want pager"; exit 1; }
	@got=$$($(AMTOOL) config routes test --config.file=/deploy/alertmanager.yml severity=ticket alertname=WebhookDeadLetterQueueLarge); \
	  test "$$got" = "tickets" || { echo "ticket routed to '$$got', want tickets"; exit 1; }
	@echo "alertmanager routing: OK"

test: db test-alerts ## unit + integration tests, and alert rule tests
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -timeout 5m ./...

test-unit: ## unit tests only; integration tests skip without TEST_DATABASE_URL
	go test -race -count=1 -timeout 5m ./...

fmt:
	gofmt -w .

lint:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	go vet ./...

# BENCH_SCALE=quick|default|full (full uses design.md sizes and takes hours).
BENCH_SCALE ?= default

bench: db ## run the four benchmark scenarios and print a report
	go run ./cmd/bench -scale $(BENCH_SCALE) -database-url "$(TEST_DATABASE_URL)"

replay: ## replay dead deliveries, e.g. make replay ARGS="--account=account-123"
	$(COMPOSE) exec -T notifier-1 notifier replay $(ARGS)

requeue-events: ## requeue input dead letters, e.g. make requeue-events ARGS="--reason-prefix=received"
	$(COMPOSE) exec -T notifier-1 notifier requeue-events $(ARGS)

up-metrics: ## make up plus Prometheus (:9091), Alertmanager (:9093) and the trace viewer (:16686)
	$(COMPOSE) --profile metrics up -d --build --force-recreate
