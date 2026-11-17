# Webhook Notifier

Proof of concept for a webhook delivery service: it consumes subscriber events, fans each one out to the webhooks registered on the account, and POSTs the payload to each webhook's `post_url` reliably and fairly across accounts.

**Read `docs/design.md` before writing code.** It is the source of truth for the architecture, data model, claim query, retry policy and metrics. `docs/bottlenecks.md` covers scaling limits. If the code needs to diverge from the design, say so and update the doc in the same change.

## Hard rules

- Never mention any real company or product brand in code, comments, docs, commit messages or test data. Use neutral names (`webhook-notifier`, `acme`, `account-123`).
- Stack: Go 1.27 (go.mod pins 1.27.4) and PostgreSQL 16. Keep dependencies few: `pgx/v5` for Postgres, `prometheus/client_golang` for metrics, the standard library for HTTP and logging (`log/slog`). Ask before adding anything else.
- Semantics: at-least-once delivery, no ordering guarantees. Do not add ordering or exactly-once machinery.
- Everything must run locally with one command (`make up`) and no cloud accounts.

## Architecture in one paragraph

An `EventSource` interface feeds the **Dispatcher**, which in one transaction inserts one row per (event, webhook) into `deliveries` (id = deterministic UUID of event_id + webhook_id, `ON CONFLICT DO NOTHING`) and activates the account in `account_queues`, then acks the event. Each replica runs a **claim loop** (the fair round-robin query in design.md, `FOR UPDATE SKIP LOCKED`, quantum per account) that leases deliveries to an in-process **worker pool**. Workers POST with `X-Webhook-Id`, `X-Webhook-Timestamp`, `X-Webhook-Signature` (HMAC-SHA256), then ack: succeeded, rescheduled with backoff + jitter, or dead. A **Lease Reaper** resets expired leases. Per-endpoint concurrency limits and circuit breakers protect partners.

## Repository layout (target)

```
cmd/
  notifier/    main binary; flags enable roles: --dispatcher --worker --reaper
  producer/    fake event source: configurable whale + small-account mix
  partner/     fake webhook receiver: configurable latency, error rate, outages;
               records receive time per delivery id (latency + duplicate count)
  bench/       runs scenarios and prints a report
internal/
  config/      env/flag config (quantum, accounts per tick, backoff, timeouts)
  event/       EventSource interface + in-memory/Postgres-backed mock queue
  store/       Postgres access: migrations, fan-out insert, claim, ack, reaper
  dispatcher/
  scheduler/   claim loop; also a "shared queue" mode used as the benchmark baseline
  worker/      HTTP delivery, signing, response classification, backoff
  partnerguard/ per-endpoint semaphore + circuit breaker
  metrics/
migrations/    plain SQL files, applied on startup
docs/          design.md, bottlenecks.md, img/, diagrams/ (Excalidraw sources)
docker-compose.yml, Makefile, README.md
```

## Implementation plan

Work in this order. Finish each step with passing tests and a small commit before starting the next.

1. **Skeleton**: go module, layout, config, `docker-compose.yml` (postgres, notifier x2, producer, partner, prometheus optional), `Makefile` with `up`, `down`, `test`, `bench`, `lint`.
2. **Schema and store**: migrations for `webhooks`, `deliveries` (partitioned by day on `event_time`, PK `(id, event_time)`), `account_queues`, and the partial indexes from design.md. Store functions: fan-out insert, claim, ack (succeeded / retry / dead), reset expired leases, replay dead. Integration tests against a real Postgres.
3. **Dispatcher**: consume from `EventSource`, cached webhook lookup (30s TTL), batch insert, ack after commit.
4. **Scheduler + worker**: claim loop sized to free worker slots, HTTP client (10s timeout, no redirects), signing, response classification table, backoff (30s × 3, cap 8h, 10 attempts, ±20% jitter, honour `Retry-After`).
5. **Reaper and dead letters**: lease expiry, dead-letter replay command.
6. **Partner protection**: per-host semaphore, circuit breaker that postpones without consuming attempts, SSRF guard on the dialer.
7. **Metrics**: the metrics listed in design.md section 5, exposed on `/metrics`.
8. **Fake producer and partner**, then **benchmarks**: the four scenarios in design.md section 6. The headline result is small-account p99 latency in shared-queue mode vs fair mode during a whale burst. `make bench` must print a readable table.
9. **README**: how to run, assumptions, benchmark results, known limitations, and what I would do next.

## Development workflow: TDD (mandatory)

No production code without a failing test first. For every behaviour, in this order:

1. **Red**: write one small test for one behaviour. Run it and watch it fail *for the expected reason* (feature missing, not a compile error or typo). A test that passes straight away tests nothing new: fix the test.
2. **Green**: write the minimum code that makes it pass. No extra options, no speculative features.
3. **Refactor**: clean up with the whole suite green. No new behaviour in this phase.
4. Repeat. Commit at green when a coherent slice is done.

Rules:

- Bug fixes start with a test that reproduces the bug.
- Before writing a test, name the production change that would make it fail. Concurrency and invariant tests (races, lease fencing, ring consistency) must also be shown to fail against a deliberately broken implementation (a quick mutation check) before they count.
- Test real behaviour through public APIs. Use real Postgres for anything touching SQL; never mock the database. Fakes only at process boundaries (HTTP partner, event source, clock).
- Inject time (`now func() time.Time`) and randomness (jitter) so unit tests are deterministic. Never `time.Sleep` to wait for an outcome in unit tests; integration tests may wait briefly on database time (lease expiry). Compare timestamps written by Postgres against the database clock (`now()` in SQL), never the host clock: the Docker VM clock can be hundreds of ms off.
- Exempt from test-first, but still verified: `Makefile`, `Dockerfile`, compose, Prometheus config (verified by `make up`), and the thin wiring in `cmd/*/main.go` (verified by a `make up` smoke check). SQL migrations are covered by the store integration tests.

## Verification before commit

- `make lint` (gofmt + `go vet`) and `make test` (unit + integration with `-race`) must pass, and the output must be read, not assumed. Report any failure by name.
- If a test is skipped (e.g. no `TEST_DATABASE_URL`), it did not pass: say so.
- Commit messages: imperative, describe the why. One step = one or more small commits, each green.

## Conventions

- Small, focused packages; interfaces only where there is a second implementation or a test double.
- Context-aware everything; graceful shutdown on SIGTERM (stop claiming, finish in-flight, release nothing that the lease will not cover).
- Errors wrapped with `%w`; structured logs with `delivery_id`, `account_id`, `webhook_id`, `attempt`.
- Tests: table-driven unit tests for scheduler fairness, backoff and response classification; integration tests (Postgres in Docker, one throwaway database per test via `internal/testpg`) for fan-out idempotency, claim fairness across concurrent replicas, retry and crash recovery. Test names describe the behaviour (`TestAckFencingRejectsStaleLease`), one behaviour per test.
- Store invariant: an account with pending deliveries is active in `account_queues` with `next_due_at` <= its earliest pending `next_attempt_at`. Any code that makes a row pending must keep it; tests assert it.
- Keep a short `NOTES.md` log of time spent per step; the reviewers ask for an estimate of total time.
