# webhook-notifier

Proof of concept for a webhook delivery service. It consumes subscriber events, fans each one out to the webhooks registered on the account, and POSTs a signed payload to each webhook's `post_url`: at least once, with retries, and fairly across accounts, so a burst from one huge account does not delay everyone else.

- Design: [docs/design.md](docs/design.md). Scaling limits: [docs/bottlenecks.md](docs/bottlenecks.md). Operations: [docs/runbook.md](docs/runbook.md).
- Stack: Go 1.27 and PostgreSQL 16. Dependencies: `pgx/v5`, OpenTelemetry (metrics, exported in Prometheus format), and the standard library.

## Run it

Needs Docker. Go 1.27 is only needed for tests and benchmarks on the host.

```sh
make up          # postgres, 2 notifier replicas, fake producer (20 events/s), fake partner
make up-metrics  # the same plus Prometheus on http://localhost:9091 (alert rules loaded)
make down        # stop and delete the data volume
```

| What | Where |
|---|---|
| Notifier replicas: `/healthz`, `/metrics` | http://localhost:8081, http://localhost:8082 |
| Fake partner: what it received (latency by tier, duplicates, bad signatures) | http://localhost:9090/stats |
| Fail a partner endpoint for a while | `curl -X POST 'localhost:9090/admin/outage?endpoint=account-00001&for=2m'` |
| Prometheus (with `make up-metrics`) | http://localhost:9091 |
| Postgres | `localhost:55432`, user/password/db `notifier` |

Other commands:

```sh
make test        # unit + integration tests (real Postgres in Docker, -race), alert rule tests
make bench       # the four benchmark scenarios, about 4 minutes; BENCH_SCALE=quick|default|full
make replay ARGS="--account=account-123"   # replay dead deliveries (see docs/runbook.md)
make lint        # gofmt + go vet
```

Each notifier process runs any mix of roles: `notifier --dispatcher --worker --reaper` (compose runs all three on both replicas). Configuration is by environment variable or flag; `notifier -h` lists them with defaults from the design.

## How it works

```
producer → events_queue → Dispatcher → deliveries (+ account ring) → fair claim → workers → partner
                          (fan-out, 1 tx)                          (lease)     (sign, POST, ack)
                                                    Lease Reaper ↺ (expired leases back to pending)
```

1. The **dispatcher** reads events in batches and, in one transaction, inserts one `deliveries` row per subscribed webhook (ID = UUIDv5 of event + webhook, so a redelivered event inserts nothing) and puts the account in the fair ring. Events are acked only after the commit.
2. Each replica's **claim loop** takes accounts from the front of the ring (least recently served first), takes up to a quantum of due deliveries from each, leases them for 30s and moves those accounts to the back. `FOR UPDATE SKIP LOCKED` lets replicas claim in parallel with no leader. A replica only claims as many deliveries as it has free worker slots.
3. **Workers** POST with `X-Webhook-Id`, `X-Webhook-Timestamp` and `X-Webhook-Signature` (HMAC-SHA256 of `timestamp.body`), then ack: succeeded; retry with backoff (30s ×3, cap 8h, ±20% jitter, 10 attempts, `Retry-After` honoured); or dead (other 4xx, 3xx, 410 also disables the webhook). Acks are fenced on the attempt number, so a worker whose lease expired cannot overwrite a newer attempt.
4. The **reaper** returns expired leases (crashed or hung workers) to pending every 10s.
5. **Dead letters**: deliveries that fail permanently are kept 30 days and replayable (`make replay`, ticket alerts). Events the dispatcher cannot fan out (malformed, or received 5 times without an ack) go to an input dead-letter queue, which pages: their webhooks were never called. See [docs/runbook.md](docs/runbook.md).
6. **Partner protection**: a per-host concurrency limit and circuit breaker postpone deliveries without using up attempts, and the dialer refuses internal addresses (SSRF) and plain HTTP.

## Assumptions

From the brief: about 1,000 events/s at peak, accounts from thousands to millions of subscribers, at-least-once delivery, no ordering, the event source can be mocked. My own assumptions, listed in the design: about 1.5 matching webhooks per event, a whale burst of 1M events within minutes, partner latency p50 200ms / p99 2s with a 10s timeout, a retry window of about 24h over 10 attempts, HMAC-SHA256 signing, deliveries kept 7 days and dead ones 30.

In the PoC the event source is an `events_queue` table with a visibility timeout (same at-least-once contract as a managed queue), and the latency tier of an account comes from `LARGE_ACCOUNTS` because the notifier does not know list sizes.

## Benchmark results

`make bench` (default scale) on a laptop. Each scenario gets a throwaway database, the fake partner over real HTTP, and N in-process replicas (dispatcher, claim loop, workers, reaper, each with its own connection pool). Timings are compressed (lease 3s, backoff from 0.5s, 15s outage instead of 5 minutes); latency is measured by the partner from the event time to the request. Runs vary by about ±20%.

| Scenario | Result |
|---|---|
| **Whale burst** (50k whale events + 500 small accounts × 10, 2 replicas × 32 workers): small-account p99, event to first attempt | **shared queue 34.3s → fair scheduler 4.2s** (target < 5s) |
| Whale burst: whale's last delivery | shared 41.5s, fair 48.9s |
| **Horizontal scaling** (1,200 events/s offered, 16 workers per replica, partner 50ms) | 1 / 2 / 4 replicas: 239 / 443 / 846 deliveries/s |
| **Reliability** (5% partner errors, one endpoint down 15s) | 5,997 / 5,997 delivered, 0 dead; healthy endpoints' p99 244ms; the down endpoint's deliveries landed after it recovered |
| **Crash** (one of 2 replicas killed mid-run) | 5,997 / 5,997 delivered, **0 lost**, 3 duplicates (sent but not acked before the crash) |

The full sizes from the design (1M whale events, 10-minute runs) are `BENCH_SCALE=full` and take hours; I have not run them.

## Known limitations

- **Ingest is first-in-first-out.** During a whale burst, small accounts' events wait in the input queue until the whale's earlier events are fanned out. That is most of the 4.2s above, and roughly 50s at a 1M burst, which would break the 5s target. Fairness has to start at ingest (bottlenecks.md, item 7).
- **The whale is ~18% slower with the fair scheduler** than with a shared queue: only one replica at a time can claim an account, so a lone whale's claims serialise across replicas. The quantum was made work-conserving after the first benchmark showed a 50% gap.
- Concurrency limits and circuit breakers are **per replica**: with N replicas a partner host can see N × 8 requests at once.
- Not built yet: the retry budget, distributed tracing, SLO burn-rate alerts and an Alertmanager (only the dead-letter alerts exist), dropping old daily partitions (retention), a command to requeue input dead letters (the runbook has the SQL), and batched acks.
- A replayed delivery restarts at attempt 1 and is counted as a "first attempt" with its old event time, which skews the freshness metric.
- Benchmarks run replicas in one process with compressed timings; they show behaviour and ratios, not production capacity.

## What I would do next

1. Fair ingest: consume the event queue per account (or partitioned by account), so a burst cannot delay other accounts before fan-out.
2. Let several replicas serve one busy account (split a whale into sub-queues), closing the remaining whale gap.
3. Tracing (OpenTelemetry spans from event to HTTP attempt, with the trace context stored on the delivery row), then the SLO burn-rate alerts and Alertmanager routing from the design.
4. Partition retention (drop daily partitions after 7 days, keeping dead rows for 30), and batched acks to cut write load.
5. Run the full-size benchmark on production-like hardware and tune quantum, accounts per tick and pool sizes from the results.

## Time spent

About 13 hours in total; [NOTES.md](NOTES.md) has the time per step and the benchmark history.
