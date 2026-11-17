# Notes

Time spent per step (for the total estimate).

| Step | Time | Notes |
|------|------|-------|
| 1. Skeleton | 0.5h | Module, config, compose, Makefile. Local Go upgraded to 1.27 via Homebrew. |
| 2. Schema and store | 1.5h | Migrations, partitions, fan-out, fair claim, fenced acks, reaper, replay. 16 tests. |
| 3. Dispatcher | 0.75h | EventSource + in-memory queue, TTL webhook cache, fan-out batch, run loop. Test-first; behaviour already covered by earlier code was mutation-checked instead. |
| 4. Scheduler + worker | 1.5h | Backoff, classification, Retry-After, signing, sender, claim returns target, worker acks, claim loop, wiring. Test-first; concurrency tests mutation-checked. Lost ~10 min to a hanging test (httptest handler waiting on an unread request context). Shared-queue baseline mode deferred to step 8 (benchmarks). |
| 5. Reaper + dead letters | 0.75h | Batched reaper pass + periodic loop, reaper batch config, replay command (refuses an empty filter without --all), smoke check of crash recovery and replay on the compose stack. Fixed a flaky store test that compared a Postgres timestamp with the host clock (Docker VM clock runs ahead). |
| 6. Partner protection | 1h | Postpone (refunds the attempt), per-host limiter, circuit breaker, SSRF dial guard + HTTPS-only, worker integration, config. A mutation check found an untested ordering (breaker before limiter loses the half-open probe); added the test. Retry budget deferred. |
| 7. Metrics (OpenTelemetry) | 1.25h | Switched from client_golang to the OTel SDK + Prometheus exporter at the user's request. Instruments named per design.md, scrape-tested; backlog gauges from one query; worker/dispatcher/claim/reaper instrumented; tier via LARGE_ACCOUNTS; Prometheus profile verified scraping both replicas. Tracing moved to a later step. |
| 8. Producer, partner, benchmarks | 3h | Postgres event queue + dispatcher role, shared-queue baseline, fake partner/producer, in-process bench with 4 scenarios. Bugs found: bench replicas connected to the dev DB (pgxpool ConnString returns the original string); a test hang hidden by an over-generous timeout. |

## Process

- From step 3 on, development is test-first (see CLAUDE.md, "Development workflow: TDD").
- Steps 1–2 were written before that rule: the tests came after the code. To check the
  tests are not vacuous, the ring-consistency logic was deliberately broken (activation
  upsert never updating existing rows): the race, retry, reaper and replay tests all failed
  (291/300 deliveries stranded), then passed again once restored.
- `make up` uses `--force-recreate`: Compose v2.3.3 did not recreate containers after a rebuild.

## Benchmark results (make bench, default scale, 2026-11-17, laptop, compose Postgres)

| Scenario | Result |
|---|---|
| Whale burst: small-account first-attempt p99 | shared 31.3s, fair 3.76s (8x better, under the 5s target) |
| Whale burst: whale's last delivery | shared 34.3s, fair 51.7s (fair is slower for the whale, see findings) |
| Scaling, 1 / 2 / 4 replicas | 252 / 475 / 844 deliveries/s (1.9x, 3.3x; 1,200/s offered) |
| Reliability (5% errors, endpoint down 15s) | 5,997/5,997 delivered, 0 dead, healthy p99 111ms, down endpoint p99 21.3s |
| Crash (1 of 2 replicas killed) | 5,997/5,997 delivered, 0 lost, 9 duplicates |

Findings to address:

1. The fair scheduler is not work-conserving for a lone whale: the quantum (10 per pass) and the account row lock (one replica serves an account at a time) cap the whale well below the free worker slots. The design claims a whale uses all spare capacity; it does not yet.
2. In fair mode most of the small accounts' latency is ingest: their events wait behind the whale's 50k events in the FIFO input queue until the dispatcher fans those out (~2.5s here, ~50s at 1M events).
