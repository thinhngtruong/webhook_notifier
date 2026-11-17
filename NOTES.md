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
| 8. Producer, partner, benchmarks | 3.5h | Postgres event queue + dispatcher role, shared-queue baseline, fake partner/producer, in-process bench with 4 scenarios. Bugs found: bench replicas connected to the dev DB (pgxpool ConnString returns the original string); a test hang hidden by an over-generous timeout. |
| 8b. Dead-letter metric + alerts | 0.5h | webhook_dead_deliveries gauge, three ticket-level DLQ alerts unit-tested with promtool (mutation-checked), runbook. |
| 9b. Input dead-letter queue | 0.75h | Receive limit + explicit dead-lettering of malformed events, gauges, page alert (promtool-tested), runbook. Found and fixed: a zero event time made fan-out create a partition per day back to year 1 (62,160 partitions before the test was stopped). |
| 9. README | 0.5h | How to run, assumptions, benchmark results, limitations, next steps. |
| 10. Tracing | 1h | W3C trace context on queue and delivery rows; producer, fan-out, deliver and HTTP spans; parent-based ratio sampling; OTLP to a local trace viewer. Found: a package-level tracer stays bound to the first provider installed. |
| 11. Operability gaps | 1.25h | replayed_at (replays out of the freshness metric), requeue-events command, partition retention with a 30-day dead archive and replay from it. |
| **Total** | **~15h** | |

## Process

- From step 3 on, development is test-first (see CLAUDE.md, "Development workflow: TDD").
- Steps 1–2 were written before that rule: the tests came after the code. To check the
  tests are not vacuous, the ring-consistency logic was deliberately broken (activation
  upsert never updating existing rows): the race, retry, reaper and replay tests all failed
  (291/300 deliveries stranded), then passed again once restored.
- `make up` uses `--force-recreate`: Compose v2.3.3 did not recreate containers after a rebuild.

## Benchmark results (make bench, default scale, laptop, compose Postgres)

Two runs on 2026-11-17/18: before and after making the quantum work-conserving. Runs vary by roughly ±20% on this laptop (the shared-queue whale time alone moved from 34s to 41s between runs).

| Scenario | Run 1 (strict quantum) | Run 2 (work-conserving) |
|---|---|---|
| Whale burst: small-account first-attempt p99, shared → fair | 31.3s → 3.76s | 34.3s → 4.25s (under the 5s target) |
| Whale burst: whale's last delivery, shared / fair | 34.3s / 51.7s | 41.5s / 48.9s |
| Scaling, 1 / 2 / 4 replicas (1,200/s offered) | 252 / 475 / 844 /s | 239 / 443 / 846 /s |
| Reliability (5% errors, endpoint down 15s) | 5,997/5,997, 0 dead, healthy p99 111ms | 5,997/5,997, 0 dead, healthy p99 244ms |
| Crash (1 of 2 replicas killed) | 0 lost, 9 duplicates | 0 lost, 3 duplicates |

Findings:

1. Fixed (partly): the fair scheduler was not work-conserving for a lone whale (strict quantum of 10 per pass). The quantum is now a floor when every due account fits in a pass. The whale is still ~18% slower than with a shared queue: only one replica can hold the whale's account row at a time, so the other replica's pass comes back empty and idles. Next step would be letting a busy account be served by several replicas (e.g. splitting a whale into sub-queues) if this matters.
2. Documented, not fixed: in fair mode most small-account latency is ingest. Their events wait behind the whale's 50k events in the FIFO input queue until fanned out (~2.5s here, ~50s at 1M). See bottlenecks.md item 7.
