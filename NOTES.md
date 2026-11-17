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

## Process

- From step 3 on, development is test-first (see CLAUDE.md, "Development workflow: TDD").
- Steps 1–2 were written before that rule: the tests came after the code. To check the
  tests are not vacuous, the ring-consistency logic was deliberately broken (activation
  upsert never updating existing rows): the race, retry, reaper and replay tests all failed
  (291/300 deliveries stranded), then passed again once restored.
- `make up` uses `--force-recreate`: Compose v2.3.3 did not recreate containers after a rebuild.
