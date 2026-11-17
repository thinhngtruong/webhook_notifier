# Notes

Time spent per step (for the total estimate).

| Step | Time | Notes |
|------|------|-------|
| 1. Skeleton | 0.5h | Module, config, compose, Makefile. Local Go upgraded to 1.27 via Homebrew. |
| 2. Schema and store | 1.5h | Migrations, partitions, fan-out, fair claim, fenced acks, reaper, replay. 16 tests. |

## Process

- From step 3 on, development is test-first (see CLAUDE.md, "Development workflow: TDD").
- Steps 1–2 were written before that rule: the tests came after the code. To check the
  tests are not vacuous, the ring-consistency logic was deliberately broken (activation
  upsert never updating existing rows): the race, retry, reaper and replay tests all failed
  (291/300 deliveries stranded), then passed again once restored.
- `make up` uses `--force-recreate`: Compose v2.3.3 did not recreate containers after a rebuild.
