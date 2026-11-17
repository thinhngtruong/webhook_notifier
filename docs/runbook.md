# Webhook Notifier: Runbook

Linked from alert annotations. Alerts are evaluated by Prometheus (`make up-metrics`, http://localhost:9091/alerts); the PoC has no Alertmanager, so nothing routes them.

## Dead letters

Alerts: `WebhookDeadLetterRatioHigh`, `WebhookDeadLetterQueueLarge`, `WebhookDeadLetterQueueGrowing` (all tickets, never pages).

A delivery is dead when the partner rejected it permanently (4xx, 3xx, 410), its URL is not allowed (internal address, plain HTTP), its webhook was disabled, or it failed on every attempt of the ~24h retry window. Dead deliveries are kept 30 days and can be replayed.

1. Find the cause: `sum by (cause) (rate(webhook_deliveries_total{outcome=~"dead|gone"}[1h]))`.
   - `http_4xx`, `http_3xx`, `http_410`: the partner rejects the payload or the URL moved. A partner-side fix; 410 already disabled the webhook.
   - `http_5xx`, `timeout`, `connection` (attempts exhausted): the endpoint was down longer than the retry window.
   - `blocked`: the post_url points at an internal address or uses plain HTTP.
   - `webhook_disabled`: deliveries for a webhook disabled after they were queued. Usually nothing to do.
2. Find who is affected (account IDs are not metric labels):
   ```sql
   SELECT account_id, webhook_id, count(*), min(event_time), max(event_time), max(last_error)
     FROM (SELECT * FROM deliveries WHERE status = 'dead'
           UNION ALL
           SELECT * FROM deliveries_dead_archive) dead   -- older than 7 days: archived by retention
    WHERE event_time > now() - interval '1 day'
    GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20;
   ```
3. Once the cause is fixed, replay (archived ones too, for up to 30 days). Replays go through the fair scheduler, and each replayed delivery gets a fresh attempt budget:
   ```sh
   make replay ARGS="--account=account-123 --from=2026-11-17T00:00:00Z"
   make replay ARGS="--webhook=<webhook id>"
   ```
   An empty filter is refused unless `--all` is passed. Prefer narrow filters so a replay does not flood a partner.

## Input dead letters

Alert: `WebhookInputDeadLetters` (page). Gauges: `webhook_input_dead_letters`, `webhook_input_dead_letter_oldest_age_seconds`.

These are events the dispatcher could not fan out, so none of their webhooks were called: a durability miss. An event lands here when it is malformed (missing account, event name or time, or a payload that is not a JSON object; reason `malformed: ...`) or when it was received `EVENT_MAX_RECEIVES` times (default 5) without an ack, meaning fan-out kept failing (reason `received N times without an ack`).

1. See what is there:
   ```sql
   SELECT reason, count(*), min(dead_at), max(dead_at)
     FROM events_dead_letters GROUP BY reason ORDER BY 2 DESC;
   ```
2. Malformed events: fix the producer side; the event itself usually cannot be repaired here. Failing repeatedly: check `webhook_errors_total{component=~"fanout|webhook_lookup"}` and the dispatcher logs for the database error.
3. Requeue once the cause is fixed. Requeued events get a fresh receive count; the deliveries primary key makes this safe if an event was partly fanned out before. An empty filter is refused unless `--all` is passed:
   ```sh
   make requeue-events ARGS="--reason-prefix=received"   # the ones that kept failing
   make requeue-events ARGS="--id=evt-1,evt-2"
   ```

## Freshness

Alerts: `WebhookFreshnessBudgetBurnFast` and `WebhookFreshnessBudgetBurnSlow` (pages), `WebhookFreshnessBudgetBurnTicket` (ticket). SLO: 99% of small-tier first attempts within 5s of the event. The error ratio per window is recorded as `webhook:freshness_error_ratio:rate5m` … `rate3d`.

1. Ours or theirs? First-attempt latency is time on our side: the event waiting to be fanned out, then the delivery waiting to be claimed. Partner response time is not in it.
   - `webhook_input_oldest_event_age_seconds` high: ingest is behind (dispatcher or database). A whale burst shows up here first (bottlenecks.md, item 7).
   - `webhook_oldest_due_pending_age_seconds` high with `webhook_worker_slots_in_use` at the limit: not enough workers; scale replicas.
   - Slots in use high and `webhook_http_duration_seconds` rising: slow partners are holding workers; check `webhook_endpoint_throttled_total` and `webhook_circuit_open`.
   - `webhook_errors_total{component="claim"}` rising: database problems in the claim loop.
2. A trace of a late delivery (`make up-metrics`, http://localhost:16686) splits the wait: `webhook.deliver` carries `webhook.queue_wait_ms`, and the `HTTP POST` child shows the partner's share.
3. During a whale burst, check that fairness is holding: `webhook_first_attempt_latency_seconds{tier="small"}` should stay flat while `{tier="large"}` grows.
