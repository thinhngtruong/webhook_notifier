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
     FROM deliveries WHERE status = 'dead' AND event_time > now() - interval '1 day'
    GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20;
   ```
3. Once the cause is fixed, replay. Replays go through the fair scheduler, and each replayed delivery gets a fresh attempt budget:
   ```sh
   make replay ARGS="--account=account-123 --from=2026-11-17T00:00:00Z"
   make replay ARGS="--webhook=<webhook id>"
   ```
   An empty filter is refused unless `--all` is passed. Prefer narrow filters so a replay does not flood a partner.
