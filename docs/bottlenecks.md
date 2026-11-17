# Webhook Notifier: Bottleneck Discussion

Author: Thinh Nguyen

Companion to [design.md](design.md).

## Bottlenecks and how to address them

1.  PostgreSQL write throughput. A single primary is the ceiling, at about 3 writes per delivery. To push it further: batch inserts, claims and acks; set fillfactor below 100 so updates stay HOT; drop old daily partitions instead of deleting rows, which avoids vacuum debt; and consider async commit for acks. The next step after that is sharding by hash(account_id) across several databases. Fairness then holds within each shard, which is good enough.

2.  The claim query when many accounts are active. Picking by last_served_at only scans the index of active accounts. With 50k active accounts and LIMIT 50 this stays cheap. If it ever becomes a problem, the active account ring can move to an in-memory store while Postgres keeps the durable state.

3.  Hot rows for whales. The account_queues row is updated once per round, not once per delivery, and inserts only touch it when the account was inactive.

4.  Slow partners holding capacity. Per-endpoint limits and circuit breakers keep other accounts unaffected. The remaining case is a whale whose only endpoint is slow. That whale drains slowly, which is acceptable and visible in the metrics.

5.  Retry storms after an outage. Jitter, the circuit breaker's probe requests and Retry-After spread the load. Retried deliveries also go through the fair scheduler, so they cannot crowd out new traffic.

6.  Fan-out amplification. An account with many webhooks multiplies the writes. The API should cap webhooks per account, for example at 20.

7.  Dispatcher lag. Add Dispatcher replicas pulling from the same event queue; competing consumers scale without partitioning. The Dispatcher only does a cheap insert, so the database usually becomes the limit first.

8.  Duplicates. These come with at-least-once delivery: a worker can crash after the POST but before the ack. The lease length bounds how often it happens, and partners are told to dedupe on X-Webhook-Id.

9.  Going to 10x scale. The input event queue already acts as an ingest buffer. Beyond that: fair queues held in memory and sharded by account hash (or a managed cloud queue with per-tenant fair queues, see Fair scheduling in the design doc), with Postgres only for durable delivery state and dead letters.
