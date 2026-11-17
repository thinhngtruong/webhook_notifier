-- Initial schema. See docs/design.md section 1 (Data model).

-- Owned by the existing webhook API; the notifier only reads it (and
-- disables a webhook on 410 Gone). The PoC seeds it directly.
CREATE TABLE webhooks (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id text        NOT NULL,
    post_url   text        NOT NULL,
    events     text[]      NOT NULL,
    secret     text        NOT NULL,
    status     text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Dispatcher webhook lookup.
CREATE INDEX webhooks_account_active_idx ON webhooks (account_id) WHERE status = 'active';

-- One row per (event, webhook). id is a UUIDv5 of (event_id, webhook_id),
-- so a redelivered event hits the primary key and inserts nothing.
-- Partitioned by day on the event's own time, which keeps a redelivered
-- event in the same partition so the key still deduplicates. Daily
-- partitions are created on demand by the store (EnsurePartitions).
CREATE TABLE deliveries (
    id              uuid        NOT NULL,
    event_time      timestamptz NOT NULL,
    account_id      text        NOT NULL,
    webhook_id      uuid        NOT NULL,
    event_id        text        NOT NULL,
    event_name      text        NOT NULL,
    payload         jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'in_flight', 'succeeded', 'dead')),
    -- Incremented on every claim; doubles as the lease fencing token.
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until     timestamptz,
    last_error      text,
    delivered_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

-- Partial indexes only hold rows in the matching state, so they stay small
-- however large the table grows. Created on the parent, they propagate to
-- every partition.
CREATE INDEX deliveries_pending_idx  ON deliveries (account_id, next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_lease_idx    ON deliveries (lease_until)                 WHERE status = 'in_flight';
CREATE INDEX deliveries_dead_idx     ON deliveries (account_id, event_time)      WHERE status = 'dead';

-- Fairness state: the ring of accounts with pending work, ordered by when
-- they were last served. New accounts start at the front ('-infinity').
CREATE TABLE account_queues (
    account_id     text        PRIMARY KEY,
    active         boolean     NOT NULL DEFAULT true,
    last_served_at timestamptz NOT NULL DEFAULT '-infinity',
    next_due_at    timestamptz NOT NULL DEFAULT now()
);

-- Scheduler account pick.
CREATE INDEX account_queues_ring_idx ON account_queues (last_served_at) WHERE active;
