-- Input dead-letter queue: events the dispatcher could not fan out, either
-- malformed (rejected on sight) or received too many times without an ack
-- (failing repeatedly). Any row here is an event that produced no
-- deliveries, so it counts against the durability SLO.
CREATE TABLE events_dead_letters (
    id            text        PRIMARY KEY,
    name          text        NOT NULL,
    account_id    text        NOT NULL,
    event_time    timestamptz NOT NULL,
    payload       jsonb       NOT NULL,
    receive_count integer     NOT NULL,
    reason        text        NOT NULL,
    dead_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX events_dead_letters_dead_at_idx ON events_dead_letters (dead_at);
