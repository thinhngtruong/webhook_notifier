-- Stand-in for the platform's event queue (docs/design.md: the event source
-- is mocked). The producer process publishes here and the dispatcher
-- consumes with the same at-least-once contract as a managed queue:
-- received events are invisible until visible_at, then delivered again
-- unless acked (deleted).
CREATE TABLE events_queue (
    id            text        PRIMARY KEY,
    name          text        NOT NULL,
    account_id    text        NOT NULL,
    event_time    timestamptz NOT NULL,
    payload       jsonb       NOT NULL,
    enqueued_at   timestamptz NOT NULL DEFAULT now(),
    visible_at    timestamptz NOT NULL DEFAULT now(),
    receive_count integer     NOT NULL DEFAULT 0
);

CREATE INDEX events_queue_visible_idx ON events_queue (visible_at);

-- Input-queue age gauge (oldest unacked event).
CREATE INDEX events_queue_enqueued_idx ON events_queue (enqueued_at);
