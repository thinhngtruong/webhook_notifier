-- W3C traceparent carried from the event to every delivery attempt, so a
-- delivery's spans join the event's trace (docs/design.md, Logs and tracing).
ALTER TABLE events_queue        ADD COLUMN trace_context text;
ALTER TABLE events_dead_letters ADD COLUMN trace_context text;
ALTER TABLE deliveries          ADD COLUMN trace_context text;
