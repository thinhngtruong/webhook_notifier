-- Set when an admin replays a dead delivery, so its new first attempt is
-- not counted in first-attempt (freshness) latency with an old event time.
ALTER TABLE deliveries ADD COLUMN replayed_at timestamptz;
