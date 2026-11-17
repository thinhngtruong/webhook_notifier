-- Benchmark baseline only (shared-queue claim mode): pick the oldest due
-- deliveries across all accounts without sorting the whole backlog, so the
-- baseline is not slowed down by a missing index.
CREATE INDEX deliveries_pending_due_idx ON deliveries (next_attempt_at) WHERE status = 'pending';
