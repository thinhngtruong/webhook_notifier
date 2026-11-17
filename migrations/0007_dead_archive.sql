-- Retention: daily delivery partitions are dropped after 7 days, but dead
-- deliveries are kept 30 days for inspection and replay. Before a
-- partition is dropped its dead rows move here; rows older than 30 days
-- are purged. Columns mirror deliveries: a later ALTER on deliveries must
-- also alter this table.
CREATE TABLE deliveries_dead_archive (LIKE deliveries INCLUDING DEFAULTS);
ALTER TABLE deliveries_dead_archive ADD PRIMARY KEY (id, event_time);
CREATE INDEX deliveries_dead_archive_account_idx ON deliveries_dead_archive (account_id, event_time);
