package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// activateSQL returns a statement that puts the accounts produced by source
// (columns: account_id, due) back in the fair ring, or moves their
// next_due_at earlier.
//
// It writes only when the account was inactive or when the new work is due
// before the account's current next_due_at, so an account that already has
// due work (a whale mid-burst) is not rewritten on every insert or ack.
// Accounts are upserted in account_id order so concurrent writers lock them
// in the same order.
//
// The upsert must run after the caller's change to deliveries in the same
// transaction: if Claim holds the account row, ON CONFLICT waits for it and
// then re-checks against Claim's committed result (possibly active = false),
// so the reactivation is not lost.
func activateSQL(source string) string {
	return `INSERT INTO account_queues AS q (account_id, active, next_due_at)
	SELECT account_id, true, min(due) FROM (` + source + `) src
	 GROUP BY account_id
	 ORDER BY account_id
	ON CONFLICT (account_id) DO UPDATE
	   SET active = true,
	       next_due_at = CASE WHEN q.active
	                          THEN LEAST(q.next_due_at, EXCLUDED.next_due_at)
	                          ELSE EXCLUDED.next_due_at END
	 WHERE NOT q.active OR q.next_due_at > EXCLUDED.next_due_at`
}

// NewDelivery is one (event, webhook) pair to insert. Its ID is derived
// from EventID and WebhookID.
type NewDelivery struct {
	EventID   string
	EventName string
	EventTime time.Time
	AccountID string
	WebhookID UUID
	Payload   []byte // JSON
}

// FanOut inserts the deliveries and activates their accounts in one
// transaction. Deliveries that already exist (a redelivered event) are
// skipped. It returns the number of rows actually inserted. The caller may
// ack the source events once FanOut returns nil.
func (s *Store) FanOut(ctx context.Context, ds []NewDelivery) (int64, error) {
	if len(ds) == 0 {
		return 0, nil
	}

	var (
		ids        = make([]UUID, len(ds))
		eventTimes = make([]time.Time, len(ds))
		accounts   = make([]string, len(ds))
		webhooks   = make([]UUID, len(ds))
		eventIDs   = make([]string, len(ds))
		eventNames = make([]string, len(ds))
		payloads   = make([]string, len(ds))
		minDay     = ds[0].EventTime
		maxDay     = ds[0].EventTime
	)
	for i, d := range ds {
		ids[i] = DeliveryID(d.EventID, d.WebhookID)
		eventTimes[i] = d.EventTime
		accounts[i] = d.AccountID
		webhooks[i] = d.WebhookID
		eventIDs[i] = d.EventID
		eventNames[i] = d.EventName
		payloads[i] = string(d.Payload)
		if d.EventTime.Before(minDay) {
			minDay = d.EventTime
		}
		if d.EventTime.After(maxDay) {
			maxDay = d.EventTime
		}
	}
	if err := s.EnsurePartitions(ctx, minDay, maxDay); err != nil {
		return 0, err
	}

	// One statement: the insert CTE runs first, the activation reads its
	// RETURNING rows, so only accounts that got new rows are touched.
	sql := `WITH ins AS (
		INSERT INTO deliveries (id, event_time, account_id, webhook_id, event_id, event_name, payload)
		SELECT id, event_time, account_id, webhook_id, event_id, event_name, payload::jsonb
		  FROM unnest($1::uuid[], $2::timestamptz[], $3::text[], $4::uuid[], $5::text[], $6::text[], $7::text[])
		       AS t(id, event_time, account_id, webhook_id, event_id, event_name, payload)
		ON CONFLICT (id, event_time) DO NOTHING
		RETURNING account_id, next_attempt_at
	), act AS (` + activateSQL(`SELECT account_id, next_attempt_at AS due FROM ins`) + `)
	SELECT count(*) FROM ins`

	var inserted int64
	err := s.pool.QueryRow(ctx, sql, ids, eventTimes, accounts, webhooks, eventIDs, eventNames, payloads).Scan(&inserted)
	if err != nil {
		return 0, fmt.Errorf("fan out %d deliveries: %w", len(ds), err)
	}
	return inserted, nil
}

// partitionLockKey serialises partition DDL across replicas; concurrent
// CREATE TABLE IF NOT EXISTS can otherwise fail on catalog unique keys.
const partitionLockKey = 7_201_640_012

// EnsurePartitions creates the daily deliveries partitions (UTC days)
// covering [from, to]. Days already created by this Store are skipped
// without a round trip.
// maxPartitionSpan bounds the days one call may cover. A batch spanning
// more is a bad event time (zero or far off), which would otherwise create
// thousands of partitions and wedge the dispatcher.
const maxPartitionSpan = 62 * 24 * time.Hour

func (s *Store) EnsurePartitions(ctx context.Context, from, to time.Time) error {
	day := from.UTC().Truncate(24 * time.Hour)
	last := to.UTC().Truncate(24 * time.Hour)
	if last.Sub(day) > maxPartitionSpan {
		return fmt.Errorf("event times span %s to %s, more than %v: refusing to create partitions",
			day.Format("2006-01-02"), last.Format("2006-01-02"), maxPartitionSpan)
	}
	for ; !day.After(last); day = day.Add(24 * time.Hour) {
		if err := s.ensurePartition(ctx, day); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensurePartition(ctx context.Context, day time.Time) error {
	key := day.Format("20060102")
	s.mu.Lock()
	_, known := s.partitions[key]
	s.mu.Unlock()
	if known {
		return nil
	}

	// Identifiers and bounds come from a formatted time, not user input.
	ddl := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS deliveries_p%s PARTITION OF deliveries
		 FOR VALUES FROM ('%s') TO ('%s')`,
		key, day.Format(time.RFC3339), day.Add(24*time.Hour).Format(time.RFC3339))
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", partitionLockKey); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, ddl)
		return err
	})
	if err != nil {
		return fmt.Errorf("create deliveries partition %s: %w", key, err)
	}

	s.mu.Lock()
	s.partitions[key] = struct{}{}
	s.mu.Unlock()
	return nil
}
