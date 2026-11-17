package event

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGQueue is a Source backed by the events_queue table, shared by the
// producer and notifier processes. It stands in for the platform's queue.
type PGQueue struct {
	pool        *pgxpool.Pool
	visibility  time.Duration
	maxReceives int
}

// NewPGQueue returns a queue whose received events become visible again
// after visibility unless acked. An event received maxReceives times
// without an ack moves to the dead-letter queue instead of being received
// again.
func NewPGQueue(pool *pgxpool.Pool, visibility time.Duration, maxReceives int) *PGQueue {
	return &PGQueue{pool: pool, visibility: visibility, maxReceives: maxReceives}
}

// Publish enqueues events. Publishing an ID that is already queued is a
// no-op, like a deduplicating queue.
func (q *PGQueue) Publish(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	var (
		ids      = make([]string, len(events))
		names    = make([]string, len(events))
		accounts = make([]string, len(events))
		times    = make([]time.Time, len(events))
		payloads = make([]string, len(events))
		traces   = make([]string, len(events))
	)
	for i, e := range events {
		ids[i], names[i], accounts[i], times[i], payloads[i] = e.ID, e.Name, e.AccountID, e.Time, string(e.Payload)
		traces[i] = e.TraceContext
	}
	_, err := q.pool.Exec(ctx, `
		INSERT INTO events_queue (id, name, account_id, event_time, payload, trace_context)
		SELECT id, name, account_id, event_time, payload::jsonb, NULLIF(trace_context, '')
		  FROM unnest($1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::text[], $6::text[])
		       AS t(id, name, account_id, event_time, payload, trace_context)
		ON CONFLICT (id) DO NOTHING`, ids, names, accounts, times, payloads, traces)
	if err != nil {
		return fmt.Errorf("publish %d events: %w", len(events), err)
	}
	return nil
}

// Receive implements Source. Competing consumers skip each other's rows.
// Due events that already used maxReceives move to the dead-letter queue
// instead of being received again.
func (q *PGQueue) Receive(ctx context.Context, max int) ([]Event, error) {
	if _, err := q.pool.Exec(ctx, `
		WITH dead AS (
			DELETE FROM events_queue
			 WHERE id IN (SELECT id FROM events_queue
			               WHERE visible_at <= now() AND receive_count >= $1
			               ORDER BY visible_at
			               LIMIT 100
			               FOR UPDATE SKIP LOCKED)
			RETURNING id, name, account_id, event_time, payload, receive_count, trace_context
		)
		INSERT INTO events_dead_letters (id, name, account_id, event_time, payload, receive_count, reason, trace_context)
		SELECT id, name, account_id, event_time, payload, receive_count,
		       'received ' || receive_count || ' times without an ack', trace_context
		  FROM dead
		ON CONFLICT (id) DO NOTHING`, q.maxReceives); err != nil {
		return nil, fmt.Errorf("dead-letter exhausted events: %w", err)
	}

	rows, err := q.pool.Query(ctx, `
		UPDATE events_queue e
		   SET visible_at = now() + make_interval(secs => $2), receive_count = receive_count + 1
		  FROM (SELECT id FROM events_queue
		         WHERE visible_at <= now() AND receive_count < $3
		         ORDER BY visible_at
		         LIMIT $1
		         FOR UPDATE SKIP LOCKED) due
		 WHERE e.id = due.id
		RETURNING e.id, e.name, e.account_id, e.event_time, e.payload, COALESCE(e.trace_context, '')`, max, q.visibility.Seconds(), q.maxReceives)
	if err != nil {
		return nil, fmt.Errorf("receive events: %w", err)
	}
	events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.Name, &e.AccountID, &e.Time, &e.Payload, &e.TraceContext)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("receive events: %w", err)
	}
	return events, nil
}

// Ack implements Source.
func (q *PGQueue) Ack(ctx context.Context, ids []string) error {
	if _, err := q.pool.Exec(ctx, `DELETE FROM events_queue WHERE id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("ack %d events: %w", len(ids), err)
	}
	return nil
}

// QueueStats describes the input backlog.
type QueueStats struct {
	Waiting   int64         // events not yet acked
	OldestAge time.Duration // since the oldest unacked event was enqueued
}

// Stats returns the input backlog, for metrics.
// The count is exact, so it grows with the input backlog; fine for a mock
// queue scraped every few seconds.
func (q *PGQueue) Stats(ctx context.Context) (QueueStats, error) {
	var s QueueStats
	var age float64
	err := q.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM events_queue),
		       COALESCE(EXTRACT(EPOCH FROM now() - (SELECT min(enqueued_at) FROM events_queue)), 0)::float8`,
	).Scan(&s.Waiting, &age)
	if err != nil {
		return QueueStats{}, fmt.Errorf("queue stats: %w", err)
	}
	s.OldestAge = time.Duration(age * float64(time.Second))
	return s, nil
}

// DeadLetter implements Source.
func (q *PGQueue) DeadLetter(ctx context.Context, id, reason string) error {
	_, err := q.pool.Exec(ctx, `
		WITH dead AS (
			DELETE FROM events_queue WHERE id = $1
			RETURNING id, name, account_id, event_time, payload, receive_count, trace_context
		)
		INSERT INTO events_dead_letters (id, name, account_id, event_time, payload, receive_count, reason, trace_context)
		SELECT id, name, account_id, event_time, payload, receive_count, $2, trace_context FROM dead
		ON CONFLICT (id) DO NOTHING`, id, reason)
	if err != nil {
		return fmt.Errorf("dead-letter event %s: %w", id, err)
	}
	return nil
}

// DeadLetterStats returns the dead-letter queue size and the age of its
// oldest event, for metrics.
func (q *PGQueue) DeadLetterStats(ctx context.Context) (QueueStats, error) {
	var s QueueStats
	var age float64
	err := q.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM events_dead_letters),
		       COALESCE(EXTRACT(EPOCH FROM now() - (SELECT min(dead_at) FROM events_dead_letters)), 0)::float8`,
	).Scan(&s.Waiting, &age)
	if err != nil {
		return QueueStats{}, fmt.Errorf("dead-letter stats: %w", err)
	}
	s.OldestAge = time.Duration(age * float64(time.Second))
	return s, nil
}
