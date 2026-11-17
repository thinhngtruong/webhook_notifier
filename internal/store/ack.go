package store

import (
	"context"
	"fmt"
	"time"
)

// leaseMatch is the fencing condition shared by every ack: the row is still
// leased by this attempt. A worker whose lease expired and was reset (or
// re-claimed) matches nothing and gets ErrStale.
const leaseMatch = `id = $1 AND event_time = $2 AND status = 'in_flight' AND attempts = $3`

// AckSucceeded records a 2xx response.
func (s *Store) AckSucceeded(ctx context.Context, l Lease) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		   SET status = 'succeeded', delivered_at = now(), lease_until = NULL, last_error = NULL
		 WHERE `+leaseMatch, l.ID, l.EventTime, l.Attempt)
	if err != nil {
		return fmt.Errorf("ack succeeded %s: %w", l.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStale
	}
	return nil
}

// AckDead records a permanent failure (non-retryable 4xx, or attempts
// exhausted). Dead deliveries can be replayed with ReplayDead.
func (s *Store) AckDead(ctx context.Context, l Lease, reason string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		   SET status = 'dead', lease_until = NULL, last_error = $4
		 WHERE `+leaseMatch, l.ID, l.EventTime, l.Attempt, reason)
	if err != nil {
		return fmt.Errorf("ack dead %s: %w", l.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStale
	}
	return nil
}

// AckRetry puts the delivery back to pending, due after delay, and makes
// sure its account is in the ring for that time. The claim may have taken
// the account out of the ring while this delivery was in flight.
func (s *Store) AckRetry(ctx context.Context, l Lease, delay time.Duration, reason string) error {
	if err := s.reschedule(ctx, l, delay, reason, false); err != nil {
		return fmt.Errorf("ack retry %s: %w", l.ID, err)
	}
	return nil
}

// ReapResult counts what ResetExpiredLeases did.
type ReapResult struct {
	Reset int64 // back to pending
	Dead  int64 // lease expired on the last allowed attempt
}

// ResetExpiredLeases returns up to limit deliveries whose lease expired
// (the worker crashed or hung) to pending, due now. A delivery that already
// used maxAttempts goes to dead instead. Their accounts are reactivated.
func (s *Store) ResetExpiredLeases(ctx context.Context, maxAttempts, limit int) (ReapResult, error) {
	var res ReapResult
	err := s.pool.QueryRow(ctx, `
			WITH expired AS (
				SELECT id, event_time FROM deliveries
				 WHERE status = 'in_flight' AND lease_until < now()
				 ORDER BY lease_until
				 LIMIT $1
				 FOR UPDATE SKIP LOCKED
			), upd AS (
				UPDATE deliveries d
				   SET status = CASE WHEN d.attempts >= $2 THEN 'dead' ELSE 'pending' END,
				       next_attempt_at = now(), lease_until = NULL, last_error = 'lease expired'
				  FROM expired e
				 WHERE d.id = e.id AND d.event_time = e.event_time
				RETURNING d.account_id, d.status, d.next_attempt_at
			), act AS (`+activateSQL(`SELECT account_id, next_attempt_at AS due FROM upd WHERE status = 'pending'`)+`)
			SELECT count(*) FILTER (WHERE status = 'pending'), count(*) FILTER (WHERE status = 'dead') FROM upd`,
		limit, maxAttempts).Scan(&res.Reset, &res.Dead)
	if err != nil {
		return ReapResult{}, fmt.Errorf("reset expired leases: %w", err)
	}
	return res, nil
}

// ReplayFilter selects dead deliveries to replay. Empty fields match all.
type ReplayFilter struct {
	AccountID string
	WebhookID *UUID
	From, To  time.Time // event_time range [From, To); zero means unbounded
}

// ReplayDead returns matching dead deliveries to pending with a fresh
// attempt budget, due now, and reactivates their accounts. It returns how
// many were replayed.
func (s *Store) ReplayDead(ctx context.Context, f ReplayFilter) (int64, error) {
	var account *string
	if f.AccountID != "" {
		account = &f.AccountID
	}
	var from, to *time.Time
	if !f.From.IsZero() {
		from = &f.From
	}
	if !f.To.IsZero() {
		to = &f.To
	}

	var n int64
	err := s.pool.QueryRow(ctx, `
			WITH upd AS (
				UPDATE deliveries
				   SET status = 'pending', attempts = 0, next_attempt_at = now(),
				       lease_until = NULL, last_error = NULL
				 WHERE status = 'dead'
				   AND ($1::text IS NULL OR account_id = $1)
				   AND ($2::uuid IS NULL OR webhook_id = $2)
				   AND ($3::timestamptz IS NULL OR event_time >= $3)
				   AND ($4::timestamptz IS NULL OR event_time < $4)
				RETURNING account_id, next_attempt_at
			), act AS (`+activateSQL(`SELECT account_id, next_attempt_at AS due FROM upd`)+`)
			SELECT count(*) FROM upd`,
		account, f.WebhookID, from, to).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("replay dead: %w", err)
	}
	return n, nil
}

// Postpone puts a leased delivery back to pending, due after delay,
// without using up an attempt: the request was never sent (circuit open or
// endpoint at its concurrency limit). The account is reactivated.
func (s *Store) Postpone(ctx context.Context, l Lease, delay time.Duration, reason string) error {
	if err := s.reschedule(ctx, l, delay, reason, true); err != nil {
		return fmt.Errorf("postpone %s: %w", l.ID, err)
	}
	return nil
}

// reschedule returns a leased delivery to pending, due after delay, and
// activates its account in the same statement. refund gives back the
// attempt the claim counted. It returns ErrStale if the lease is gone.
func (s *Store) reschedule(ctx context.Context, l Lease, delay time.Duration, reason string, refund bool) error {
	attempts := "attempts"
	if refund {
		attempts = "attempts - 1"
	}
	var updated int64
	err := s.pool.QueryRow(ctx, `
		WITH upd AS (
			UPDATE deliveries
			   SET status = 'pending', attempts = `+attempts+`, lease_until = NULL, last_error = $4,
			       next_attempt_at = now() + make_interval(secs => $5)
			 WHERE `+leaseMatch+`
			RETURNING account_id, next_attempt_at
		), act AS (`+activateSQL(`SELECT account_id, next_attempt_at AS due FROM upd`)+`)
		SELECT count(*) FROM upd`,
		l.ID, l.EventTime, l.Attempt, reason, seconds(delay)).Scan(&updated)
	if err != nil {
		return err
	}
	if updated == 0 {
		return ErrStale
	}
	return nil
}
