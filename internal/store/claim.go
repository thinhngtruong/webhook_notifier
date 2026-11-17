package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ClaimParams tunes one claim pass.
type ClaimParams struct {
	Accounts int           // K: accounts taken from the front of the ring
	Quantum  int           // Q: max deliveries per account
	Limit    int           // free worker slots; total deliveries returned
	Lease    time.Duration // lease length
}

// Claim runs one pass of the fair round-robin scheduler (docs/design.md
// section 3) in a single short transaction:
//
//  1. Lock up to min(K, Limit) due accounts from the front of the ring
//     (oldest last_served_at), skipping accounts another replica holds.
//  2. Take up to Q due pending deliveries from each, skipping locked rows,
//     and keep at most Limit of them, interleaved so every account gets its
//     first row before any gets its second. Mark them in_flight, increment
//     attempts and set the lease.
//  3. Move the accounts that got rows to the back of the ring, and recompute
//     next_due_at and active for every picked account from what is still
//     pending. Accounts with nothing pending leave the ring.
//
// It returns the leased deliveries; an empty slice means nothing was due.
func (s *Store) Claim(ctx context.Context, p ClaimParams) ([]Delivery, error) {
	if p.Accounts <= 0 || p.Quantum <= 0 || p.Limit <= 0 {
		return nil, nil
	}
	accounts := min(p.Accounts, p.Limit)

	var claimed []Delivery
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		claimed = nil

		rows, err := tx.Query(ctx, `
			SELECT account_id FROM account_queues
			 WHERE active AND next_due_at <= now()
			 ORDER BY last_served_at, account_id
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED`, accounts)
		if err != nil {
			return fmt.Errorf("pick accounts: %w", err)
		}
		picked, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("pick accounts: %w", err)
		}
		if len(picked) == 0 {
			return nil
		}

		// rn = position within the account, ord = position in the ring.
		// Ordering by (rn, ord) before LIMIT interleaves accounts when
		// there are fewer free slots than K × Q.
		rows, err = tx.Query(ctx, `
			WITH picked AS (
				SELECT account_id, ord FROM unnest($1::text[]) WITH ORDINALITY AS p(account_id, ord)
			), candidates AS (
				SELECT c.id, c.event_time, p.ord,
				       row_number() OVER (PARTITION BY p.account_id ORDER BY c.next_attempt_at) AS rn
				  FROM picked p
				 CROSS JOIN LATERAL (
					SELECT id, event_time, next_attempt_at
					  FROM deliveries
					 WHERE account_id = p.account_id
					   AND status = 'pending'
					   AND next_attempt_at <= now()
					 ORDER BY next_attempt_at
					 LIMIT $2
					 FOR UPDATE SKIP LOCKED
				 ) c
			), chosen AS (
				SELECT id, event_time FROM candidates ORDER BY rn, ord LIMIT $3
			)
			UPDATE deliveries d
			   SET status = 'in_flight',
			       attempts = d.attempts + 1,
			       lease_until = now() + make_interval(secs => $4)
			  FROM chosen c
			 WHERE d.id = c.id AND d.event_time = c.event_time
			`+claimReturning,
			picked, p.Quantum, p.Limit, seconds(p.Lease))
		if err != nil {
			return fmt.Errorf("lease deliveries: %w", err)
		}
		claimed, err = pgx.CollectRows(rows, scanClaimed)
		if err != nil {
			return fmt.Errorf("lease deliveries: %w", err)
		}

		served := make([]string, 0, len(picked))
		seen := make(map[string]bool, len(picked))
		for _, d := range claimed {
			if !seen[d.AccountID] {
				seen[d.AccountID] = true
				served = append(served, d.AccountID)
			}
		}

		// Accounts that got nothing (the slot limit ran out first) keep
		// their place at the front of the ring.
		_, err = tx.Exec(ctx, `
			UPDATE account_queues q
			   SET last_served_at = CASE WHEN q.account_id = ANY($2::text[]) THEN now() ELSE q.last_served_at END,
			       next_due_at    = COALESCE(n.next_due, q.next_due_at),
			       active         = n.next_due IS NOT NULL
			  FROM (
				SELECT a.account_id,
				       (SELECT min(next_attempt_at) FROM deliveries d
				         WHERE d.account_id = a.account_id AND d.status = 'pending') AS next_due
				  FROM unnest($1::text[]) AS a(account_id)
			  ) n
			 WHERE q.account_id = n.account_id`, picked, served)
		if err != nil {
			return fmt.Errorf("update account ring: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return claimed, nil
}

// scanClaimed reads a delivery followed by its webhook URL, secret and
// status (all NULL when the webhook row is gone).
func scanClaimed(r pgx.CollectableRow) (Delivery, error) {
	var d Delivery
	var url, secret, status *string
	err := r.Scan(&d.ID, &d.EventTime, &d.AccountID, &d.WebhookID, &d.EventID, &d.EventName,
		&d.Payload, &d.Status, &d.Attempts, &d.NextAttemptAt, &d.LeaseUntil,
		&d.LastError, &d.DeliveredAt, &url, &secret, &status)
	if err == nil && url != nil {
		d.Target = &Target{URL: *url, Secret: *secret, Active: *status == "active"}
	}
	return d, err
}

func scanDelivery(r pgx.CollectableRow) (Delivery, error) {
	var d Delivery
	err := r.Scan(&d.ID, &d.EventTime, &d.AccountID, &d.WebhookID, &d.EventID, &d.EventName,
		&d.Payload, &d.Status, &d.Attempts, &d.NextAttemptAt, &d.LeaseUntil,
		&d.LastError, &d.DeliveredAt)
	return d, err
}

const deliveryColumns = `id, event_time, account_id, webhook_id, event_id, event_name,
	payload, status, attempts, next_attempt_at, lease_until, last_error, delivered_at`

// GetDelivery reads one delivery. Used by tests and admin tooling.
func (s *Store) GetDelivery(ctx context.Context, id UUID, eventTime time.Time) (Delivery, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = $1 AND event_time = $2`, id, eventTime)
	if err != nil {
		return Delivery{}, fmt.Errorf("get delivery %s: %w", id, err)
	}
	d, err := pgx.CollectExactlyOneRow(rows, scanDelivery)
	if err != nil {
		return Delivery{}, fmt.Errorf("get delivery %s: %w", id, err)
	}
	return d, nil
}

// Backlog returns pending, active-account and oldest-due-age figures in
// one query.
//
// The counts use the partial pending index and the ring index, so their
// cost tracks the backlog, not the table. Called once per metrics scrape.
func (s *Store) Backlog(ctx context.Context) (BacklogStats, error) {
	var b BacklogStats
	var ageSeconds float64
	err := s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM deliveries WHERE status = 'pending'),
		       (SELECT count(*) FROM account_queues WHERE active),
		       COALESCE(EXTRACT(EPOCH FROM now() - (
		           SELECT min(next_attempt_at) FROM deliveries
		            WHERE status = 'pending' AND next_attempt_at <= now())), 0)::float8`,
	).Scan(&b.Pending, &b.ActiveAccounts, &ageSeconds)
	if err != nil {
		return BacklogStats{}, fmt.Errorf("backlog: %w", err)
	}
	b.OldestDueAge = time.Duration(ageSeconds * float64(time.Second))
	return b, nil
}

// ClaimShared is the benchmark baseline: one shared queue for all accounts.
// It leases up to limit due deliveries, oldest first, with no per-account
// fairness. Not used in production.
func (s *Store) ClaimShared(ctx context.Context, limit int, lease time.Duration) ([]Delivery, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE deliveries d
		   SET status = 'in_flight', attempts = d.attempts + 1,
		       lease_until = now() + make_interval(secs => $2)
		  FROM (SELECT id, event_time FROM deliveries
		         WHERE status = 'pending' AND next_attempt_at <= now()
		         ORDER BY next_attempt_at
		         LIMIT $1
		         FOR UPDATE SKIP LOCKED) c
		 WHERE d.id = c.id AND d.event_time = c.event_time
		`+claimReturning, limit, seconds(lease))
	if err != nil {
		return nil, fmt.Errorf("claim shared: %w", err)
	}
	ds, err := pgx.CollectRows(rows, scanClaimed)
	if err != nil {
		return nil, fmt.Errorf("claim shared: %w", err)
	}
	return ds, nil
}

// claimReturning lists the columns scanClaimed reads: the delivery and its
// webhook's URL, secret and status (NULL when the webhook row is gone).
const claimReturning = `RETURNING d.id, d.event_time, d.account_id, d.webhook_id, d.event_id, d.event_name,
		          d.payload, d.status, d.attempts, d.next_attempt_at, d.lease_until,
		          d.last_error, d.delivered_at,
		          (SELECT w.post_url FROM webhooks w WHERE w.id = d.webhook_id),
		          (SELECT w.secret   FROM webhooks w WHERE w.id = d.webhook_id),
		          (SELECT w.status   FROM webhooks w WHERE w.id = d.webhook_id)`
