// Package store is the PostgreSQL access layer: webhook lookup, fan-out
// insert, the fair claim, acks, the lease reaper and dead-letter replay.
//
// Invariant kept by every function here: an account with at least one
// pending delivery has an active account_queues row whose next_due_at is
// not later than its earliest pending next_attempt_at. Anything that turns a
// row into pending (fan-out, retry ack, reaper, replay) therefore activates
// the account in the same transaction, after touching deliveries. Claim locks
// the account row before recomputing it, so the two orders interleave safely
// (see docs/design.md, "Keeping the account ring consistent").
package store

import (
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Delivery states.
const (
	StatusPending   = "pending"
	StatusInFlight  = "in_flight"
	StatusSucceeded = "succeeded"
	StatusDead      = "dead"
)

// ErrStale is returned by acks whose lease is no longer valid: the reaper
// reset the delivery (and maybe another worker re-claimed it) or it was
// already acked. The caller must drop its result.
var ErrStale = errors.New("store: stale lease")

// Store wraps a connection pool.
type Store struct {
	pool *pgxpool.Pool

	mu         sync.Mutex
	partitions map[string]struct{} // days known to have a deliveries partition
}

// New returns a Store using pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, partitions: make(map[string]struct{})}
}

// Pool exposes the underlying pool (health checks, metrics).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Delivery is one (event, webhook) delivery row.
type Delivery struct {
	ID            UUID
	EventTime     time.Time
	AccountID     string
	WebhookID     UUID
	EventID       string
	EventName     string
	Payload       []byte
	Status        string
	Attempts      int
	NextAttemptAt time.Time
	LeaseUntil    *time.Time
	LastError     *string
	DeliveredAt   *time.Time

	// Target is the webhook to deliver to, filled by Claim; nil if the
	// webhook row no longer exists.
	Target *Target
}

// Target is where and how a claimed delivery is sent.
type Target struct {
	URL    string
	Secret string
	Active bool // false once the webhook is disabled
}

// Lease identifies a claimed delivery for acking. Attempt is the fencing
// token: acks only apply while the row is still in_flight with that attempt
// number.
type Lease struct {
	ID        UUID
	EventTime time.Time
	Attempt   int
}

// Lease returns the ack handle of a claimed delivery.
func (d Delivery) Lease() Lease {
	return Lease{ID: d.ID, EventTime: d.EventTime, Attempt: d.Attempts}
}

// seconds converts a duration to the float seconds make_interval expects.
func seconds(d time.Duration) float64 { return d.Seconds() }

// BacklogStats is the pipeline-wide backlog, for metrics.
type BacklogStats struct {
	Pending        int64
	ActiveAccounts int64
	OldestDueAge   time.Duration // 0 when nothing is due
	Dead           int64         // dead deliveries not replayed (the dead-letter queue)
}
