package bench

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/event"
	"webhook-notifier/internal/partner"
	"webhook-notifier/internal/partnerguard"
	"webhook-notifier/internal/reaper"
	"webhook-notifier/internal/scheduler"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/worker"
	"webhook-notifier/migrations"
)

// Timings are compressed so scenarios finish in seconds; the ratios
// between them follow the design (lease > HTTP timeout, reaper well under
// the lease, backoff growing towards a cap).
var timing = struct {
	lease, httpTimeout, reaperInterval, idle, visibility, breakerCooldown time.Duration
	backoff                                                               worker.Backoff
	maxAttempts, breakerThreshold                                         int
}{
	lease: 3 * time.Second, httpTimeout: time.Second, reaperInterval: 500 * time.Millisecond,
	idle: 5 * time.Millisecond, visibility: 5 * time.Second, breakerCooldown: 2 * time.Second,
	backoff:     worker.Backoff{Base: 500 * time.Millisecond, Factor: 2, Max: 5 * time.Second, Jitter: 0.2},
	maxAttempts: 30, breakerThreshold: 5,
}

const secret = "bench-secret"

// env is one scenario run's throwaway database.
type env struct {
	adminURL, name, url string
	pool                *pgxpool.Pool
	st                  *store.Store
	queue               *event.PGQueue
}

func newEnv(ctx context.Context, adminURL string) (*env, error) {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	e := &env{adminURL: adminURL, name: "bench_" + hex.EncodeToString(b)}
	if err := e.admin(ctx, "CREATE DATABASE "+e.name); err != nil {
		return nil, err
	}
	var err error
	if e.url, err = withDatabase(adminURL, e.name); err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(e.url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
	if e.pool, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		return nil, err
	}
	if err := migrations.Apply(ctx, e.pool); err != nil {
		e.close()
		return nil, err
	}
	e.st = store.New(e.pool)
	e.queue = event.NewPGQueue(e.pool, timing.visibility, 5)
	return e, nil
}

func (e *env) admin(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, e.adminURL)
	if err != nil {
		return fmt.Errorf("bench: connect admin: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("bench: %s: %w", sql, err)
	}
	return nil
}

func (e *env) close() {
	if e.pool != nil {
		e.pool.Close()
	}
	_ = e.admin(context.Background(), "DROP DATABASE IF EXISTS "+e.name+" WITH (FORCE)")
}

func (e *env) dead(ctx context.Context) (int, error) {
	var n int
	err := e.pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE status = 'dead'`).Scan(&n)
	return n, err
}

// replica is one notifier process: dispatcher, claim loop with workers and
// reaper, sharing its own connection pool.
type replica struct {
	pool   *pgxpool.Pool
	cancel context.CancelFunc
	done   chan struct{}
}

type replicaConfig struct {
	mode    string // "fair" or "shared"
	workers int
	perHost int
}

func (e *env) startReplica(rc replicaConfig) (*replica, error) {
	cfg, err := pgxpool.ParseConfig(e.url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(rc.workers/2 + 6)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	st := store.New(pool)
	w := worker.New(st, worker.NewSender(worker.SenderOptions{
		Timeout: timing.httpTimeout, AllowHTTP: true, AllowPrivateNetworks: true,
	}), worker.Options{
		Backoff: timing.backoff, MaxAttempts: timing.maxAttempts,
		Limiter: partnerguard.NewLimiter(rc.perHost),
		Breaker: partnerguard.NewBreaker(timing.breakerThreshold, timing.breakerCooldown, nil),
	})
	var claimer scheduler.Claimer = st
	if rc.mode == "shared" {
		claimer = scheduler.SharedQueue(st)
	}
	loop := scheduler.New(claimer, func(ctx context.Context, d store.Delivery) { _, _ = w.Process(ctx, d) },
		scheduler.Options{Workers: rc.workers, AccountsPerTick: 50, Quantum: 10, Lease: timing.lease, Idle: timing.idle})
	disp := dispatcher.New(event.NewPGQueue(pool, timing.visibility, 5), st,
		dispatcher.NewWebhookCache(30*time.Second, nil, st.ActiveWebhooks), 500, nil)
	reap := reaper.New(st, timing.maxAttempts, 1000, timing.reaperInterval, nil)

	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{pool: pool, cancel: cancel, done: make(chan struct{})}
	var wg sync.WaitGroup
	wg.Go(func() { _ = disp.Run(ctx, timing.idle) })
	wg.Go(func() { _ = loop.Run(ctx) })
	wg.Go(func() { _ = reap.Run(ctx) })
	go func() { wg.Wait(); close(r.done) }()
	return r, nil
}

// stop shuts the replica down gracefully.
func (r *replica) stop() {
	r.cancel()
	<-r.done
	r.pool.Close()
}

// crash kills the replica as a process crash would look to the database:
// it stops at once and its connections go away, so in-flight requests may
// still reach the partner but are never acked. Their leases expire and
// another replica's reaper resends them.
func (r *replica) crash() {
	r.cancel()
	r.pool.Close()
	<-r.done
}

func startReplicas(e *env, n int, rc replicaConfig) ([]*replica, error) {
	rs := make([]*replica, 0, n)
	for range n {
		r, err := e.startReplica(rc)
		if err != nil {
			stopAll(rs)
			return nil, err
		}
		rs = append(rs, r)
	}
	return rs, nil
}

func stopAll(rs []*replica) {
	for _, r := range rs {
		select {
		case <-r.done: // crashed already
		default:
			r.stop()
		}
	}
}

// waitDelivered polls the partner until it has seen want distinct
// successful deliveries, and returns how long that took from start.
func waitDelivered(ctx context.Context, p *partner.Partner, want int, start time.Time, timeout time.Duration) (time.Duration, error) {
	deadline := time.Now().Add(timeout)
	for {
		if got := p.Stats().Deliveries; got >= want {
			return time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return time.Since(start), fmt.Errorf("bench: %d of %d deliveries after %v", p.Stats().Deliveries, want, timeout)
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// withDatabase returns adminURL pointing at database name instead.
func withDatabase(adminURL, name string) (string, error) {
	u, err := url.Parse(adminURL)
	if err != nil {
		return "", fmt.Errorf("bench: parse database url: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}
