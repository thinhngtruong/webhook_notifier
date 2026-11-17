// Package reaper returns deliveries whose lease expired (the worker crashed
// or hung) to pending, so they are delivered again.
package reaper

import (
	"context"
	"log/slog"
	"time"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/store"
)

// Reaper resets expired leases periodically.
type Reaper struct {
	st          *store.Store
	maxAttempts int
	batch       int
	interval    time.Duration
	metrics     *metrics.Metrics
}

// New returns a Reaper that resets up to batch leases per statement and
// moves deliveries that already used maxAttempts to dead.
// m may be nil.
func New(st *store.Store, maxAttempts, batch int, interval time.Duration, m *metrics.Metrics) *Reaper {
	return &Reaper{st: st, maxAttempts: maxAttempts, batch: batch, interval: interval, metrics: m}
}

// ReapOnce resets every currently expired lease, batch by batch, so a
// crash that orphaned many leases is recovered in one pass while each
// statement stays short.
func (r *Reaper) ReapOnce(ctx context.Context) (store.ReapResult, error) {
	var total store.ReapResult
	for {
		res, err := r.st.ResetExpiredLeases(ctx, r.maxAttempts, r.batch)
		total.Reset += res.Reset
		total.Dead += res.Dead
		if err != nil {
			return total, err
		}
		if res.Reset+res.Dead < int64(r.batch) {
			return total, nil
		}
	}
}

// Run reaps every interval until ctx is cancelled, then returns nil.
// Errors are logged and retried on the next tick.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		res, err := r.ReapOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.metrics.Error(ctx, "reaper")
			slog.Error("reap expired leases", "err", err)
		}
		if res.Reset > 0 || res.Dead > 0 {
			slog.Warn("reset expired leases", "reset", res.Reset, "dead", res.Dead)
		}
	}
}

// Retention applies the delivery retention policy periodically: drop
// daily partitions after keep, keeping dead rows (archived) for keepDead.
type Retention struct {
	st             *store.Store
	keep, keepDead time.Duration
	interval       time.Duration
	metrics        *metrics.Metrics
}

// NewRetention returns a Retention that runs every interval. m may be nil.
func NewRetention(st *store.Store, keep, keepDead, interval time.Duration, m *metrics.Metrics) *Retention {
	return &Retention{st: st, keep: keep, keepDead: keepDead, interval: interval, metrics: m}
}

// Run applies retention every interval until ctx is cancelled, then
// returns nil. Errors are logged and retried on the next tick.
func (r *Retention) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		res, err := r.st.ApplyRetention(ctx, r.keep, r.keepDead)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.metrics.Error(ctx, "retention")
			slog.Error("apply retention", "err", err)
		}
		if len(res.Dropped) > 0 || res.Purged > 0 {
			slog.Info("retention applied", "dropped", res.Dropped, "archived_dead", res.Archived, "purged_dead", res.Purged)
		}
		if len(res.Skipped) > 0 {
			// Work older than the retention window is stuck: worth a look.
			slog.Warn("old partitions kept: they still hold pending or in-flight deliveries", "partitions", res.Skipped)
		}
	}
}
