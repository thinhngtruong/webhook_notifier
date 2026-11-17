// Package scheduler runs the claim loop: it leases deliveries with the fair
// claim, sized to the free worker slots, and hands them to a worker pool.
package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/store"
)

// Claimer leases due deliveries (store.Store in production).
type Claimer interface {
	Claim(ctx context.Context, p store.ClaimParams) ([]store.Delivery, error)
}

// Processor handles one leased delivery.
type Processor func(ctx context.Context, d store.Delivery)

// Options tunes the loop.
type Options struct {
	Workers         int
	AccountsPerTick int
	Quantum         int
	Lease           time.Duration
	Idle            time.Duration    // wait after a pass that claimed nothing
	Metrics         *metrics.Metrics // nil: not recorded
}

// Loop is the claim loop plus its worker pool.
type Loop struct {
	claimer  Claimer
	process  Processor
	opts     Options
	inFlight atomic.Int64
}

// New returns a Loop.
func New(c Claimer, process Processor, opts Options) *Loop {
	return &Loop{claimer: c, process: process, opts: opts}
}

// Run claims and processes deliveries until ctx is cancelled. Each pass
// claims only as many deliveries as there are free worker slots, so a
// replica never leases work it cannot start. On cancellation it stops
// claiming and waits for in-flight deliveries; those run on a context that
// shutdown does not cancel, bounded by the HTTP timeout (shorter than the
// lease), so their results are still recorded.
func (l *Loop) Run(ctx context.Context) error {
	slots := make(chan struct{}, l.opts.Workers)
	work := context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		// Block until at least one slot is free.
		select {
		case <-ctx.Done():
			return nil
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-slots
			return nil
		}
		// This goroutine is the only one taking slots, so the rest of the
		// free ones can be reserved without blocking.
		free := 1 + cap(slots) - len(slots)
		for range free - 1 {
			slots <- struct{}{}
		}

		ds, err := l.claimer.Claim(ctx, store.ClaimParams{
			Accounts: l.opts.AccountsPerTick, Quantum: l.opts.Quantum, Limit: free, Lease: l.opts.Lease,
		})
		for range free - len(ds) {
			<-slots
		}
		if ctx.Err() != nil && len(ds) == 0 {
			return nil
		}
		if err != nil {
			l.opts.Metrics.Error(ctx, "claim")
			slog.Error("claim failed", "err", err)
		}

		for _, d := range ds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				l.inFlight.Add(1)
				defer l.inFlight.Add(-1)
				l.process(work, d)
			}()
		}

		if len(ds) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(l.opts.Idle):
			}
		}
	}
}

// InFlight returns how many deliveries are being processed right now.
func (l *Loop) InFlight() int64 { return l.inFlight.Load() }

// SharedQueue adapts the store's shared-queue claim (benchmark baseline)
// to the Claimer interface. It honours the slot limit and lease and
// ignores the fairness parameters.
func SharedQueue(st *store.Store) Claimer { return sharedQueue{st} }

type sharedQueue struct{ st *store.Store }

func (q sharedQueue) Claim(ctx context.Context, p store.ClaimParams) ([]store.Delivery, error) {
	return q.st.ClaimShared(ctx, p.Limit, p.Lease)
}
