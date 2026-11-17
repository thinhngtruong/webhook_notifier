package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/metrics/metricstest"
	"webhook-notifier/internal/scheduler"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
)

func seed(t *testing.T, st *store.Store, accounts, each int) {
	t.Helper()
	ctx := context.Background()
	at := time.Now().UTC()
	for a := range accounts {
		account := fmt.Sprintf("account-%d", a)
		hook, err := st.InsertWebhook(ctx, store.Webhook{AccountID: account, PostURL: "https://partner.example", Events: []string{"subscriber.created"}, Secret: "s"})
		if err != nil {
			t.Fatal(err)
		}
		ds := make([]store.NewDelivery, each)
		for i := range ds {
			ds[i] = store.NewDelivery{EventID: fmt.Sprintf("%s-%d", account, i), EventName: "subscriber.created",
				EventTime: at, AccountID: account, WebhookID: hook, Payload: []byte(`{}`)}
		}
		if _, err := st.FanOut(ctx, ds); err != nil {
			t.Fatal(err)
		}
	}
}

var opts = scheduler.Options{Workers: 4, AccountsPerTick: 50, Quantum: 10, Lease: time.Minute, Idle: 5 * time.Millisecond}

func TestRunDeliversEverythingWithinWorkerLimit(t *testing.T) {
	st := store.New(testpg.New(t))
	const total = 200
	seed(t, st, 5, total/5)

	var (
		inFlight, maxInFlight atomic.Int64
		mu                    sync.Mutex
		seen                  = map[store.UUID]int{}
		done                  = make(chan struct{})
		processed             atomic.Int64
	)
	process := func(ctx context.Context, d store.Delivery) {
		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond) // simulated partner latency, so work overlaps
		if err := st.AckSucceeded(ctx, d.Lease()); err != nil {
			t.Error(err)
		}
		mu.Lock()
		seen[d.ID]++
		mu.Unlock()
		inFlight.Add(-1)
		if processed.Add(1) == total {
			close(done)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- scheduler.New(st, process, opts).Run(ctx) }()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("processed %d of %d", processed.Load(), total)
	}
	if m := maxInFlight.Load(); m > int64(opts.Workers) || m < 2 {
		t.Errorf("max concurrent = %d, want 2..%d", m, opts.Workers)
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("delivery %s processed %d times", id, n)
		}
	}
}

func TestRunStopsClaimingAndFinishesInFlightOnShutdown(t *testing.T) {
	st := store.New(testpg.New(t))
	seed(t, st, 1, 10)

	started := make(chan struct{}, 10)
	release := make(chan struct{})
	var claimedAfterCancel atomic.Bool
	var cancelled atomic.Bool
	var finishedWithLiveCtx atomic.Int64
	process := func(ctx context.Context, d store.Delivery) {
		if cancelled.Load() {
			claimedAfterCancel.Store(true)
		}
		started <- struct{}{}
		<-release
		if ctx.Err() == nil {
			finishedWithLiveCtx.Add(1)
		}
	}

	o := opts
	o.Workers = 2
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- scheduler.New(st, process, o).Run(ctx) }()

	for range 2 {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("workers never started")
		}
	}
	cancelled.Store(true)
	cancel()
	select {
	case err := <-result:
		t.Fatalf("Run returned (%v) while deliveries were still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after in-flight work finished")
	}
	if claimedAfterCancel.Load() {
		t.Error("claimed new work after shutdown began")
	}
	if finishedWithLiveCtx.Load() != 2 {
		t.Errorf("%d of 2 in-flight deliveries finished with a live context", finishedWithLiveCtx.Load())
	}
}

func TestInFlightReportsBusySlots(t *testing.T) {
	st := store.New(testpg.New(t))
	seed(t, st, 1, 3)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	l := scheduler.New(st, func(ctx context.Context, d store.Delivery) {
		started <- struct{}{}
		<-release
	}, opts)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	for range 3 {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("deliveries never started")
		}
	}
	if n := l.InFlight(); n != 3 {
		t.Errorf("InFlight = %d while 3 deliveries run, want 3", n)
	}
	close(release)
	cancel()
	<-done
	if n := l.InFlight(); n != 0 {
		t.Errorf("InFlight = %d after shutdown, want 0", n)
	}
}

type failingClaimer struct{ calls atomic.Int64 }

func (c *failingClaimer) Claim(context.Context, store.ClaimParams) ([]store.Delivery, error) {
	c.calls.Add(1)
	return nil, errors.New("db down")
}

func TestRunCountsClaimErrors(t *testing.T) {
	m, scrape := metricstest.New(t, metrics.Options{})
	c := &failingClaimer{}
	o := opts
	o.Metrics = m
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.New(c, func(context.Context, store.Delivery) {}, o).Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for c.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if out := scrape(); !strings.Contains(out, `webhook_errors_total{component="claim"}`) {
		t.Fatalf("claim errors not counted:\n%s", out)
	}
}
