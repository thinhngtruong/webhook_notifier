package reaper_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/metrics/metricstest"
	"webhook-notifier/internal/reaper"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
)

// seedLeased creates n deliveries for one account and claims them with the
// given lease. It returns them.
func seedLeased(t *testing.T, st *store.Store, account string, n int, lease time.Duration) []store.Delivery {
	t.Helper()
	ctx := context.Background()
	hook, err := st.InsertWebhook(ctx, store.Webhook{AccountID: account, PostURL: "https://partner.example", Events: []string{"subscriber.created"}, Secret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	ds := make([]store.NewDelivery, n)
	for i := range ds {
		ds[i] = store.NewDelivery{EventID: fmt.Sprintf("%s-%d", account, i), EventName: "subscriber.created",
			EventTime: at, AccountID: account, WebhookID: hook, Payload: []byte(`{}`)}
	}
	if _, err := st.FanOut(ctx, ds); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.Claim(ctx, store.ClaimParams{Accounts: 1, Quantum: n, Limit: n, Lease: lease})
	if err != nil || len(claimed) != n {
		t.Fatalf("claim = %d, %v", len(claimed), err)
	}
	return claimed
}

func TestReapOnceDrainsAllExpiredLeasesInBatches(t *testing.T) {
	st := store.New(testpg.New(t))
	ctx := context.Background()
	expired := seedLeased(t, st, "account-crashed", 5, 50*time.Millisecond)
	live := seedLeased(t, st, "account-busy", 2, time.Hour)
	time.Sleep(100 * time.Millisecond) // let the short leases expire (database time)

	res, err := reaper.New(st, 10, 2, time.Second, nil).ReapOnce(ctx)
	if err != nil || res.Reset != 5 || res.Dead != 0 {
		t.Fatalf("ReapOnce = %+v, %v; want 5 reset with batch size 2", res, err)
	}
	for _, d := range expired {
		if got, _ := st.GetDelivery(ctx, d.ID, d.EventTime); got.Status != store.StatusPending {
			t.Errorf("expired %s status = %s, want pending", d.EventID, got.Status)
		}
	}
	for _, d := range live {
		if got, _ := st.GetDelivery(ctx, d.ID, d.EventTime); got.Status != store.StatusInFlight {
			t.Errorf("live lease %s status = %s, want in_flight", d.EventID, got.Status)
		}
	}
}

func TestRunReapsPeriodicallyAndStopsOnCancel(t *testing.T) {
	st := store.New(testpg.New(t))
	d := seedLeased(t, st, "account-crashed", 1, 50*time.Millisecond)[0]

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reaper.New(st, 10, 100, 20*time.Millisecond, nil).Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := st.GetDelivery(context.Background(), d.ID, d.EventTime)
		if err == nil && got.Status == store.StatusPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired lease never reset: status %s", got.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestRunCountsReaperErrors(t *testing.T) {
	pool := testpg.New(t)
	st := store.New(pool)
	pool.Close() // every query now fails
	m, scrape := metricstest.New(t, metrics.Options{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reaper.New(st, 10, 100, 5*time.Millisecond, m).Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(scrape(), `webhook_errors_total{component="reaper"}`) {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("reaper errors not counted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestRetentionRunDropsOldPartitions(t *testing.T) {
	pool := testpg.New(t)
	st := store.New(pool)
	ctx := context.Background()
	old := time.Now().UTC().Add(-10 * 24 * time.Hour)
	d := seedLeasedAt(t, st, "account-old", old)
	st.AckSucceeded(ctx, d.Lease())

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- reaper.NewRetention(st, 7*24*time.Hour, 30*24*time.Hour, 20*time.Millisecond, nil).Run(runCtx)
	}()
	name := "deliveries_p" + old.Format("20060102")
	deadline := time.Now().Add(5 * time.Second)
	for {
		var exists bool
		pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists)
		if !exists {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("partition %s never dropped", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func seedLeasedAt(t *testing.T, st *store.Store, account string, at time.Time) store.Delivery {
	t.Helper()
	ctx := context.Background()
	hook, err := st.InsertWebhook(ctx, store.Webhook{AccountID: account, PostURL: "https://partner.example", Events: []string{"subscriber.created"}, Secret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.FanOut(ctx, []store.NewDelivery{{EventID: account + "-1", EventName: "subscriber.created", EventTime: at, AccountID: account, WebhookID: hook, Payload: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	ds, err := st.Claim(ctx, store.ClaimParams{Accounts: 1, Quantum: 1, Limit: 1, Lease: time.Minute})
	if err != nil || len(ds) != 1 {
		t.Fatalf("claim = %v, %v", ds, err)
	}
	return ds[0]
}
