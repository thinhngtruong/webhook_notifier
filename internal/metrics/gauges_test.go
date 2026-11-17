package metrics_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/metrics/metricstest"
)

func TestObservedGauges(t *testing.T) {
	m, scrape := metricstest.New(t, metrics.Options{})
	backlogCalls := 0
	err := m.Observe(metrics.Sources{
		Backlog: func(context.Context) (metrics.Backlog, error) {
			backlogCalls++
			return metrics.Backlog{Pending: 7, ActiveAccounts: 3, OldestDueAge: 12500 * time.Millisecond, Dead: 5}, nil
		},
		WorkerSlotsInUse: func() int64 { return 4 },
		DBPoolInUse:      func() int64 { return 2 },
		OpenCircuits:     func() []string { return []string{"a.example"} },
	})
	if err != nil {
		t.Fatal(err)
	}

	out := scrape()
	for _, want := range []string{
		"webhook_pending_deliveries 7",
		"webhook_dead_deliveries 5",
		"webhook_active_accounts 3",
		"webhook_oldest_due_pending_age_seconds 12.5",
		"webhook_worker_slots_in_use 4",
		"db_pool_in_use 2",
		`webhook_circuit_open{host="a.example"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	if backlogCalls != 1 {
		t.Errorf("backlog queried %d times for one scrape, want 1 (one query feeds three gauges)", backlogCalls)
	}
}

func TestInputQueueGauges(t *testing.T) {
	m, scrape := metricstest.New(t, metrics.Options{})
	if err := m.Observe(metrics.Sources{
		InputQueue: func(context.Context) (int64, time.Duration, error) { return 42, 3 * time.Second, nil },
	}); err != nil {
		t.Fatal(err)
	}
	out := scrape()
	for _, want := range []string{"webhook_input_events_waiting 42", "webhook_input_oldest_event_age_seconds 3"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
}
