package dispatcher_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/event"
	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/metrics/metricstest"
)

func TestProcessBatchCountsConsumedEventsAndFanOutErrors(t *testing.T) {
	ctx := context.Background()
	m, scrape := metricstest.New(t, metrics.Options{})
	q := event.NewMemQueue(0, nil)
	cache := dispatcher.NewWebhookCache(time.Minute, nil, oneHook)

	q.Publish(evt("e1", "subscriber.created", "account-1"))
	if _, err := dispatcher.New(q, failingStore{err: errors.New("db down")}, cache, 10, m).ProcessBatch(ctx); err == nil {
		t.Fatal("want fan-out error")
	}
	out := scrape()
	if !strings.Contains(out, `webhook_errors_total{component="fanout"} 1`) {
		t.Errorf("fan-out failure not counted as our error:\n%s", out)
	}
	if strings.Contains(out, "webhook_events_consumed_total") {
		t.Error("unacked events counted as consumed")
	}

	q.Publish(evt("e2", "subscriber.created", "account-1"), evt("e3", "subscriber.created", "account-1"))
	if _, err := dispatcher.New(q, failingStore{}, cache, 10, m).ProcessBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if out := scrape(); !strings.Contains(out, "webhook_events_consumed_total 3") {
		t.Errorf("want 3 events consumed (e1 redelivered + e2 + e3):\n%s", out)
	}
}
