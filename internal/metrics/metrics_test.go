package metrics_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/metrics/metricstest"
)

func TestPrometheusOutputUsesDesignNamesAndLabels(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	m, scrape := metricstest.New(t, metrics.Options{
		Now:  func() time.Time { return now },
		Tier: func(a string) string { return map[string]string{"account-whale": "large"}[a] },
	})
	ctx := context.Background()

	m.FirstAttempt(ctx, "account-small", now.Add(-2*time.Second))
	m.FirstAttempt(ctx, "account-whale", now.Add(-90*time.Second))
	m.HTTPRequest(ctx, 200*time.Millisecond, true)
	m.HTTPRequest(ctx, 10*time.Second, false)
	m.Delivery(ctx, "succeeded", "http_2xx")
	m.Delivery(ctx, "retry", "http_5xx")
	m.Finished(ctx, "succeeded", now.Add(-3*time.Second))
	m.Throttled(ctx)
	m.EventsConsumed(ctx, 5)
	m.Error(ctx, "claim")

	out := scrape()
	for _, want := range []string{
		`webhook_first_attempt_latency_seconds_count{tier="small"} 1`,
		`webhook_first_attempt_latency_seconds_sum{tier="small"} 2`,
		`webhook_first_attempt_latency_seconds_count{tier="large"} 1`,
		`webhook_http_duration_seconds_count{result="success"} 1`,
		`webhook_http_duration_seconds_count{result="failure"} 1`,
		`webhook_deliveries_total{cause="http_2xx",outcome="succeeded"} 1`,
		`webhook_deliveries_total{cause="http_5xx",outcome="retry"} 1`,
		`webhook_end_to_end_seconds_sum{outcome="succeeded"} 3`,
		`webhook_endpoint_throttled_total 1`,
		`webhook_events_consumed_total 5`,
		`webhook_errors_total{component="claim"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(out, "otel_scope") {
		t.Error("scope labels leak into every series; disable them")
	}
	if t.Failed() {
		t.Logf("scrape output:\n%s", out)
	}
}

func TestNilMetricsIsANoOp(t *testing.T) {
	var m *metrics.Metrics
	ctx := context.Background()
	m.FirstAttempt(ctx, "a", time.Now())
	m.HTTPRequest(ctx, time.Second, true)
	m.Delivery(ctx, "succeeded", "http_2xx")
	m.Finished(ctx, "dead", time.Now())
	m.Throttled(ctx)
	m.EventsConsumed(ctx, 1)
	m.Error(ctx, "claim")
}
