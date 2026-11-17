// Package metrics records the notifier's metrics (docs/design.md section
// 5) with OpenTelemetry and exposes them in Prometheus format on /metrics.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Options configures Metrics.
type Options struct {
	// Tier maps an account to its latency tier label ("small", "large").
	// Account IDs are too many to be labels themselves. nil: "small".
	Tier func(accountID string) string
	Now  func() time.Time // nil: time.Now
}

// Metrics holds the instruments. A nil *Metrics records nothing, so
// components work without metrics in tests.
type Metrics struct {
	tier func(string) string
	now  func() time.Time

	meter          metric.Meter
	firstAttempt   metric.Float64Histogram
	endToEnd       metric.Float64Histogram
	httpDuration   metric.Float64Histogram
	deliveries     metric.Int64Counter
	throttled      metric.Int64Counter
	eventsConsumed metric.Int64Counter
	errors         metric.Int64Counter
}

// Latency buckets in seconds: sub-second freshness up to the 8h backoff
// cap, since end-to-end time includes retries.
var latencyBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300, 900, 3600, 4 * 3600, 12 * 3600, 30 * 3600}

// Partner request buckets in seconds, up to the 10s timeout.
var httpBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}

// NewPrometheus returns Metrics backed by an OpenTelemetry meter provider
// whose Prometheus exporter is served by the returned handler.
func NewPrometheus(o Options) (*Metrics, http.Handler, error) {
	reg := prometheus.NewRegistry()
	exp, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutScopeInfo(),
		otelprom.WithoutTargetInfo(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	m, err := New(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp)), o)
	if err != nil {
		return nil, nil, err
	}
	return m, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

// New creates the instruments on mp.
func New(mp metric.MeterProvider, o Options) (*Metrics, error) {
	if o.Tier == nil {
		o.Tier = func(string) string { return "small" }
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	m := &Metrics{tier: o.Tier, now: o.Now, meter: mp.Meter("webhook-notifier")}

	var err error
	hist := func(name, desc string, buckets []float64) metric.Float64Histogram {
		if err != nil {
			return nil
		}
		var h metric.Float64Histogram
		h, err = m.meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(buckets...))
		return h
	}
	counter := func(name, desc string) metric.Int64Counter {
		if err != nil {
			return nil
		}
		var c metric.Int64Counter
		c, err = m.meter.Int64Counter(name, metric.WithDescription(desc))
		return c
	}

	m.firstAttempt = hist("webhook.first_attempt_latency", "Time from the event to its first delivery attempt (freshness SLI).", latencyBuckets)
	m.endToEnd = hist("webhook.end_to_end", "Time from the event to a final state.", latencyBuckets)
	m.httpDuration = hist("webhook.http_duration", "Partner request duration.", httpBuckets)
	m.deliveries = counter("webhook.deliveries", "Delivery attempts by outcome and cause.")
	m.throttled = counter("webhook.endpoint_throttled", "Deliveries postponed because their endpoint was at its concurrency limit.")
	m.eventsConsumed = counter("webhook.events_consumed", "Events taken from the event source.")
	m.errors = counter("webhook.errors", "Our own errors (not partner errors), by component.")
	if err != nil {
		return nil, fmt.Errorf("create instruments: %w", err)
	}
	return m, nil
}

func attrs(kv ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(kv...))
}

// FirstAttempt records the time from the event to its first delivery attempt.
func (m *Metrics) FirstAttempt(ctx context.Context, accountID string, eventTime time.Time) {
	if m == nil {
		return
	}
	tier := m.tier(accountID)
	if tier == "" {
		tier = "small"
	}
	m.firstAttempt.Record(ctx, m.now().Sub(eventTime).Seconds(), attrs(attribute.String("tier", tier)))
}

// HTTPRequest records a partner request's duration; success means the
// partner answered 2xx.
func (m *Metrics) HTTPRequest(ctx context.Context, d time.Duration, success bool) {
	if m == nil {
		return
	}
	result := "failure"
	if success {
		result = "success"
	}
	m.httpDuration.Record(ctx, d.Seconds(), attrs(attribute.String("result", result)))
}

// Delivery counts an attempt's outcome and its cause (e.g. "http_5xx",
// "timeout", "circuit_open").
func (m *Metrics) Delivery(ctx context.Context, outcome, cause string) {
	if m == nil {
		return
	}
	m.deliveries.Add(ctx, 1, attrs(attribute.String("outcome", outcome), attribute.String("cause", cause)))
}

// Finished records the time from the event to a final state (succeeded or dead).
func (m *Metrics) Finished(ctx context.Context, outcome string, eventTime time.Time) {
	if m == nil {
		return
	}
	m.endToEnd.Record(ctx, m.now().Sub(eventTime).Seconds(), attrs(attribute.String("outcome", outcome)))
}

// Throttled counts a delivery postponed because its endpoint was at its
// concurrency limit.
func (m *Metrics) Throttled(ctx context.Context) {
	if m == nil {
		return
	}
	m.throttled.Add(ctx, 1)
}

// EventsConsumed counts events taken from the event source.
func (m *Metrics) EventsConsumed(ctx context.Context, n int) {
	if m == nil {
		return
	}
	m.eventsConsumed.Add(ctx, int64(n))
}

// Error counts one of our own errors (not a partner's), by component.
func (m *Metrics) Error(ctx context.Context, component string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, attrs(attribute.String("component", component)))
}
