// Package metricstest builds Metrics for tests and scrapes their
// Prometheus output.
package metricstest

import (
	"io"
	"net/http/httptest"
	"testing"

	"webhook-notifier/internal/metrics"
)

// New returns Metrics and a function that scrapes /metrics as text.
func New(t testing.TB, o metrics.Options) (*metrics.Metrics, func() string) {
	t.Helper()
	m, handler, err := metrics.NewPrometheus(o)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	return m, func() string {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		body, _ := io.ReadAll(rec.Result().Body)
		return string(body)
	}
}
