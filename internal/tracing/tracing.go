// Package tracing wires OpenTelemetry traces: W3C trace context carried on
// queue and delivery rows, and an OTLP/HTTP exporter when configured.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var propagator = propagation.TraceContext{}

// Inject returns the W3C traceparent of the span in ctx, or "" if there is
// no valid span.
func Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract returns ctx carrying the remote span context from a traceparent
// produced by Inject. An empty or invalid value returns ctx unchanged.
func Extract(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// Setup installs a global tracer provider that exports over OTLP/HTTP when
// OTEL_EXPORTER_OTLP_ENDPOINT is set (read through getenv), sampling by
// the parent's decision. Without it, tracing stays a no-op. The returned
// shutdown flushes pending spans.
func Setup(ctx context.Context, service string, getenv func(string) string) (shutdown func(context.Context) error, enabled bool, err error) {
	noop := func(context.Context) error { return nil }
	if getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return noop, false, nil
	}
	// The exporter reads the OTEL_EXPORTER_OTLP_* variables itself, with the
	// standard semantics (base URL + /v1/traces).
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return noop, false, fmt.Errorf("otlp trace exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName(service)))
	if err != nil {
		return noop, false, fmt.Errorf("trace resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		// Follow the event's sampling decision (made by the producer), so a
		// trace is kept or dropped end to end; roots use the configured ratio.
		sdktrace.WithSampler(sampler(getenv)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)
	otel.SetErrorHandler(rateLimitedErrors())
	return tp.Shutdown, true, nil
}

// rateLimitedErrors logs exporter errors at most once a minute, so an
// unreachable trace backend does not flood the logs.
func rateLimitedErrors() otel.ErrorHandlerFunc {
	var mu sync.Mutex
	var last time.Time
	return func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(last) < time.Minute {
			return
		}
		last = time.Now()
		slog.Warn("tracing", "err", err)
	}
}

// sampler samples root spans at OTEL_TRACES_SAMPLER_ARG (0..1, default 1)
// and follows the parent's decision otherwise.
func sampler(getenv func(string) string) sdktrace.Sampler {
	ratio := 1.0
	if v, err := strconv.ParseFloat(getenv("OTEL_TRACES_SAMPLER_ARG"), 64); err == nil && v >= 0 && v <= 1 {
		ratio = v
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
}
