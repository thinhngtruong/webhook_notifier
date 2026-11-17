package tracing

import (
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "event")
	defer span.End()

	tp1 := Inject(ctx)
	if !strings.HasPrefix(tp1, "00-"+span.SpanContext().TraceID().String()+"-") {
		t.Fatalf("traceparent = %q, want version 00 with the span's trace ID", tp1)
	}
	got := trace.SpanContextFromContext(Extract(context.Background(), tp1))
	if got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() || !got.IsRemote() {
		t.Fatalf("extracted = %+v, want the same trace and span, marked remote", got)
	}
}

func TestInjectWithoutSpanAndExtractGarbage(t *testing.T) {
	if got := Inject(context.Background()); got != "" {
		t.Fatalf("Inject without span = %q, want empty", got)
	}
	for _, v := range []string{"", "garbage", "00-zz-yy-01"} {
		if sc := trace.SpanContextFromContext(Extract(context.Background(), v)); sc.IsValid() {
			t.Errorf("Extract(%q) produced a valid span context", v)
		}
	}
}

func TestSetupIsANoOpWithoutEndpoint(t *testing.T) {
	shutdown, enabled, err := Setup(context.Background(), "test", func(string) string { return "" })
	if err != nil || enabled {
		t.Fatalf("Setup without endpoint = enabled %v, %v", enabled, err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupEnablesExportWithEndpoint(t *testing.T) {
	env := map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1"}
	shutdown, enabled, err := Setup(context.Background(), "test", func(k string) string { return env[k] })
	if err != nil || !enabled {
		t.Fatalf("Setup with endpoint = enabled %v, %v", enabled, err)
	}
	_ = shutdown(context.Background()) // nothing listens there; flushing may fail
}

func TestSamplerFollowsParentAndUsesTheRootRatio(t *testing.T) {
	tests := []struct{ arg, want string }{
		{"", "ParentBased{root:TraceIDRatioBased{1}"},
		{"0.01", "ParentBased{root:TraceIDRatioBased{0.01}"},
		{"nonsense", "ParentBased{root:TraceIDRatioBased{1}"},
	}
	for _, tt := range tests {
		got := sampler(func(k string) string {
			if k == "OTEL_TRACES_SAMPLER_ARG" {
				return tt.arg
			}
			return ""
		}).Description()
		if !strings.HasPrefix(got, tt.want) {
			t.Errorf("arg %q: sampler = %s, want prefix %s", tt.arg, got, tt.want)
		}
	}
}
