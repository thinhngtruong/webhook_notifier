package dispatcher_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/tracing"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func TestFanOutSpanJoinsTheEventTraceAndIsStoredOnDeliveries(t *testing.T) {
	rec := recordSpans(t)
	f := newFixture(t)
	f.hook(t, "account-1", "subscriber.created")

	eventCtx, eventSpan := otel.Tracer("producer").Start(context.Background(), "event")
	eventSpan.End()
	traced := evt("traced", "subscriber.created", "account-1")
	traced.TraceContext = tracing.Inject(eventCtx)
	untraced := evt("untraced", "subscriber.created", "account-1")
	f.queue.Publish(traced, untraced)

	if _, err := dispatcher.New(f.queue, f.st, f.cache, 10, nil).ProcessBatch(f.ctx); err != nil {
		t.Fatal(err)
	}

	fanout := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		if s.Name() == "webhook.fanout" {
			for _, a := range s.Attributes() {
				if a.Key == "webhook.event_id" {
					fanout[a.Value.AsString()] = s
				}
			}
		}
	}
	tr, un := fanout["traced"], fanout["untraced"]
	if tr == nil || un == nil {
		t.Fatalf("fan-out spans = %v, want one per event", fanout)
	}
	if tr.Parent().SpanID() != eventSpan.SpanContext().SpanID() || tr.SpanContext().TraceID() != eventSpan.SpanContext().TraceID() {
		t.Error("traced event's fan-out span is not a child of the event span")
	}
	if un.Parent().IsValid() {
		t.Error("untraced event's fan-out span should start a new trace")
	}

	ds, err := f.st.Claim(f.ctx, store.ClaimParams{Accounts: 10, Quantum: 10, Limit: 10, Lease: time.Minute})
	if err != nil || len(ds) != 2 {
		t.Fatalf("claim = %d, %v", len(ds), err)
	}
	for _, d := range ds {
		want := fanout[d.EventID].SpanContext().SpanID()
		if got := trace.SpanContextFromContext(tracing.Extract(context.Background(), d.TraceContext)).SpanID(); got != want {
			t.Errorf("%s delivery trace context points at span %s, want its fan-out span %s", d.EventID, got, want)
		}
	}
}
