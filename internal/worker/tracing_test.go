package worker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"webhook-notifier/internal/partnerguard"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
	"webhook-notifier/internal/tracing"
	"webhook-notifier/internal/worker"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func spanNamed(rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	for _, s := range rec.Ended() {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func attr(s sdktrace.ReadOnlySpan, key string) attribute.Value {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value
		}
	}
	return attribute.Value{}
}

func TestProcessTracesTheAttemptUnderTheFanOutSpan(t *testing.T) {
	rec := recordSpans(t)
	st := store.New(testpg.New(t))
	ctx := context.Background()

	var traceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
	}))
	defer srv.Close()

	d := claimOne(t, st, srv.URL, 1, false)
	fanCtx, fanSpan := otel.Tracer("test").Start(ctx, "webhook.fanout")
	fanSpan.End()
	d.TraceContext = tracing.Inject(fanCtx)

	w := worker.New(st, worker.NewSender(worker.SenderOptions{Timeout: time.Second, AllowHTTP: true, AllowPrivateNetworks: true}),
		worker.Options{MaxAttempts: 10})
	if _, err := w.Process(ctx, d); err != nil {
		t.Fatal(err)
	}

	deliver := spanNamed(rec, "webhook.deliver")
	post := spanNamed(rec, "HTTP POST")
	if deliver == nil || post == nil {
		t.Fatalf("spans = deliver %v, post %v", deliver != nil, post != nil)
	}
	if deliver.Parent().SpanID() != fanSpan.SpanContext().SpanID() {
		t.Error("webhook.deliver is not a child of the fan-out span")
	}
	if post.Parent().SpanID() != deliver.SpanContext().SpanID() || post.SpanKind() != trace.SpanKindClient {
		t.Error("HTTP POST is not a client child span of webhook.deliver")
	}
	if attr(deliver, "webhook.outcome").AsString() != "succeeded" || attr(deliver, "webhook.attempt").AsInt64() != 1 ||
		attr(deliver, "webhook.delivery_id").AsString() != d.ID.String() || attr(deliver, "webhook.queue_wait_ms").Type() != attribute.INT64 {
		t.Errorf("deliver attributes = %v", deliver.Attributes())
	}
	if attr(post, "http.response.status_code").AsInt64() != 200 {
		t.Errorf("post attributes = %v", post.Attributes())
	}
	if !strings.Contains(traceparent, post.SpanContext().SpanID().String()) {
		t.Errorf("partner got traceparent %q, want the HTTP span %s", traceparent, post.SpanContext().SpanID())
	}
}

func TestPostponedAttemptHasNoHTTPSpan(t *testing.T) {
	rec := recordSpans(t)
	st := store.New(testpg.New(t))
	p := newPartner(t, 200, "")
	limiter := partnerguard.NewLimiter(1)
	release, _ := limiter.TryAcquire(strings.TrimPrefix(p.srv.URL, "http://"))
	defer release()

	if _, err := guardedWorker(st, limiter, nil).Process(context.Background(), claimOne(t, st, p.srv.URL, 1, false)); err != nil {
		t.Fatal(err)
	}
	deliver := spanNamed(rec, "webhook.deliver")
	if deliver == nil || attr(deliver, "webhook.outcome").AsString() != "postponed" || attr(deliver, "webhook.cause").AsString() != "throttled" {
		t.Fatalf("deliver span = %v", deliver)
	}
	if spanNamed(rec, "HTTP POST") != nil {
		t.Error("an HTTP span was recorded for a postponed delivery")
	}
}
