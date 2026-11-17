package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/partnerguard"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/tracing"
)

// Options configures a Worker.
type Options struct {
	Backoff     Backoff
	MaxAttempts int
	Now         func() time.Time      // nil: time.Now
	Rand        func() float64        // uniform in [0, 1); nil: math/rand
	Limiter     *partnerguard.Limiter // per-host concurrency limit; nil: none
	Breaker     *partnerguard.Breaker // per-host circuit breaker; nil: none
	Metrics     *metrics.Metrics      // nil: not recorded
}

// Worker performs one delivery attempt and records its outcome.
type Worker struct {
	st      *store.Store
	sender  *Sender
	backoff Backoff
	max     int
	now     func() time.Time
	rand    func() float64
	limiter *partnerguard.Limiter
	breaker *partnerguard.Breaker
	metrics *metrics.Metrics
}

// New returns a Worker.
func New(st *store.Store, sender *Sender, opts Options) *Worker {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}
	return &Worker{st: st, sender: sender, backoff: opts.Backoff, max: opts.MaxAttempts, now: opts.Now, rand: opts.Rand,
		limiter: opts.Limiter, breaker: opts.Breaker, metrics: opts.Metrics}
}

// Process sends a claimed delivery and acks the result. It returns the
// outcome; err is only set when recording the outcome failed. A stale lease
// (the reaper took the delivery back) is logged and not an error: the
// delivery is already scheduled again.
func (w *Worker) Process(ctx context.Context, d store.Delivery) (Outcome, error) {
	// The attempt joins the trace of the event's fan-out span. Queue wait
	// (event to send) and the HTTP child span split a slow delivery into
	// time on our side and time at the partner.
	ctx, span := tracer().Start(tracing.Extract(ctx, d.TraceContext), "webhook.deliver",
		trace.WithAttributes(
			attribute.String("webhook.delivery_id", d.ID.String()),
			attribute.String("webhook.account_id", d.AccountID),
			attribute.String("webhook.webhook_id", d.WebhookID.String()),
			attribute.Int("webhook.attempt", d.Attempts),
			attribute.Int64("webhook.queue_wait_ms", w.now().Sub(d.EventTime).Milliseconds()),
		))
	defer span.End()

	outcome, cause, err := w.process(ctx, d)
	span.SetAttributes(attribute.String("webhook.outcome", outcome.String()), attribute.String("webhook.cause", cause))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "recording the outcome failed")
	} else if outcome == Dead || outcome == Gone {
		span.SetStatus(codes.Error, "delivery is dead: "+cause)
	}
	w.metrics.Delivery(ctx, outcome.String(), cause)
	switch outcome {
	case Succeeded:
		w.metrics.Finished(ctx, "succeeded", d.EventTime)
	case Dead, Gone:
		w.metrics.Finished(ctx, "dead", d.EventTime)
	}
	return outcome, err
}

// process does the work of Process and also returns the metrics cause.
func (w *Worker) process(ctx context.Context, d store.Delivery) (Outcome, string, error) {
	log := slog.With("delivery_id", d.ID.String(), "account_id", d.AccountID,
		"webhook_id", d.WebhookID.String(), "attempt", d.Attempts)

	if d.Target == nil || !d.Target.Active {
		return Dead, "webhook_disabled", w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), "webhook disabled"))
	}

	// Guards, limiter first: a half-open circuit hands out a single probe,
	// and that probe must not then be postponed by the limiter, or the
	// circuit would never learn the result.
	host := hostOf(d.Target.URL)
	if w.limiter != nil {
		release, ok := w.limiter.TryAcquire(host)
		if !ok {
			w.metrics.Throttled(ctx)
			return Postponed, "throttled", w.ack(ctx, log, w.st.Postpone(ctx, d.Lease(), throttleDelay, "endpoint at concurrency limit"))
		}
		defer release()
	}
	if w.breaker != nil {
		if wait, ok := w.breaker.Allow(host); !ok {
			return Postponed, "circuit_open", w.ack(ctx, log, w.st.Postpone(ctx, d.Lease(), wait, "circuit open"))
		}
	}

	if d.Attempts == 1 {
		w.metrics.FirstAttempt(ctx, d.AccountID, d.EventTime)
	}
	resp, sendErr := w.sender.Send(ctx, Request{
		URL: d.Target.URL, Secret: d.Target.Secret, DeliveryID: d.ID.String(), Body: d.Payload,
	})
	outcome := Classify(resp.Status, sendErr)
	cause := Cause(resp.Status, sendErr)
	if !errors.Is(sendErr, ErrBlockedTarget) {
		w.metrics.HTTPRequest(ctx, resp.Duration, outcome == Succeeded)
	}
	reason := fmt.Sprintf("HTTP %d", resp.Status)
	if sendErr != nil {
		reason = sendErr.Error()
	}
	if w.breaker != nil {
		// Only partner-side failures count; a 4xx means the endpoint is up.
		w.breaker.Record(host, outcome != Retry)
	}
	log.Info("delivery attempt", "status", resp.Status, "duration_ms", resp.Duration.Milliseconds(),
		"outcome", outcome.String(), "cause", cause, "err", sendErr)

	switch outcome {
	case Succeeded:
		return outcome, cause, w.ack(ctx, log, w.st.AckSucceeded(ctx, d.Lease()))
	case Retry:
		if d.Attempts >= w.max {
			return Dead, cause, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), "attempts exhausted: "+reason))
		}
		retryAfter, _ := ParseRetryAfter(resp.RetryAfter, w.now())
		delay := w.backoff.RetryDelay(d.Attempts, w.rand(), retryAfter)
		return outcome, cause, w.ack(ctx, log, w.st.AckRetry(ctx, d.Lease(), delay, reason))
	case Gone:
		if err := w.st.DisableWebhook(ctx, d.WebhookID); err != nil {
			return outcome, cause, err
		}
		return outcome, cause, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), reason))
	default:
		return outcome, cause, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), reason))
	}
}

// tracer is looked up on each use: a package-level tracer would stay bound
// to whichever provider was installed first.
func tracer() trace.Tracer { return otel.Tracer("webhook-notifier/worker") }

// throttleDelay is how long a delivery waits when its endpoint is at its
// concurrency limit: short, since slots free up within one request time.
const throttleDelay = time.Second

// hostOf returns the host:port a URL points at, the key for per-endpoint
// limits. An unparsable URL gets its own key and fails later in Send.
func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Host
}

// ack turns a stale-lease result into a log line; other errors pass through.
func (w *Worker) ack(ctx context.Context, log *slog.Logger, err error) error {
	if errors.Is(err, store.ErrStale) {
		log.Warn("lease lost before ack; result dropped")
		return nil
	}
	return err
}

func (o Outcome) String() string {
	switch o {
	case Succeeded:
		return "succeeded"
	case Retry:
		return "retry"
	case Dead:
		return "dead"
	case Gone:
		return "gone"
	case Postponed:
		return "postponed"
	}
	return fmt.Sprintf("outcome(%d)", int(o))
}
