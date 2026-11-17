package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"webhook-notifier/internal/store"
)

// Options configures a Worker.
type Options struct {
	Backoff     Backoff
	MaxAttempts int
	Now         func() time.Time // nil: time.Now
	Rand        func() float64   // uniform in [0, 1); nil: math/rand
}

// Worker performs one delivery attempt and records its outcome.
type Worker struct {
	st      *store.Store
	sender  *Sender
	backoff Backoff
	max     int
	now     func() time.Time
	rand    func() float64
}

// New returns a Worker.
func New(st *store.Store, sender *Sender, opts Options) *Worker {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}
	return &Worker{st: st, sender: sender, backoff: opts.Backoff, max: opts.MaxAttempts, now: opts.Now, rand: opts.Rand}
}

// Process sends a claimed delivery and acks the result. It returns the
// outcome; err is only set when recording the outcome failed. A stale lease
// (the reaper took the delivery back) is logged and not an error: the
// delivery is already scheduled again.
func (w *Worker) Process(ctx context.Context, d store.Delivery) (Outcome, error) {
	log := slog.With("delivery_id", d.ID.String(), "account_id", d.AccountID,
		"webhook_id", d.WebhookID.String(), "attempt", d.Attempts)

	if d.Target == nil || !d.Target.Active {
		return Dead, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), "webhook disabled"))
	}

	resp, sendErr := w.sender.Send(ctx, Request{
		URL: d.Target.URL, Secret: d.Target.Secret, DeliveryID: d.ID.String(), Body: d.Payload,
	})
	outcome := Classify(resp.Status, sendErr)
	reason := fmt.Sprintf("HTTP %d", resp.Status)
	if sendErr != nil {
		reason = sendErr.Error()
	}
	log.Info("delivery attempt", "status", resp.Status, "duration_ms", resp.Duration.Milliseconds(),
		"outcome", outcome.String(), "err", sendErr)

	switch outcome {
	case Succeeded:
		return outcome, w.ack(ctx, log, w.st.AckSucceeded(ctx, d.Lease()))
	case Retry:
		if d.Attempts >= w.max {
			return Dead, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), "attempts exhausted: "+reason))
		}
		retryAfter, _ := ParseRetryAfter(resp.RetryAfter, w.now())
		delay := w.backoff.RetryDelay(d.Attempts, w.rand(), retryAfter)
		return outcome, w.ack(ctx, log, w.st.AckRetry(ctx, d.Lease(), delay, reason))
	case Gone:
		if err := w.st.DisableWebhook(ctx, d.WebhookID); err != nil {
			return outcome, err
		}
		return outcome, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), reason))
	default:
		return outcome, w.ack(ctx, log, w.st.AckDead(ctx, d.Lease(), reason))
	}
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
	}
	return fmt.Sprintf("outcome(%d)", int(o))
}
