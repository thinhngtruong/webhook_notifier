// Package dispatcher consumes subscriber events and fans each one out into
// deliveries rows, one per subscribed webhook (docs/design.md section 2).
package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/tracing"
)

// tracer is looked up on each use: a package-level tracer would stay bound
// to whichever provider was installed first.
func tracer() trace.Tracer { return otel.Tracer("webhook-notifier/dispatcher") }

// Store is the part of store.Store the dispatcher writes to.
type Store interface {
	FanOut(ctx context.Context, ds []store.NewDelivery) (int64, error)
}

// Dispatcher moves events from a Source into the deliveries table.
type Dispatcher struct {
	src       event.Source
	st        Store
	hooks     *WebhookCache
	batchSize int
	metrics   *metrics.Metrics
}

// New returns a Dispatcher reading up to batchSize events at a time. m may
// be nil.
func New(src event.Source, st Store, hooks *WebhookCache, batchSize int, m *metrics.Metrics) *Dispatcher {
	return &Dispatcher{src: src, st: st, hooks: hooks, batchSize: batchSize, metrics: m}
}

// ProcessBatch handles one batch: it receives events, inserts one delivery
// per subscribed webhook in a single FanOut transaction, and acks the
// events only after that commits. It returns how many events it consumed.
func (d *Dispatcher) ProcessBatch(ctx context.Context) (int, error) {
	events, err := d.src.Receive(ctx, d.batchSize)
	if err != nil {
		d.metrics.Error(ctx, "event_source")
		return 0, fmt.Errorf("receive events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	var deliveries []store.NewDelivery
	ids := make([]string, 0, len(events))
	for _, e := range events {
		if reason := validate(e); reason != "" {
			// One bad event must not fail the whole batch forever.
			if err := d.src.DeadLetter(ctx, e.ID, "malformed: "+reason); err != nil {
				d.metrics.Error(ctx, "event_source")
				return 0, fmt.Errorf("dead-letter event %s: %w", e.ID, err)
			}
			d.metrics.Error(ctx, "malformed_event")
			slog.Warn("malformed event dead-lettered", "event_id", e.ID, "account_id", e.AccountID, "reason", reason)
			continue
		}
		ids = append(ids, e.ID)
		// One span per event, joining the event's trace when it has one.
		// Its context is stored on each delivery so attempts become its
		// children.
		_, span := tracer().Start(tracing.Extract(ctx, e.TraceContext), "webhook.fanout",
			trace.WithAttributes(
				attribute.String("webhook.event_id", e.ID),
				attribute.String("webhook.event_name", e.Name),
				attribute.String("webhook.account_id", e.AccountID),
			))
		spanCtx := trace.ContextWithSpan(ctx, span)
		hooks, err := d.hooks.Get(ctx, e.AccountID)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "webhook lookup failed")
			span.End()
			d.metrics.Error(ctx, "webhook_lookup")
			return 0, fmt.Errorf("webhooks for %s: %w", e.AccountID, err)
		}
		matched := 0
		for _, h := range hooks {
			if !h.Subscribes(e.Name) {
				continue
			}
			matched++
			deliveries = append(deliveries, store.NewDelivery{
				EventID: e.ID, EventName: e.Name, EventTime: e.Time,
				AccountID: e.AccountID, WebhookID: h.ID, Payload: e.Payload,
				TraceContext: tracing.Inject(spanCtx),
			})
		}
		span.SetAttributes(attribute.Int("webhook.deliveries", matched))
		span.End()
	}

	if len(ids) == 0 {
		d.metrics.EventsConsumed(ctx, len(events))
		return len(events), nil
	}
	if _, err := d.st.FanOut(ctx, deliveries); err != nil {
		d.metrics.Error(ctx, "fanout")
		return 0, err
	}
	if err := d.src.Ack(ctx, ids); err != nil {
		d.metrics.Error(ctx, "event_source")
		return 0, fmt.Errorf("ack %d events: %w", len(ids), err)
	}
	d.metrics.EventsConsumed(ctx, len(events))
	return len(events), nil
}

// Run processes batches until ctx is cancelled, then returns nil. After an
// empty batch or an error it waits idle before trying again; failed events
// were not acked, so the source delivers them again.
func (d *Dispatcher) Run(ctx context.Context, idle time.Duration) error {
	for {
		n, err := d.ProcessBatch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			slog.Error("dispatch batch failed", "err", err)
		}
		if n > 0 && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(idle):
		}
	}
}

// validate returns why an event cannot be fanned out, or "" if it can.
func validate(e event.Event) string {
	switch {
	case e.AccountID == "":
		return "missing account_id"
	case e.Name == "":
		return "missing event name"
	case e.Time.IsZero():
		return "missing event time"
	case !json.Valid(e.Payload) || !bytes.HasPrefix(bytes.TrimSpace(e.Payload), []byte("{")):
		return "payload is not a JSON object"
	}
	return ""
}
