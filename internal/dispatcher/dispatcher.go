// Package dispatcher consumes subscriber events and fans each one out into
// deliveries rows, one per subscribed webhook (docs/design.md section 2).
package dispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/store"
)

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
}

// New returns a Dispatcher reading up to batchSize events at a time.
func New(src event.Source, st Store, hooks *WebhookCache, batchSize int) *Dispatcher {
	return &Dispatcher{src: src, st: st, hooks: hooks, batchSize: batchSize}
}

// ProcessBatch handles one batch: it receives events, inserts one delivery
// per subscribed webhook in a single FanOut transaction, and acks the
// events only after that commits. It returns how many events it consumed.
func (d *Dispatcher) ProcessBatch(ctx context.Context) (int, error) {
	events, err := d.src.Receive(ctx, d.batchSize)
	if err != nil {
		return 0, fmt.Errorf("receive events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	var deliveries []store.NewDelivery
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
		hooks, err := d.hooks.Get(ctx, e.AccountID)
		if err != nil {
			return 0, fmt.Errorf("webhooks for %s: %w", e.AccountID, err)
		}
		for _, h := range hooks {
			if !h.Subscribes(e.Name) {
				continue
			}
			deliveries = append(deliveries, store.NewDelivery{
				EventID: e.ID, EventName: e.Name, EventTime: e.Time,
				AccountID: e.AccountID, WebhookID: h.ID, Payload: e.Payload,
			})
		}
	}

	if _, err := d.st.FanOut(ctx, deliveries); err != nil {
		return 0, err
	}
	if err := d.src.Ack(ctx, ids); err != nil {
		return 0, fmt.Errorf("ack %d events: %w", len(ids), err)
	}
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
