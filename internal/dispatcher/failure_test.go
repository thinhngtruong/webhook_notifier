package dispatcher_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/event"
	"webhook-notifier/internal/store"
)

type failingStore struct{ err error }

func (s failingStore) FanOut(context.Context, []store.NewDelivery) (int64, error) { return 0, s.err }

func oneHook(context.Context, string) ([]store.Webhook, error) {
	return []store.Webhook{{ID: store.NewRandomUUID(), Events: []string{"subscriber.created"}}}, nil
}

// A failure anywhere before the commit must leave the events unacked, so
// the source delivers them again (at-least-once).
func TestProcessBatchDoesNotAckOnFailure(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name  string
		st    dispatcher.Store
		hooks dispatcher.LoadFunc
	}{
		{"fan-out fails", failingStore{err: boom}, oneHook},
		{"webhook lookup fails", failingStore{}, func(context.Context, string) ([]store.Webhook, error) { return nil, boom }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(0, 0)}
			q := event.NewMemQueue(time.Minute, clk.Now)
			q.Publish(evt("e1", "subscriber.created", "account-1"))
			d := dispatcher.New(q, tt.st, dispatcher.NewWebhookCache(time.Minute, clk.Now, tt.hooks), 10, nil)

			if _, err := d.ProcessBatch(context.Background()); !errors.Is(err, boom) {
				t.Fatalf("err = %v, want %v", err, boom)
			}
			clk.Advance(2 * time.Minute)
			if again, _ := q.Receive(context.Background(), 10); len(again) != 1 {
				t.Fatalf("redelivered %d events, want 1 (must not be acked)", len(again))
			}
		})
	}
}

func TestProcessBatchAcksEventsWithNoSubscribedWebhook(t *testing.T) {
	f := newFixture(t)
	f.hook(t, "account-1", "subscriber.unsubscribed")
	f.queue.Publish(evt("e1", "subscriber.created", "account-1"), evt("e2", "subscriber.created", "account-none"))

	n, err := dispatcher.New(f.queue, f.st, f.cache, 10, nil).ProcessBatch(f.ctx)
	if err != nil || n != 2 {
		t.Fatalf("ProcessBatch = %d, %v", n, err)
	}
	f.clk.Advance(2 * time.Minute)
	if again, _ := f.queue.Receive(f.ctx, 10); len(again) != 0 {
		t.Fatalf("%d events left in the queue", len(again))
	}
}
