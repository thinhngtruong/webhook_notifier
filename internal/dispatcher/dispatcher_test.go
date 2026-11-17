package dispatcher_test

import (
	"context"
	"testing"
	"time"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/event"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

type fixture struct {
	ctx   context.Context
	st    *store.Store
	clk   *fakeClock
	queue *event.MemQueue
	cache *dispatcher.WebhookCache
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st := store.New(testpg.New(t))
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return &fixture{
		ctx:   context.Background(),
		st:    st,
		clk:   clk,
		queue: event.NewMemQueue(time.Minute, clk.Now),
		cache: dispatcher.NewWebhookCache(30*time.Second, clk.Now, st.ActiveWebhooks),
	}
}

func (f *fixture) hook(t *testing.T, account string, events ...string) store.UUID {
	t.Helper()
	id, err := f.st.InsertWebhook(f.ctx, store.Webhook{
		AccountID: account, PostURL: "https://partner.example/" + account, Events: events, Secret: "s",
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func evt(id, name, account string) event.Event {
	return event.Event{
		ID: id, Name: name, AccountID: account,
		Time:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Payload: []byte(`{"id":"` + id + `"}`),
	}
}

func (f *fixture) delivery(t *testing.T, eventID string, hook store.UUID) (store.Delivery, bool) {
	t.Helper()
	d, err := f.st.GetDelivery(f.ctx, store.DeliveryID(eventID, hook), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return d, err == nil
}

func TestProcessBatchFansOutToSubscribedWebhooksOnly(t *testing.T) {
	f := newFixture(t)
	created1 := f.hook(t, "account-1", "subscriber.created")
	unsub1 := f.hook(t, "account-1", "subscriber.unsubscribed")
	created2 := f.hook(t, "account-2", "subscriber.created", "subscriber.added_to_segment")

	f.queue.Publish(
		evt("e1", "subscriber.created", "account-1"),
		evt("e2", "subscriber.unsubscribed", "account-1"),
		evt("e3", "subscriber.created", "account-2"),
	)
	d := dispatcher.New(f.queue, f.st, f.cache, 100)
	n, err := d.ProcessBatch(f.ctx)
	if err != nil || n != 3 {
		t.Fatalf("ProcessBatch = %d, %v; want 3 events", n, err)
	}

	want := map[string]store.UUID{"e1": created1, "e2": unsub1, "e3": created2}
	for ev, hook := range want {
		got, ok := f.delivery(t, ev, hook)
		if !ok || got.EventName == "" || string(got.Payload) != `{"id": "`+ev+`"}` {
			t.Errorf("delivery for %s missing or wrong: %+v", ev, got)
		}
	}
	for _, unwanted := range []struct {
		ev   string
		hook store.UUID
	}{{"e1", unsub1}, {"e2", created1}} {
		if _, ok := f.delivery(t, unwanted.ev, unwanted.hook); ok {
			t.Errorf("unsubscribed webhook got %s", unwanted.ev)
		}
	}

	f.clk.Advance(2 * time.Minute) // past the queue's visibility timeout
	if again, _ := f.queue.Receive(f.ctx, 100); len(again) != 0 {
		t.Fatalf("events not acked: %d redelivered", len(again))
	}
}
