package event_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/testpg"
)

func pgEvent(id string) event.Event {
	return event.Event{ID: id, Name: "subscriber.created", AccountID: "account-1",
		Time: time.Now().UTC(), Payload: []byte(`{"id":"` + id + `"}`)}
}

func ids(es []event.Event) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.ID
	}
	sort.Strings(out)
	return out
}

func TestPGQueueReceivesInBatchesAndHidesReceivedEvents(t *testing.T) {
	q := event.NewPGQueue(testpg.New(t), time.Minute)
	ctx := context.Background()
	if err := q.Publish(ctx, pgEvent("e1"), pgEvent("e2"), pgEvent("e3")); err != nil {
		t.Fatal(err)
	}
	first, err := q.Receive(ctx, 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("first receive = %v, %v", ids(first), err)
	}
	second, _ := q.Receive(ctx, 2)
	if len(second) != 1 {
		t.Fatalf("second receive = %v, want the remaining one", ids(second))
	}
	if third, _ := q.Receive(ctx, 2); len(third) != 0 {
		t.Fatalf("third receive = %v, want none while all are in flight", ids(third))
	}
	got := second[0]
	if got.Name != "subscriber.created" || got.AccountID != "account-1" || got.Time.IsZero() || string(got.Payload) != `{"id": "`+got.ID+`"}` {
		t.Fatalf("event round trip = %+v", got)
	}
}

func TestPGQueueRedeliversUnackedEventsAfterVisibility(t *testing.T) {
	q := event.NewPGQueue(testpg.New(t), 100*time.Millisecond)
	ctx := context.Background()
	q.Publish(ctx, pgEvent("acked"), pgEvent("lost"))
	if got, _ := q.Receive(ctx, 10); len(got) != 2 {
		t.Fatalf("receive = %v", ids(got))
	}
	if err := q.Ack(ctx, []string{"acked"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // visibility timeout, on database time
	got, _ := q.Receive(ctx, 10)
	if len(got) != 1 || got[0].ID != "lost" {
		t.Fatalf("after visibility = %v, want [lost]", ids(got))
	}
}

func TestPGQueuePublishIsIdempotent(t *testing.T) {
	q := event.NewPGQueue(testpg.New(t), time.Minute)
	ctx := context.Background()
	q.Publish(ctx, pgEvent("e1"))
	if err := q.Publish(ctx, pgEvent("e1")); err != nil {
		t.Fatalf("republish: %v", err)
	}
	if got, _ := q.Receive(ctx, 10); len(got) != 1 {
		t.Fatalf("receive = %v, want one copy", ids(got))
	}
}

func TestPGQueueStats(t *testing.T) {
	pool := testpg.New(t)
	q := event.NewPGQueue(pool, time.Minute)
	ctx := context.Background()
	if s, err := q.Stats(ctx); err != nil || s != (event.QueueStats{}) {
		t.Fatalf("empty stats = %+v, %v", s, err)
	}
	q.Publish(ctx, pgEvent("old"), pgEvent("new"))
	if _, err := pool.Exec(ctx, `UPDATE events_queue SET enqueued_at = now() - interval '30 seconds' WHERE id = 'old'`); err != nil {
		t.Fatal(err)
	}
	s, err := q.Stats(ctx)
	if err != nil || s.Waiting != 2 || s.OldestAge < 29*time.Second || s.OldestAge > 35*time.Second {
		t.Fatalf("stats = %+v, %v; want 2 waiting, oldest ~30s", s, err)
	}
}
