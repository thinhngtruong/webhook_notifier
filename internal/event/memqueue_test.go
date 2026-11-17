package event

import (
	"context"
	"testing"
	"time"
)

func ids(es []Event) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.ID
	}
	return out
}

func TestMemQueueReceiveReturnsPublishedEventsInBatches(t *testing.T) {
	q := NewMemQueue(time.Minute, nil)
	q.Publish(Event{ID: "e1"}, Event{ID: "e2"}, Event{ID: "e3"})
	ctx := context.Background()

	first, err := q.Receive(ctx, 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("first receive = %v, %v; want 2 events", ids(first), err)
	}
	second, _ := q.Receive(ctx, 2)
	if len(second) != 1 || second[0].ID != "e3" {
		t.Fatalf("second receive = %v; want [e3]", ids(second))
	}
	if third, _ := q.Receive(ctx, 2); len(third) != 0 {
		t.Fatalf("third receive = %v; want none (all in flight)", ids(third))
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func TestMemQueueRedeliversUnackedEventsAfterVisibilityTimeout(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	q := NewMemQueue(30*time.Second, clk.Now)
	q.Publish(Event{ID: "acked"}, Event{ID: "lost"})
	ctx := context.Background()

	if got, _ := q.Receive(ctx, 10); len(got) != 2 {
		t.Fatalf("receive = %v", ids(got))
	}
	if err := q.Ack(ctx, []string{"acked"}); err != nil {
		t.Fatal(err)
	}

	clk.Advance(29 * time.Second)
	if got, _ := q.Receive(ctx, 10); len(got) != 0 {
		t.Fatalf("redelivered before timeout: %v", ids(got))
	}
	clk.Advance(2 * time.Second)
	got, _ := q.Receive(ctx, 10)
	if len(got) != 1 || got[0].ID != "lost" {
		t.Fatalf("after timeout = %v; want [lost]", ids(got))
	}
}
