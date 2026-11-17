package event_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
	q := event.NewPGQueue(testpg.New(t), time.Minute, 5)
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
	q := event.NewPGQueue(testpg.New(t), 100*time.Millisecond, 5)
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
	q := event.NewPGQueue(testpg.New(t), time.Minute, 5)
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
	q := event.NewPGQueue(pool, time.Minute, 5)
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

func deadLetters(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id, reason FROM events_dead_letters`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, reason string
		if err := rows.Scan(&id, &reason); err != nil {
			t.Fatal(err)
		}
		out[id] = reason
	}
	return out
}

func TestPGQueueDeadLettersEventsReceivedTooOften(t *testing.T) {
	pool := testpg.New(t)
	q := event.NewPGQueue(pool, 50*time.Millisecond, 2)
	ctx := context.Background()
	q.Publish(ctx, pgEvent("poison"), pgEvent("fine"))

	for receive := 1; receive <= 2; receive++ { // received twice, never acked
		got, _ := q.Receive(ctx, 10)
		if receive == 1 && len(got) != 2 {
			t.Fatalf("receive %d = %v", receive, ids(got))
		}
		for _, e := range got {
			if e.ID == "fine" && receive == 1 {
				q.Ack(ctx, []string{"fine"})
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got, _ := q.Receive(ctx, 10); len(got) != 0 {
		t.Fatalf("third receive = %v; an event at the receive limit must not be delivered again", ids(got))
	}
	dl := deadLetters(t, pool)
	if len(dl) != 1 || dl["poison"] == "" {
		t.Fatalf("dead letters = %v, want only poison with a reason", dl)
	}
}

func TestPGQueueDeadLetterAndStats(t *testing.T) {
	pool := testpg.New(t)
	q := event.NewPGQueue(pool, 50*time.Millisecond, 5)
	ctx := context.Background()
	if s, err := q.DeadLetterStats(ctx); err != nil || s != (event.QueueStats{}) {
		t.Fatalf("empty stats = %+v, %v", s, err)
	}
	q.Publish(ctx, pgEvent("bad"), pgEvent("old-bad"))
	q.Receive(ctx, 10)
	if err := q.DeadLetter(ctx, "bad", "malformed: missing account"); err != nil {
		t.Fatal(err)
	}
	q.DeadLetter(ctx, "old-bad", "malformed: x")
	pool.Exec(ctx, `UPDATE events_dead_letters SET dead_at = now() - interval '20 seconds' WHERE id = 'old-bad'`)

	time.Sleep(100 * time.Millisecond)
	if got, _ := q.Receive(ctx, 10); len(got) != 0 {
		t.Fatalf("dead-lettered events received again: %v", ids(got))
	}
	if dl := deadLetters(t, pool); dl["bad"] != "malformed: missing account" {
		t.Fatalf("dead letters = %v", dl)
	}
	s, err := q.DeadLetterStats(ctx)
	if err != nil || s.Waiting != 2 || s.OldestAge < 19*time.Second || s.OldestAge > 25*time.Second {
		t.Fatalf("dead-letter stats = %+v, %v; want 2, oldest ~20s", s, err)
	}
}

func TestPGQueueCarriesTraceContext(t *testing.T) {
	q := event.NewPGQueue(testpg.New(t), time.Minute, 5)
	ctx := context.Background()
	e := pgEvent("e1")
	e.TraceContext = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	q.Publish(ctx, e, pgEvent("e2"))
	got, _ := q.Receive(ctx, 10)
	by := map[string]string{}
	for _, g := range got {
		by[g.ID] = g.TraceContext
	}
	if by["e1"] != e.TraceContext || by["e2"] != "" {
		t.Fatalf("trace contexts = %v", by)
	}
}

func TestPGQueueRequeue(t *testing.T) {
	pool := testpg.New(t)
	q := event.NewPGQueue(pool, 50*time.Millisecond, 5)
	ctx := context.Background()
	q.Publish(ctx, pgEvent("r1"), pgEvent("r2"), pgEvent("r3"), pgEvent("bad"))
	q.Receive(ctx, 10)
	for _, id := range []string{"r1", "r2", "r3"} {
		q.DeadLetter(ctx, id, "received 5 times without an ack")
	}
	q.DeadLetter(ctx, "bad", "malformed: missing account_id")

	n, err := q.Requeue(ctx, event.RequeueFilter{ReasonPrefix: "received", IDs: []string{"r1", "r2"}})
	if err != nil || n != 2 {
		t.Fatalf("requeue = %d, %v; want 2", n, err)
	}
	got, _ := q.Receive(ctx, 10)
	if strings.Join(ids(got), ",") != "r1,r2" {
		t.Fatalf("received after requeue = %v", ids(got))
	}
	var counts []int
	rows, _ := pool.Query(ctx, `SELECT receive_count FROM events_queue ORDER BY id`)
	for rows.Next() {
		var c int
		rows.Scan(&c)
		counts = append(counts, c)
	}
	if len(counts) != 2 || counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("receive counts = %v, want a fresh count (1 after this receive)", counts)
	}
	if dl := deadLetters(t, pool); len(dl) != 2 || dl["r3"] == "" || dl["bad"] == "" {
		t.Fatalf("remaining dead letters = %v, want r3 and bad", dl)
	}
	if n, _ := q.Requeue(ctx, event.RequeueFilter{}); n != 2 {
		t.Fatalf("requeue all = %d, want 2", n)
	}
}
