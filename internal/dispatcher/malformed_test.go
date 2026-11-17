package dispatcher_test

import (
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/event"
	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/metrics/metricstest"
)

// One malformed event must not hold back the rest of its batch: it is
// dead-lettered with a reason and the others are fanned out and acked.
func TestProcessBatchDeadLettersMalformedEventsAndKeepsTheRest(t *testing.T) {
	f := newFixture(t)
	hook := f.hook(t, "account-1", "subscriber.created")
	m, scrape := metricstest.New(t, metrics.Options{})

	good := evt("good", "subscriber.created", "account-1")
	noAccount := evt("no-account", "subscriber.created", "")
	noName := evt("no-name", "", "account-1")
	noTime := evt("no-time", "subscriber.created", "account-1")
	noTime.Time = time.Time{}
	notObject := evt("not-object", "subscriber.created", "account-1")
	notObject.Payload = []byte(`["a list"]`)
	notJSON := evt("not-json", "subscriber.created", "account-1")
	notJSON.Payload = []byte(`{oops`)
	f.queue.Publish(good, noAccount, noName, noTime, notObject, notJSON)

	n, err := dispatcher.New(f.queue, f.st, f.cache, 100, m).ProcessBatch(f.ctx)
	if err != nil || n != 6 {
		t.Fatalf("ProcessBatch = %d, %v; want all 6 handled", n, err)
	}
	if _, ok := f.delivery(t, "good", hook); !ok {
		t.Fatal("the well-formed event was not fanned out")
	}
	dl := f.queue.DeadLetters()
	for _, id := range []string{"no-account", "no-name", "no-time", "not-object", "not-json"} {
		if !strings.HasPrefix(dl[id], "malformed: ") {
			t.Errorf("%s dead-letter reason = %q, want malformed: ...", id, dl[id])
		}
	}
	if _, bad := dl["good"]; bad {
		t.Error("well-formed event dead-lettered")
	}
	f.clk.Advance(2 * time.Minute)
	if again, _ := f.queue.Receive(f.ctx, 100); len(again) != 0 {
		t.Fatalf("%d events came back", len(again))
	}
	if out := scrape(); !strings.Contains(out, `webhook_errors_total{component="malformed_event"} 5`) {
		t.Errorf("malformed events not counted:\n%s", out)
	}
	_ = event.Event{}
}
