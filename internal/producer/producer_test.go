package producer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/producer"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
)

func TestNewEventPayloadCarriesEventTimeAndTier(t *testing.T) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 123456789, time.UTC)
	e := producer.NewEvent("e1", "account-1", "small", at)
	if e.ID != "e1" || e.Name != "subscriber.created" || e.AccountID != "account-1" || !e.Time.Equal(at) {
		t.Fatalf("event = %+v", e)
	}
	var p struct {
		ID        string    `json:"id"`
		AccountID string    `json:"account_id"`
		EventTime time.Time `json:"event_time"`
		Tier      string    `json:"tier"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.ID != "e1" || p.AccountID != "account-1" || p.Tier != "small" || !p.EventTime.Equal(at) {
		t.Fatalf("payload = %s", e.Payload)
	}
}

func TestWhaleBurstMix(t *testing.T) {
	b := producer.WhaleBurst(100, 5, 3, time.Now())
	if len(b.Whale) != 100 || len(b.Small) != 15 || len(b.SmallAccounts) != 5 {
		t.Fatalf("burst = %d whale, %d small, %d small accounts", len(b.Whale), len(b.Small), len(b.SmallAccounts))
	}
	ids := map[string]bool{}
	for _, e := range append(append([]event.Event{}, b.Whale...), b.Small...) {
		if ids[e.ID] {
			t.Fatalf("duplicate event ID %s", e.ID)
		}
		ids[e.ID] = true
	}
	if b.Whale[0].AccountID != producer.WhaleAccount || b.Small[0].AccountID == producer.WhaleAccount {
		t.Fatalf("accounts: whale %s, small %s", b.Whale[0].AccountID, b.Small[0].AccountID)
	}
}

type countingPub struct {
	mu       sync.Mutex
	n        int
	accounts map[string]int
}

func (c *countingPub) Publish(_ context.Context, es ...event.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range es {
		c.n++
		c.accounts[e.AccountID]++
	}
	return nil
}

func TestSteadyPublishesAtRoughlyTheRate(t *testing.T) {
	pub := &countingPub{accounts: map[string]int{}}
	n, err := producer.Steady(context.Background(), pub, 2000, 300*time.Millisecond,
		[]string{"a", "b", "c"}, func(string) string { return "small" })
	if err != nil {
		t.Fatal(err)
	}
	if n != pub.n || n < 400 || n > 800 {
		t.Fatalf("published %d (counted %d), want ~600 for 2000/s over 300ms", n, pub.n)
	}
	for _, a := range []string{"a", "b", "c"} {
		if pub.accounts[a] < n/3-2 {
			t.Errorf("account %s got %d of %d: not spread round-robin", a, pub.accounts[a], n)
		}
	}
}

func TestSeedWebhooks(t *testing.T) {
	st := store.New(testpg.New(t))
	ctx := context.Background()
	accounts := []string{"account-1", "account-2"}
	if err := producer.SeedWebhooks(ctx, st, accounts, func(a string) string { return "http://partner/hooks/" + a }, "s"); err != nil {
		t.Fatal(err)
	}
	for _, a := range accounts {
		hooks, err := st.ActiveWebhooks(ctx, a)
		if err != nil || len(hooks) != 1 || hooks[0].PostURL != fmt.Sprintf("http://partner/hooks/%s", a) || !hooks[0].Subscribes("subscriber.created") {
			t.Fatalf("%s hooks = %+v, %v", a, hooks, err)
		}
	}
}

func TestSeedWebhooksIsIdempotent(t *testing.T) {
	st := store.New(testpg.New(t))
	ctx := context.Background()
	url := func(a string) string { return "http://partner/hooks/" + a }
	for range 2 {
		if err := producer.SeedWebhooks(ctx, st, []string{"account-1"}, url, "s"); err != nil {
			t.Fatal(err)
		}
	}
	if hooks, _ := st.ActiveWebhooks(ctx, "account-1"); len(hooks) != 1 {
		t.Fatalf("after seeding twice: %d webhooks, want 1", len(hooks))
	}
}
