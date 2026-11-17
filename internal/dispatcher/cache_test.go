package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"webhook-notifier/internal/store"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// countingLoader returns one webhook named after the account and the call
// number, so tests can tell a cached result from a fresh load.
type countingLoader struct {
	calls map[string]int
	err   error
}

func (l *countingLoader) load(_ context.Context, account string) ([]store.Webhook, error) {
	l.calls[account]++
	if l.err != nil {
		return nil, l.err
	}
	return []store.Webhook{{AccountID: account, Secret: string(rune('0' + l.calls[account]))}}, nil
}

func TestWebhookCacheLoadsEachAccountOncePerTTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := &countingLoader{calls: map[string]int{}}
	c := NewWebhookCache(30*time.Second, clk.Now, l.load)
	ctx := context.Background()

	for range 3 {
		hooks, err := c.Get(ctx, "account-1")
		if err != nil || len(hooks) != 1 || hooks[0].Secret != "1" {
			t.Fatalf("get = %+v, %v; want the first load", hooks, err)
		}
	}
	c.Get(ctx, "account-2")
	if l.calls["account-1"] != 1 || l.calls["account-2"] != 1 {
		t.Fatalf("loads = %v; want one per account", l.calls)
	}

	clk.Advance(30 * time.Second)
	hooks, _ := c.Get(ctx, "account-1")
	if l.calls["account-1"] != 2 || hooks[0].Secret != "2" {
		t.Fatalf("after TTL: loads = %d, hooks = %+v; want a reload", l.calls["account-1"], hooks)
	}
}

func TestWebhookCacheDoesNotCacheErrors(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := &countingLoader{calls: map[string]int{}, err: errors.New("db down")}
	c := NewWebhookCache(30*time.Second, clk.Now, l.load)
	ctx := context.Background()

	if _, err := c.Get(ctx, "account-1"); err == nil {
		t.Fatal("want load error")
	}
	l.err = nil
	hooks, err := c.Get(ctx, "account-1")
	if err != nil || len(hooks) != 1 || l.calls["account-1"] != 2 {
		t.Fatalf("after recovery: %+v, %v, loads %d", hooks, err, l.calls["account-1"])
	}
}
