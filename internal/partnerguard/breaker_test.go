package partnerguard

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker() (*Breaker, *fakeClock) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	return NewBreaker(3, 30*time.Second, clk.Now), clk
}

func fail(b *Breaker, host string, n int) {
	for range n {
		b.Allow(host)
		b.Record(host, false)
	}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreaker()
	fail(b, "a.example", 2)
	b.Record("a.example", true) // a success resets the count
	fail(b, "a.example", 2)
	if _, ok := b.Allow("a.example"); !ok {
		t.Fatal("opened before 3 consecutive failures")
	}
	b.Record("a.example", false)
	wait, ok := b.Allow("a.example")
	if ok || wait != 30*time.Second {
		t.Fatalf("after 3 failures Allow = %v, %v; want open with 30s wait", wait, ok)
	}
	if _, ok := b.Allow("b.example"); !ok {
		t.Fatal("another host must not be affected")
	}
}

func TestBreakerHalfOpensWithASingleProbe(t *testing.T) {
	b, clk := newTestBreaker()
	fail(b, "a.example", 3)

	clk.Advance(10 * time.Second)
	if wait, ok := b.Allow("a.example"); ok || wait != 20*time.Second {
		t.Fatalf("during cooldown Allow = %v, %v; want 20s wait", wait, ok)
	}

	clk.Advance(20 * time.Second)
	if _, ok := b.Allow("a.example"); !ok {
		t.Fatal("after cooldown one probe should be allowed")
	}
	if _, ok := b.Allow("a.example"); ok {
		t.Fatal("only one probe may be in flight while half-open")
	}

	b.Record("a.example", false) // probe failed: open for another cooldown
	if wait, ok := b.Allow("a.example"); ok || wait != 30*time.Second {
		t.Fatalf("after failed probe Allow = %v, %v; want reopened", wait, ok)
	}

	clk.Advance(30 * time.Second)
	b.Allow("a.example")
	b.Record("a.example", true) // probe succeeded: closed
	for range 5 {
		if _, ok := b.Allow("a.example"); !ok {
			t.Fatal("circuit should be closed after a successful probe")
		}
	}
}
