package partnerguard

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestLimiterCapsEachHostIndependently(t *testing.T) {
	l := NewLimiter(2)
	r1, ok1 := l.TryAcquire("a.example")
	_, ok2 := l.TryAcquire("a.example")
	if !ok1 || !ok2 {
		t.Fatal("first two acquisitions should succeed")
	}
	if _, ok := l.TryAcquire("a.example"); ok {
		t.Fatal("third acquisition on a full host should fail")
	}
	if _, ok := l.TryAcquire("b.example"); !ok {
		t.Fatal("another host must not be affected")
	}
	r1()
	if _, ok := l.TryAcquire("a.example"); !ok {
		t.Fatal("release should free a slot")
	}
}

func TestLimiterNeverExceedsLimitUnderConcurrency(t *testing.T) {
	const limit = 3
	l := NewLimiter(limit)
	var held, maxHeld atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				release, ok := l.TryAcquire("a.example")
				if !ok {
					continue
				}
				n := held.Add(1)
				for m := maxHeld.Load(); n > m && !maxHeld.CompareAndSwap(m, n); m = maxHeld.Load() {
				}
				held.Add(-1)
				release()
			}
		}()
	}
	wg.Wait()
	if m := maxHeld.Load(); m > limit || m == 0 {
		t.Fatalf("max held = %d, want 1..%d", m, limit)
	}
}
