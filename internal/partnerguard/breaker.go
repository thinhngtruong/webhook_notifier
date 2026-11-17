package partnerguard

import (
	"sync"
	"time"
)

// probeWait is how long callers are told to wait while a half-open
// circuit's single probe is in flight.
const probeWait = time.Second

// Breaker is a per-host circuit breaker. After threshold consecutive
// failures the circuit opens: requests are refused for the cooldown, so a
// failing partner is not hammered and its deliveries do not burn attempts.
// Then one probe is let through: success closes the circuit, failure opens
// it again.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu    sync.Mutex
	hosts map[string]*circuit
}

type circuit struct {
	failures  int
	openUntil time.Time // zero when closed
	probing   bool      // half-open with the probe in flight
}

// NewBreaker returns a Breaker that opens after threshold consecutive
// failures and half-opens after cooldown. A nil now uses time.Now.
func NewBreaker(threshold int, cooldown time.Duration, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: now, hosts: map[string]*circuit{}}
}

// Allow reports whether a request to host may be sent now. When it may
// not, wait is how long until the circuit lets a probe through.
func (b *Breaker) Allow(host string) (wait time.Duration, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.hosts[host]
	if c == nil || c.openUntil.IsZero() {
		return 0, true
	}
	if c.probing {
		return probeWait, false
	}
	if now := b.now(); now.Before(c.openUntil) {
		return c.openUntil.Sub(now), false
	}
	c.probing = true
	return 0, true
}

// Record reports the result of a request that Allow let through.
func (b *Breaker) Record(host string, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if success {
		delete(b.hosts, host) // closed and healthy: nothing to remember
		return
	}
	c := b.hosts[host]
	if c == nil {
		c = &circuit{}
		b.hosts[host] = c
	}
	c.failures++
	if c.probing || c.failures >= b.threshold {
		c.openUntil = b.now().Add(b.cooldown)
		c.probing = false
	}
}
