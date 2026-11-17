// Package partnerguard protects partner endpoints and our worker pool from
// each other: a per-host concurrency limit and a per-host circuit breaker.
package partnerguard

import "sync"

// Limiter caps concurrent requests per host. It never blocks: a caller that
// cannot get a slot postpones the delivery instead of holding a worker.
type Limiter struct {
	perHost int

	mu    sync.Mutex
	inUse map[string]int
}

// NewLimiter returns a Limiter allowing perHost concurrent requests.
func NewLimiter(perHost int) *Limiter {
	return &Limiter{perHost: perHost, inUse: map[string]int{}}
}

// TryAcquire takes a slot for host. ok is false when the host is at its
// limit. release must be called exactly once when ok is true.
func (l *Limiter) TryAcquire(host string) (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inUse[host] >= l.perHost {
		return nil, false
	}
	l.inUse[host]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.inUse[host]--; l.inUse[host] == 0 {
			delete(l.inUse, host) // keep the map bounded to busy hosts
		}
	}, true
}
