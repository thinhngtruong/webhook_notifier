// Package worker delivers leased deliveries to partner endpoints: it signs
// and POSTs the payload, classifies the response and records the outcome.
package worker

import (
	"net/http"
	"strconv"
	"time"
)

// Backoff computes retry delays: Base × Factor^(n-1), capped at Max, then
// spread by ±Jitter (0.2 = ±20%) so retries after an outage do not arrive
// as one spike.
type Backoff struct {
	Base   time.Duration
	Factor float64
	Max    time.Duration
	Jitter float64
}

// Delay returns the wait before the next attempt after `failures` failed
// attempts (>= 1). r is a uniform random number in [0, 1); 0.5 means no
// jitter.
func (b Backoff) Delay(failures int, r float64) time.Duration {
	d := float64(b.Base)
	for i := 1; i < failures && d < float64(b.Max); i++ {
		d *= b.Factor
	}
	d = min(d, float64(b.Max))
	return time.Duration(d * (1 + b.Jitter*(2*r-1)))
}

// ParseRetryAfter reads a Retry-After header value: delta-seconds or an
// HTTP date, relative to now. ok is false when the header is absent or
// unusable.
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	at, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	return max(at.Sub(now), 0), true
}

// RetryDelay is the delay before the next attempt: the backoff delay, or
// the partner's Retry-After if that is longer, never more than Max for a
// Retry-After value.
func (b Backoff) RetryDelay(failures int, r float64, retryAfter time.Duration) time.Duration {
	return max(b.Delay(failures, r), min(retryAfter, b.Max))
}
