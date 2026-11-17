package worker

import (
	"context"
	"errors"
	"fmt"
)

// Outcome is what to do with a delivery after an attempt.
type Outcome int

const (
	Succeeded Outcome = iota // 2xx
	Retry                    // try again later with backoff
	Dead                     // give up; replayable by an admin
	Gone                     // 410: give up and disable the webhook
	Postponed                // not sent (circuit open or host at its limit); attempt not used
)

// Classify maps an attempt's result to an Outcome. err is a transport
// error (timeout, connection refused, TLS); status is ignored when set.
func Classify(status int, err error) Outcome {
	switch {
	case errors.Is(err, ErrBlockedTarget):
		return Dead // the URL itself is not allowed; retrying cannot help
	case err != nil:
		return Retry
	case status >= 200 && status < 300:
		return Succeeded
	case status == 408 || status == 429 || status >= 500:
		return Retry
	case status == 410:
		return Gone
	default:
		return Dead
	}
}

// Cause labels an attempt's result for metrics: which kind of partner
// response or transport failure it was.
func Cause(status int, err error) string {
	var timeout interface{ Timeout() bool }
	switch {
	case errors.Is(err, ErrBlockedTarget):
		return "blocked"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		return "timeout"
	case err != nil:
		return "connection"
	case status == 410:
		return "http_410"
	case status == 429:
		return "http_429"
	default:
		return fmt.Sprintf("http_%dxx", status/100)
	}
}
