package worker

// Outcome is what to do with a delivery after an attempt.
type Outcome int

const (
	Succeeded Outcome = iota // 2xx
	Retry                    // try again later with backoff
	Dead                     // give up; replayable by an admin
	Gone                     // 410: give up and disable the webhook
)

// Classify maps an attempt's result to an Outcome. err is a transport
// error (timeout, connection refused, TLS); status is ignored when set.
func Classify(status int, err error) Outcome {
	switch {
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
