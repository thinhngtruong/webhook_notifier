package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Request is one signed POST to a partner.
type Request struct {
	URL        string
	Secret     string
	DeliveryID string
	Body       []byte
}

// Response is what the partner answered.
type Response struct {
	Status     int
	RetryAfter string // raw Retry-After header, if any
	Duration   time.Duration
}

// Sender POSTs signed payloads. Redirects are never followed.
type Sender struct {
	client *http.Client
	now    func() time.Time
}

// NewSender returns a Sender with the given per-request timeout. A nil now
// uses time.Now.
func NewSender(timeout time.Duration, now func() time.Time) *Sender {
	if now == nil {
		now = time.Now
	}
	return &Sender{
		client: &http.Client{
			Timeout: timeout,
			// A redirect would send the signed payload somewhere the
			// partner did not register; report the 3xx instead.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: now,
	}
}

// maxDrain bounds how much of a response body is read so the connection
// can be reused without letting a partner stream an unbounded body.
const maxDrain = 64 << 10

// Send performs one attempt. A transport error (timeout, refused
// connection) is returned as err; any HTTP status is returned in Response.
func (s *Sender) Send(ctx context.Context, r Request) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return Response{}, fmt.Errorf("build request: %w", err)
	}
	ts := s.now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhook-notifier/1.0")
	req.Header.Set(HeaderID, r.DeliveryID)
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, Sign(r.Secret, ts, r.Body))

	start := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		return Response{Duration: time.Since(start)}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	return Response{
		Status:     resp.StatusCode,
		RetryAfter: resp.Header.Get("Retry-After"),
		Duration:   time.Since(start),
	}, nil
}
