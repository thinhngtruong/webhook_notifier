package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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

// ErrBlockedTarget means the URL may not be called: plain HTTP when only
// HTTPS is allowed, or an address in a private, loopback, link-local or
// otherwise internal range (SSRF protection).
var ErrBlockedTarget = errors.New("blocked target")

// SenderOptions configures a Sender. The zero value of the Allow fields is
// the production setting.
type SenderOptions struct {
	Timeout              time.Duration
	Now                  func() time.Time // nil: time.Now
	AllowHTTP            bool             // allow plain http:// URLs (local dev)
	AllowPrivateNetworks bool             // allow internal addresses (local dev)
}

// Sender POSTs signed payloads. Redirects are never followed.
type Sender struct {
	client    *http.Client
	now       func() time.Time
	allowHTTP bool
}

// NewSender returns a Sender.
func NewSender(o SenderOptions) *Sender {
	if o.Now == nil {
		o.Now = time.Now
	}
	dialer := &net.Dialer{Timeout: o.Timeout}
	if !o.AllowPrivateNetworks {
		// Check the address actually dialled, after DNS resolution, so a
		// hostname that resolves (or re-resolves) to an internal IP is
		// caught too.
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip, err := netip.ParseAddr(host)
			if err != nil || IsBlockedIP(ip) {
				return fmt.Errorf("%w: %s is an internal address", ErrBlockedTarget, host)
			}
			return nil
		}
	}
	return &Sender{
		client: &http.Client{
			Timeout: o.Timeout,
			Transport: &http.Transport{
				Proxy:               nil, // never route partner traffic through an env proxy
				DialContext:         dialer.DialContext,
				TLSHandshakeTimeout: o.Timeout,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   true,
			},
			// A redirect would send the signed payload somewhere the
			// partner did not register; report the 3xx instead.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now:       o.Now,
		allowHTTP: o.AllowHTTP,
	}
}

// IsBlockedIP reports whether ip is an internal address partners may not
// point us at.
func IsBlockedIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() || sharedAddressSpace.Contains(ip)
}

// sharedAddressSpace is 100.64.0.0/10 (carrier-grade NAT, RFC 6598), which
// netip does not classify as private.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

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
	switch req.URL.Scheme {
	case "https":
	case "http":
		if !s.allowHTTP {
			return Response{}, fmt.Errorf("%w: plain http is not allowed", ErrBlockedTarget)
		}
	default:
		return Response{}, fmt.Errorf("%w: unsupported scheme %q", ErrBlockedTarget, req.URL.Scheme)
	}
	ctx, span := tracer().Start(ctx, "HTTP POST", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", http.MethodPost),
			attribute.String("server.address", req.URL.Hostname()),
			attribute.String("url.scheme", req.URL.Scheme),
		))
	defer span.End()
	req = req.WithContext(ctx)
	// Partners that trace can join the trace; they can ignore the header.
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))

	ts := s.now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhook-notifier/1.0")
	req.Header.Set(HeaderID, r.DeliveryID)
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, Sign(r.Secret, ts, r.Body))

	start := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "request failed")
		return Response{Duration: time.Since(start)}, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 400 {
		span.SetStatus(codes.Error, resp.Status)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	return Response{
		Status:     resp.StatusCode,
		RetryAfter: resp.Header.Get("Retry-After"),
		Duration:   time.Since(start),
	}, nil
}
