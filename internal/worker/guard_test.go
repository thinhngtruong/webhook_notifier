package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", // loopback, RFC 1918
		"169.254.169.254", // link-local: cloud metadata endpoint
		"100.64.0.1",      // carrier-grade NAT
		"0.0.0.0", "::",   // unspecified
		"::1", "fc00::1", "fe80::1", // IPv6 loopback, ULA, link-local
		"::ffff:127.0.0.1", // IPv4-mapped loopback
		"224.0.0.1",        // multicast
	}
	for _, s := range blocked {
		if !IsBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("%s should be blocked", s)
		}
	}
	for _, s := range []string{"93.184.216.34", "2606:4700::1111"} {
		if IsBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("%s should be allowed", s)
		}
	}
}

func TestSendBlocksInternalAddressesAtDialTime(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()

	// HTTPS is allowed, private networks are not: the loopback dial is refused.
	s := NewSender(SenderOptions{Timeout: time.Second})
	_, err := s.Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
	if !errors.Is(err, ErrBlockedTarget) {
		t.Fatalf("err = %v, want ErrBlockedTarget", err)
	}
	if hits.Load() != 0 {
		t.Fatal("request reached the internal server")
	}
}

func TestSendRefusesPlainHTTPUnlessAllowed(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()

	s := NewSender(SenderOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	if _, err := s.Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)}); !errors.Is(err, ErrBlockedTarget) {
		t.Fatalf("err = %v, want ErrBlockedTarget for http://", err)
	}
	if hits.Load() != 0 {
		t.Fatal("plain HTTP request was sent")
	}
}

func TestClassifyBlockedTargetIsDead(t *testing.T) {
	if got := Classify(0, errors.Join(errors.New("dial"), ErrBlockedTarget)); got != Dead {
		t.Fatalf("Classify(blocked) = %v, want Dead (retrying cannot help)", got)
	}
}
