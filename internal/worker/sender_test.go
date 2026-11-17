package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestSendPostsSignedPayloadWithHeaders(t *testing.T) {
	type seen struct {
		method, contentType, id, ts, sig string
		body                             []byte
	}
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.Header.Get("Content-Type"), r.Header.Get(HeaderID),
			r.Header.Get(HeaderTimestamp), r.Header.Get(HeaderSignature), body}
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	at := time.Unix(1767225600, 0)
	s := NewSender(SenderOptions{Timeout: time.Second, Now: func() time.Time { return at }, AllowHTTP: true, AllowPrivateNetworks: true})
	resp, err := s.Send(context.Background(), Request{
		URL: srv.URL, Secret: "whsec_test", DeliveryID: "d-1", Body: []byte(`{"id":"e1"}`),
	})
	if err != nil || resp.Status != http.StatusAccepted || resp.RetryAfter != "120" {
		t.Fatalf("Send = %+v, %v", resp, err)
	}
	r := <-got
	if r.method != http.MethodPost || r.contentType != "application/json" || r.id != "d-1" {
		t.Errorf("request = %+v", r)
	}
	if r.ts != strconv.FormatInt(at.Unix(), 10) {
		t.Errorf("timestamp header = %q", r.ts)
	}
	if string(r.body) != `{"id":"e1"}` || r.sig != Sign("whsec_test", at.Unix(), r.body) {
		t.Errorf("body %q signature %q do not verify", r.body, r.sig)
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	var followed atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	})
	mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, r *http.Request) { followed.Store(true) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := NewSender(SenderOptions{Timeout: time.Second, AllowHTTP: true, AllowPrivateNetworks: true}).Send(context.Background(), Request{URL: srv.URL + "/hook", Body: []byte(`{}`)})
	if err != nil || resp.Status != http.StatusFound || followed.Load() {
		t.Fatalf("Send = %+v, %v, followed = %v", resp, err, followed.Load())
	}
}

func TestSendTimesOut(t *testing.T) {
	// The handler hangs until the test ends. It must not wait on r.Context():
	// the server only notices a client hang-up once the body is read, so
	// srv.Close would block forever.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release) // runs before srv.Close

	start := time.Now()
	_, err := NewSender(SenderOptions{Timeout: 100 * time.Millisecond, AllowHTTP: true, AllowPrivateNetworks: true}).Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
	if err == nil {
		t.Fatal("want a timeout error")
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("timeout not enforced: waited %v", waited)
	}
}
