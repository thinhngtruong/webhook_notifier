package partner

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/worker"
)

func TestPercentileNearestRank(t *testing.T) {
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, tt := range []struct{ p, want float64 }{{0.5, 5}, {0.99, 10}, {0.9, 9}, {1, 10}} {
		if got := Percentile(vals, tt.p); got != tt.want {
			t.Errorf("Percentile(%v) = %v, want %v", tt.p, got, tt.want)
		}
	}
	if Percentile(nil, 0.5) != 0 {
		t.Error("empty input should give 0")
	}
}

type clock struct{ t time.Time }

func (c *clock) Now() time.Time { return c.t }

// post sends one signed delivery as the notifier would.
func post(t *testing.T, p *Partner, endpoint, id, secret, body string, at time.Time) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/hooks/"+endpoint, strings.NewReader(body))
	req.Header.Set(worker.HeaderID, id)
	req.Header.Set(worker.HeaderTimestamp, strconv.FormatInt(at.Unix(), 10))
	req.Header.Set(worker.HeaderSignature, worker.Sign(secret, at.Unix(), []byte(body)))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec.Code
}

func body(eventTime time.Time, tier string) string {
	return `{"id":"e","event_time":"` + eventTime.Format(time.RFC3339Nano) + `","tier":"` + tier + `"}`
}

func TestPartnerRecordsLatencyByTierAndDuplicates(t *testing.T) {
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	p := New(Options{Now: clk.Now, Secret: "s"})
	ev := clk.t.Add(-2 * time.Second)

	if code := post(t, p, "a", "d1", "s", body(ev, "small"), clk.t); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	clk.t = clk.t.Add(time.Second)
	post(t, p, "a", "d1", "s", body(ev, "small"), clk.t) // duplicate delivery
	post(t, p, "a", "d2", "s", body(clk.t.Add(-10*time.Second), "large"), clk.t)

	s := p.Stats()
	if s.Requests != 3 || s.Deliveries != 2 || s.Duplicates != 1 || s.BadSignatures != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if got := s.FirstAttempt["small"]; got.Count != 1 || got.P99 != 2 {
		t.Errorf("small first-attempt = %+v, want one sample of 2s (the duplicate does not count)", got)
	}
	if got := s.Delivered["large"]; got.Count != 1 || got.P50 != 10 {
		t.Errorf("large delivered = %+v, want 10s", got)
	}
}

func TestPartnerRejectsBadSignatures(t *testing.T) {
	p := New(Options{Secret: "s"})
	if code := post(t, p, "a", "d1", "wrong", body(time.Now(), "small"), time.Now()); code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", code)
	}
	if s := p.Stats(); s.BadSignatures != 1 || s.Deliveries != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestPartnerErrorRateAndOutages(t *testing.T) {
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	r := 0.0
	p := New(Options{Now: clk.Now, ErrorRate: 0.05, Rand: func() float64 { return r }})
	ev := body(clk.t, "small")

	r = 0.01 // below the error rate: 503
	if code := post(t, p, "a", "d1", "", ev, clk.t); code != http.StatusServiceUnavailable {
		t.Fatalf("injected error = %d, want 503", code)
	}
	r = 0.5
	if code := post(t, p, "a", "d1", "", ev, clk.t); code != http.StatusOK {
		t.Fatalf("retry = %d, want 200", code)
	}

	p.SetOutage("down", clk.t.Add(time.Minute))
	if code := post(t, p, "down", "d2", "", ev, clk.t); code != http.StatusServiceUnavailable {
		t.Fatalf("endpoint in outage = %d, want 503", code)
	}
	if code := post(t, p, "up", "d3", "", ev, clk.t); code != http.StatusOK {
		t.Fatalf("healthy endpoint during another's outage = %d, want 200", code)
	}
	clk.t = clk.t.Add(2 * time.Minute)
	if code := post(t, p, "down", "d2", "", ev, clk.t); code != http.StatusOK {
		t.Fatalf("after the outage = %d, want 200", code)
	}

	s := p.Stats()
	if s.Requests != 5 || s.Deliveries != 3 || s.Duplicates != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if s.FirstAttempt["small"].Count != 3 {
		t.Errorf("first attempts = %d, want 3 (failed requests count as attempts)", s.FirstAttempt["small"].Count)
	}
}

func TestPartnerAddsLatency(t *testing.T) {
	p := New(Options{Latency: 50 * time.Millisecond})
	start := time.Now()
	post(t, p, "a", "d1", "", body(time.Now(), "small"), time.Now())
	if took := time.Since(start); took < 50*time.Millisecond {
		t.Fatalf("answered in %v, want at least 50ms", took)
	}
}

func TestHandlerServesHooksStatsAndOutageAdmin(t *testing.T) {
	p := New(Options{})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/admin/outage?endpoint=down&for=1m", "", nil)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("outage admin = %v, %v", resp, err)
	}
	ev := body(time.Now(), "small")
	if r, _ := http.Post(srv.URL+"/hooks/down", "application/json", strings.NewReader(ev)); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("endpoint in outage = %d", r.StatusCode)
	}
	if r, _ := http.Post(srv.URL+"/hooks/up", "application/json", strings.NewReader(ev)); r.StatusCode != http.StatusOK {
		t.Fatalf("healthy endpoint = %d", r.StatusCode)
	}

	r, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var s Stats
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil || s.Requests != 2 || s.Deliveries != 1 {
		t.Fatalf("stats = %+v, %v", s, err)
	}
	if r, _ := http.Post(srv.URL+"/admin/outage?endpoint=x&for=soon", "", nil); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad duration = %d, want 400", r.StatusCode)
	}
}
