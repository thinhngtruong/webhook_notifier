// Package partner is a fake webhook receiver for local runs and
// benchmarks. It records, per delivery ID, when the first request and the
// first 2xx arrived, so latency (from the event time in the payload) and
// duplicates can be measured from the receiver's side.
package partner

import (
	"crypto/hmac"
	"encoding/json"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"webhook-notifier/internal/worker"
)

// Options configures a Partner.
type Options struct {
	Latency   time.Duration    // added to every response
	ErrorRate float64          // fraction of requests answered 503
	Secret    string           // if set, requests with a bad signature get 401
	Rand      func() float64   // nil: math/rand
	Now       func() time.Time // nil: time.Now
}

// Summary is a latency distribution in seconds.
type Summary struct {
	Count         int
	P50, P99, Max float64
}

// Stats is what the partner observed.
type Stats struct {
	Requests      int                // every request
	Deliveries    int                // distinct delivery IDs answered 2xx
	Duplicates    int                // extra 2xx answers for an already delivered ID
	BadSignatures int                // requests rejected with 401
	FirstAttempt  map[string]Summary // by tier: event time -> first request
	Delivered     map[string]Summary // by tier: event time -> first 2xx
}

// Partner is the fake receiver. It serves POST /hooks/{endpoint}.
type Partner struct {
	opts Options

	mu            sync.Mutex
	outages       map[string]time.Time // endpoint -> 503 until
	requests      int
	badSignatures int
	seen          map[string]bool // delivery IDs with at least one request
	delivered     map[string]int  // delivery ID -> 2xx answers
	firstAttempt  map[string][]float64
	firstDelivery map[string][]float64
}

// New returns a Partner.
func New(o Options) *Partner {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Rand == nil {
		o.Rand = rand.Float64
	}
	return &Partner{
		opts: o, seen: map[string]bool{}, delivered: map[string]int{}, outages: map[string]time.Time{},
		firstAttempt: map[string][]float64{}, firstDelivery: map[string][]float64{},
	}
}

// payload is the part of the event body the partner reads.
type payload struct {
	EventTime time.Time `json:"event_time"`
	Tier      string    `json:"tier"`
}

// ServeHTTP implements http.Handler.
func (p *Partner) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := p.opts.Now()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	id := r.Header.Get(worker.HeaderID)

	if p.opts.Secret != "" && !validSignature(p.opts.Secret, r, body) {
		p.mu.Lock()
		p.requests++
		p.badSignatures++
		p.mu.Unlock()
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}

	var pl payload
	_ = json.Unmarshal(body, &pl)
	tier := pl.Tier
	if tier == "" {
		tier = "small"
	}
	latency := now.Sub(pl.EventTime).Seconds()

	p.mu.Lock()
	p.requests++
	if !p.seen[id] {
		p.seen[id] = true
		p.firstAttempt[tier] = append(p.firstAttempt[tier], latency)
	}
	fail := now.Before(p.outages[strings.TrimPrefix(r.URL.Path, "/hooks/")]) || p.opts.Rand() < p.opts.ErrorRate
	if fail {
		p.mu.Unlock()
		p.wait(r)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if p.delivered[id] == 0 {
		p.firstDelivery[tier] = append(p.firstDelivery[tier], latency)
	}
	p.delivered[id]++
	p.mu.Unlock()
	p.wait(r)
	w.WriteHeader(http.StatusOK)
}

// wait applies the configured latency, cut short if the client gives up.
func (p *Partner) wait(r *http.Request) {
	if p.opts.Latency <= 0 {
		return
	}
	t := time.NewTimer(p.opts.Latency)
	defer t.Stop()
	select {
	case <-t.C:
	case <-r.Context().Done():
	}
}

// Handler returns a mux serving POST /hooks/{endpoint}, GET /stats and
// POST /admin/outage?endpoint=<name>&for=<duration>.
func (p *Partner) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /hooks/{endpoint}", p)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Stats())
	})
	mux.HandleFunc("POST /admin/outage", func(w http.ResponseWriter, r *http.Request) {
		d, err := time.ParseDuration(r.URL.Query().Get("for"))
		endpoint := r.URL.Query().Get("endpoint")
		if err != nil || endpoint == "" {
			http.Error(w, "want ?endpoint=<name>&for=<duration>", http.StatusBadRequest)
			return
		}
		p.SetOutage(endpoint, p.opts.Now().Add(d))
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func validSignature(secret string, r *http.Request, body []byte) bool {
	ts, err := strconv.ParseInt(r.Header.Get(worker.HeaderTimestamp), 10, 64)
	if err != nil {
		return false
	}
	want := worker.Sign(secret, ts, body)
	return hmac.Equal([]byte(want), []byte(r.Header.Get(worker.HeaderSignature)))
}

// Stats returns a snapshot of what was observed.
func (p *Partner) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := Stats{
		Requests: p.requests, BadSignatures: p.badSignatures,
		FirstAttempt: summarize(p.firstAttempt), Delivered: summarize(p.firstDelivery),
	}
	for _, n := range p.delivered {
		s.Deliveries++
		s.Duplicates += n - 1
	}
	return s
}

func summarize(byTier map[string][]float64) map[string]Summary {
	out := make(map[string]Summary, len(byTier))
	for tier, vals := range byTier {
		sorted := append([]float64(nil), vals...)
		sort.Float64s(sorted)
		out[tier] = Summary{
			Count: len(sorted), P50: Percentile(sorted, 0.5), P99: Percentile(sorted, 0.99), Max: sorted[len(sorted)-1],
		}
	}
	return out
}

// Percentile returns the p-th percentile (0..1) of sorted values using the
// nearest-rank method; 0 for no values.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(len(sorted))))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

// SetOutage makes endpoint answer 503 until the given time.
func (p *Partner) SetOutage(endpoint string, until time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.outages[endpoint] = until
}
