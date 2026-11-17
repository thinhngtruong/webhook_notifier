package worker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
	"webhook-notifier/internal/worker"
)

// partner answers every request with the configured status and headers.
type partner struct {
	srv        *httptest.Server
	status     int
	retryAfter string
	hits       atomic.Int64
}

func newPartner(t *testing.T, status int, retryAfter string) *partner {
	p := &partner{status: status, retryAfter: retryAfter}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.hits.Add(1)
		if p.retryAfter != "" {
			w.Header().Set("Retry-After", p.retryAfter)
		}
		w.WriteHeader(p.status)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// claimOne seeds one delivery to url, sets its attempt count so the claim
// yields attempt number `attempt`, and claims it.
func claimOne(t *testing.T, st *store.Store, url string, attempt int, disabled bool) store.Delivery {
	t.Helper()
	ctx := context.Background()
	hook, err := st.InsertWebhook(ctx, store.Webhook{AccountID: "account-1", PostURL: url, Events: []string{"subscriber.created"}, Secret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if disabled {
		st.DisableWebhook(ctx, hook)
	}
	at := time.Now().UTC()
	if _, err := st.FanOut(ctx, []store.NewDelivery{{EventID: "e1", EventName: "subscriber.created", EventTime: at,
		AccountID: "account-1", WebhookID: hook, Payload: []byte(`{"id":"e1"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE deliveries SET attempts = $1`, attempt-1); err != nil {
		t.Fatal(err)
	}
	ds, err := st.Claim(ctx, store.ClaimParams{Accounts: 1, Quantum: 1, Limit: 1, Lease: time.Minute})
	if err != nil || len(ds) != 1 || ds[0].Attempts != attempt {
		t.Fatalf("claim = %+v, %v", ds, err)
	}
	return ds[0]
}

func TestProcessRecordsOutcome(t *testing.T) {
	tests := []struct {
		name         string
		status       int // 0: no server (connection refused)
		retryAfter   string
		attempt      int
		disabled     bool
		wantOutcome  worker.Outcome
		wantStatus   string
		wantDelay    time.Duration // for retries
		wantError    string
		wantDisabled bool
		wantHits     int64
	}{
		{name: "2xx succeeds", status: 200, attempt: 1, wantOutcome: worker.Succeeded, wantStatus: store.StatusSucceeded, wantHits: 1},
		{name: "503 retries with backoff", status: 503, attempt: 2, wantOutcome: worker.Retry, wantStatus: store.StatusPending, wantDelay: 90 * time.Second, wantError: "HTTP 503", wantHits: 1},
		{name: "429 honours Retry-After", status: 429, retryAfter: "3600", attempt: 1, wantOutcome: worker.Retry, wantStatus: store.StatusPending, wantDelay: time.Hour, wantError: "HTTP 429", wantHits: 1},
		{name: "last attempt goes dead", status: 503, attempt: 10, wantOutcome: worker.Dead, wantStatus: store.StatusDead, wantError: "attempts exhausted: HTTP 503", wantHits: 1},
		{name: "4xx is dead", status: 400, attempt: 1, wantOutcome: worker.Dead, wantStatus: store.StatusDead, wantError: "HTTP 400", wantHits: 1},
		{name: "410 disables webhook", status: 410, attempt: 1, wantOutcome: worker.Gone, wantStatus: store.StatusDead, wantError: "HTTP 410", wantDisabled: true, wantHits: 1},
		{name: "connection error retries", status: 0, attempt: 1, wantOutcome: worker.Retry, wantStatus: store.StatusPending, wantDelay: 30 * time.Second},
		{name: "disabled webhook is dead without a request", status: 200, attempt: 1, disabled: true, wantOutcome: worker.Dead, wantStatus: store.StatusDead, wantError: "webhook disabled", wantDisabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := store.New(testpg.New(t))
			ctx := context.Background()

			var p *partner
			url := "http://127.0.0.1:1/unreachable"
			if tt.status != 0 {
				p = newPartner(t, tt.status, tt.retryAfter)
				url = p.srv.URL
			}
			d := claimOne(t, st, url, tt.attempt, tt.disabled)

			w := worker.New(st, worker.NewSender(time.Second, nil), worker.Options{
				Backoff:     worker.Backoff{Base: 30 * time.Second, Factor: 3, Max: 8 * time.Hour, Jitter: 0.2},
				MaxAttempts: 10,
				Rand:        func() float64 { return 0.5 },
			})
			before := time.Now()
			outcome, err := w.Process(ctx, d)
			if err != nil || outcome != tt.wantOutcome {
				t.Fatalf("Process = %v, %v; want %v", outcome, err, tt.wantOutcome)
			}

			got, err := st.GetDelivery(ctx, d.ID, d.EventTime)
			if err != nil || got.Status != tt.wantStatus {
				t.Fatalf("status = %q, %v; want %q", got.Status, err, tt.wantStatus)
			}
			if tt.wantError != "" && (got.LastError == nil || *got.LastError != tt.wantError) {
				t.Errorf("last_error = %v, want %q", got.LastError, tt.wantError)
			}
			if tt.wantDelay > 0 {
				delay := got.NextAttemptAt.Sub(before)
				if delay < tt.wantDelay-5*time.Second || delay > tt.wantDelay+5*time.Second {
					t.Errorf("next attempt in %v, want ~%v", delay, tt.wantDelay)
				}
			}
			hook, _ := st.Webhook(ctx, d.WebhookID)
			if (hook.Status == "disabled") != tt.wantDisabled {
				t.Errorf("webhook status = %s, want disabled=%v", hook.Status, tt.wantDisabled)
			}
			if p != nil && p.hits.Load() != tt.wantHits {
				t.Errorf("partner hits = %d, want %d", p.hits.Load(), tt.wantHits)
			}
		})
	}
}

// The reaper reset the delivery while the request was in flight: the late
// result is dropped without an error and does not overwrite the row.
func TestProcessDropsResultWhenLeaseWasLost(t *testing.T) {
	st := store.New(testpg.New(t))
	ctx := context.Background()
	p := newPartner(t, 200, "")
	d := claimOne(t, st, p.srv.URL, 1, false)
	if _, err := st.Pool().Exec(ctx, `UPDATE deliveries SET status = 'pending', lease_until = NULL`); err != nil {
		t.Fatal(err)
	}

	w := worker.New(st, worker.NewSender(time.Second, nil), worker.Options{MaxAttempts: 10})
	if _, err := w.Process(ctx, d); err != nil {
		t.Fatalf("Process = %v, want nil for a lost lease", err)
	}
	if got, _ := st.GetDelivery(ctx, d.ID, d.EventTime); got.Status != store.StatusPending {
		t.Fatalf("status = %s, want pending (untouched)", got.Status)
	}
}
