package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
	"webhook-notifier/migrations"
)

// ---- helpers --------------------------------------------------------------

type env struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	s    *store.Store
	hook map[string]store.UUID // account -> webhook
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := testpg.New(t)
	return &env{t: t, ctx: context.Background(), pool: pool, s: store.New(pool), hook: map[string]store.UUID{}}
}

func (e *env) webhook(account string) store.UUID {
	e.t.Helper()
	if id, ok := e.hook[account]; ok {
		return id
	}
	id, err := e.s.InsertWebhook(e.ctx, store.Webhook{
		AccountID: account, PostURL: "https://partner.example/hooks/" + account,
		Events: []string{"subscriber.created"}, Secret: "s3cret",
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.hook[account] = id
	return id
}

// events builds n deliveries for account, one webhook, event IDs prefix-0..n-1.
func (e *env) events(account, prefix string, n int, at time.Time) []store.NewDelivery {
	e.t.Helper()
	hook := e.webhook(account)
	ds := make([]store.NewDelivery, n)
	for i := range ds {
		ds[i] = store.NewDelivery{
			EventID: fmt.Sprintf("%s-%d", prefix, i), EventName: "subscriber.created",
			EventTime: at, AccountID: account, WebhookID: hook,
			Payload: []byte(fmt.Sprintf(`{"account":%q,"n":%d}`, account, i)),
		}
	}
	return ds
}

func (e *env) fanOut(ds []store.NewDelivery) int64 {
	e.t.Helper()
	n, err := e.s.FanOut(e.ctx, ds)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) claim(p store.ClaimParams) []store.Delivery {
	e.t.Helper()
	if p.Lease == 0 {
		p.Lease = 30 * time.Second
	}
	ds, err := e.s.Claim(e.ctx, p)
	if err != nil {
		e.t.Fatal(err)
	}
	return ds
}

func perAccount(ds []store.Delivery) map[string]int {
	m := map[string]int{}
	for _, d := range ds {
		m[d.AccountID]++
	}
	return m
}

type queueRow struct {
	Active       bool
	LastServedAt time.Time
	NextDueAt    time.Time
	Infinity     bool // last_served_at = '-infinity'
}

func (e *env) queue(account string) queueRow {
	e.t.Helper()
	var q queueRow
	err := e.pool.QueryRow(e.ctx, `
		SELECT active, last_served_at = '-infinity',
		       CASE WHEN last_served_at = '-infinity' THEN 'epoch' ELSE last_served_at END,
		       next_due_at
		  FROM account_queues WHERE account_id = $1`, account,
	).Scan(&q.Active, &q.Infinity, &q.LastServedAt, &q.NextDueAt)
	if err != nil {
		e.t.Fatalf("queue row %s: %v", account, err)
	}
	return q
}

func (e *env) countStatus(status string) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM deliveries WHERE status = $1`, status).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// assertRing checks the store invariant: every account with pending work is
// active in the ring and due no later than its earliest pending delivery.
func (e *env) assertRing() {
	e.t.Helper()
	rows, err := e.pool.Query(e.ctx, `
		SELECT d.account_id, q.active, q.next_due_at, min(d.next_attempt_at)
		  FROM deliveries d LEFT JOIN account_queues q USING (account_id)
		 WHERE d.status = 'pending'
		 GROUP BY d.account_id, q.active, q.next_due_at
		HAVING q.active IS NOT TRUE OR q.next_due_at > min(d.next_attempt_at)`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var acct string
		var active *bool
		var due *time.Time
		var earliest time.Time
		if err := rows.Scan(&acct, &active, &due, &earliest); err != nil {
			e.t.Fatal(err)
		}
		e.t.Errorf("ring invariant broken for %s: active=%v next_due_at=%v earliest pending=%v", acct, active, due, earliest)
	}
}

var now = time.Now().UTC()

// ---- migrations & webhooks -------------------------------------------------

func TestMigrationsConcurrentAndIdempotent(t *testing.T) {
	pool := testpg.NewEmpty(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- migrations.Apply(ctx, pool) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent apply: %v", err)
		}
	}
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("schema_migrations rows = %d, %v; want 1", n, err)
	}
}

func TestActiveWebhooks(t *testing.T) {
	e := newEnv(t)
	a := e.webhook("account-1")
	if _, err := e.s.InsertWebhook(e.ctx, store.Webhook{AccountID: "account-1", PostURL: "https://b.example", Events: []string{"subscriber.unsubscribed"}, Secret: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.InsertWebhook(e.ctx, store.Webhook{AccountID: "account-1", PostURL: "https://c.example", Events: []string{"subscriber.created"}, Secret: "x", Status: "disabled"}); err != nil {
		t.Fatal(err)
	}
	e.webhook("account-2")

	hooks, err := e.s.ActiveWebhooks(e.ctx, "account-1")
	if err != nil || len(hooks) != 2 {
		t.Fatalf("active hooks = %d, %v; want 2", len(hooks), err)
	}
	if err := e.s.DisableWebhook(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	hooks, _ = e.s.ActiveWebhooks(e.ctx, "account-1")
	if len(hooks) != 1 || hooks[0].Subscribes("subscriber.created") || !hooks[0].Subscribes("subscriber.unsubscribed") {
		t.Fatalf("after disable: %+v", hooks)
	}
}

// ---- fan-out ---------------------------------------------------------------

func TestFanOutIsIdempotent(t *testing.T) {
	e := newEnv(t)
	second, err := e.s.InsertWebhook(e.ctx, store.Webhook{AccountID: "account-1", PostURL: "https://b.example", Events: []string{"subscriber.created"}, Secret: "x"})
	if err != nil {
		t.Fatal(err)
	}
	batch := e.events("account-1", "evt", 5, now)
	for _, d := range e.events("account-1", "evt", 5, now) {
		d.WebhookID = second
		batch = append(batch, d)
	}

	if n := e.fanOut(batch); n != 10 {
		t.Fatalf("first fan-out inserted %d, want 10", n)
	}
	q := e.queue("account-1")
	if !q.Active || !q.Infinity {
		t.Fatalf("new account should be active at the front of the ring: %+v", q)
	}
	d, err := e.s.GetDelivery(e.ctx, store.DeliveryID("evt-3", second), now)
	if err != nil || d.Status != store.StatusPending || d.Attempts != 0 || string(d.Payload) != `{"n": 3, "account": "account-1"}` {
		t.Fatalf("delivery = %+v, %v", d, err)
	}

	// Drain the account so it leaves the ring, then redeliver the same
	// events: nothing is inserted and the account is not reactivated.
	if got := len(e.claim(store.ClaimParams{Accounts: 50, Quantum: 10, Limit: 100})); got != 10 {
		t.Fatalf("claimed %d, want 10", got)
	}
	if e.queue("account-1").Active {
		t.Fatal("drained account still active")
	}
	if n := e.fanOut(batch); n != 0 {
		t.Fatalf("redelivery inserted %d, want 0", n)
	}
	if e.queue("account-1").Active {
		t.Fatal("redelivery reactivated the account")
	}
	if c := e.countStatus(store.StatusInFlight); c != 10 {
		t.Fatalf("in flight = %d, want 10", c)
	}
}

func TestFanOutCreatesDailyPartitions(t *testing.T) {
	e := newEnv(t)
	late := time.Date(2026, 3, 1, 23, 59, 59, 999_000_000, time.UTC)
	early := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	batch := append(e.events("account-1", "late", 1, late), e.events("account-1", "early", 1, early)...)
	if n := e.fanOut(batch); n != 2 {
		t.Fatalf("inserted %d", n)
	}
	rows, err := e.pool.Query(e.ctx, `SELECT event_id, tableoid::regclass::text FROM deliveries ORDER BY event_id`)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for rows.Next() {
		var ev, part string
		if err := rows.Scan(&ev, &part); err != nil {
			t.Fatal(err)
		}
		got[ev] = part
	}
	if got["late-0"] != "deliveries_p20260301" || got["early-0"] != "deliveries_p20260302" {
		t.Fatalf("partitions = %v", got)
	}
	// A second Store (another replica) re-ensures without error.
	if err := store.New(e.pool).EnsurePartitions(e.ctx, late, early); err != nil {
		t.Fatal(err)
	}
}

// ---- fair claim ------------------------------------------------------------

func TestClaimFairnessWhaleVsSmallAccounts(t *testing.T) {
	e := newEnv(t)
	e.fanOut(e.events("whale", "w", 1000, now))
	for i := range 10 {
		e.fanOut(e.events(fmt.Sprintf("small-%02d", i), "s", 3, now))
	}

	got := perAccount(e.claim(store.ClaimParams{Accounts: 50, Quantum: 10, Limit: 1000}))
	if got["whale"] != 10 {
		t.Errorf("whale got %d, want quantum 10", got["whale"])
	}
	for i := range 10 {
		acct := fmt.Sprintf("small-%02d", i)
		if got[acct] != 3 {
			t.Errorf("%s got %d, want all 3", acct, got[acct])
		}
		if e.queue(acct).Active {
			t.Errorf("%s has no work left but is still active", acct)
		}
	}
	if q := e.queue("whale"); !q.Active || q.Infinity {
		t.Errorf("whale should stay active and move to the back: %+v", q)
	}

	// With nobody else waiting the whale gets the whole pass (one quantum
	// per pass; K only matters when several accounts wait).
	if got := perAccount(e.claim(store.ClaimParams{Accounts: 50, Quantum: 10, Limit: 1000})); got["whale"] != 10 || len(got) != 1 {
		t.Errorf("second pass = %v", got)
	}
	e.assertRing()
}

func TestClaimRotatesRing(t *testing.T) {
	e := newEnv(t)
	for _, a := range []string{"account-a", "account-b", "account-c"} {
		e.fanOut(e.events(a, "e", 20, now))
	}
	var order []string
	for range 6 {
		ds := e.claim(store.ClaimParams{Accounts: 1, Quantum: 5, Limit: 100})
		if len(ds) != 5 {
			t.Fatalf("claimed %d, want 5", len(ds))
		}
		order = append(order, ds[0].AccountID)
		time.Sleep(2 * time.Millisecond) // distinct last_served_at
	}
	want := []string{"account-a", "account-b", "account-c", "account-a", "account-b", "account-c"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("service order = %v, want %v", order, want)
	}
}

func TestClaimInterleavesWhenSlotsAreScarce(t *testing.T) {
	e := newEnv(t)
	for _, a := range []string{"account-a", "account-b", "account-c"} {
		e.fanOut(e.events(a, "e", 5, now))
	}

	got := perAccount(e.claim(store.ClaimParams{Accounts: 50, Quantum: 10, Limit: 4}))
	if got["account-a"] != 2 || got["account-b"] != 1 || got["account-c"] != 1 {
		t.Fatalf("limit 4 split = %v, want a:2 b:1 c:1", got)
	}

	// Fresh ring: only two slots, so account-f gets nothing and must keep
	// its place at the front for the next pass.
	for _, a := range []string{"account-d", "account-e", "account-f"} {
		e.fanOut(e.events(a, "x", 5, now))
	}
	e.pool.Exec(e.ctx, `UPDATE account_queues SET active = false WHERE account_id IN ('account-a','account-b','account-c')`)
	got = perAccount(e.claim(store.ClaimParams{Accounts: 50, Quantum: 10, Limit: 2}))
	if got["account-d"] != 1 || got["account-e"] != 1 || got["account-f"] != 0 {
		t.Fatalf("limit 2 split = %v", got)
	}
	if !e.queue("account-f").Infinity {
		t.Fatal("unserved account lost its place in the ring")
	}
	if next := e.claim(store.ClaimParams{Accounts: 1, Quantum: 10, Limit: 10}); len(next) == 0 || next[0].AccountID != "account-f" {
		t.Fatalf("next pass should serve account-f first, got %v", perAccount(next))
	}
}

func TestClaimConcurrentReplicasNeverDoubleLease(t *testing.T) {
	e := newEnv(t)
	const accounts, each = 20, 50
	for i := range accounts {
		e.fanOut(e.events(fmt.Sprintf("account-%02d", i), "e", each, now))
	}

	var (
		mu      sync.Mutex
		seen    = map[store.UUID]int{}
		total   atomic.Int64
		wg      sync.WaitGroup
		ctx, cn = context.WithTimeout(e.ctx, 30*time.Second)
	)
	defer cn()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for total.Load() < accounts*each && ctx.Err() == nil {
				ds, err := e.s.Claim(ctx, store.ClaimParams{Accounts: 5, Quantum: 10, Limit: 20, Lease: time.Minute})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, d := range ds {
					seen[d.ID]++
				}
				mu.Unlock()
				total.Add(int64(len(ds)))
			}
		}()
	}
	wg.Wait()

	if total.Load() != accounts*each || len(seen) != accounts*each {
		t.Fatalf("claimed %d (%d distinct), want %d", total.Load(), len(seen), accounts*each)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("delivery %s leased %d times", id, n)
		}
	}
	if c := e.countStatus(store.StatusPending); c != 0 {
		t.Fatalf("%d deliveries left pending", c)
	}
}

// A claim that drains an account races a fan-out adding new work to it.
// Whatever the interleaving, the new work must not be stranded with the
// account out of the ring.
func TestClaimAndFanOutRaceKeepsAccountInRing(t *testing.T) {
	e := newEnv(t)
	const events = 300
	hook := e.webhook("account-race")

	var produced atomic.Bool
	var acked atomic.Int64
	ctx, cancel := context.WithTimeout(e.ctx, 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer produced.Store(true)
		for i := range events {
			_, err := e.s.FanOut(ctx, []store.NewDelivery{{
				EventID: fmt.Sprintf("race-%d", i), EventName: "subscriber.created", EventTime: now,
				AccountID: "account-race", WebhookID: hook, Payload: []byte(`{}`),
			}})
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idle := 0
			for ctx.Err() == nil {
				ds, err := e.s.Claim(ctx, store.ClaimParams{Accounts: 10, Quantum: 2, Limit: 2, Lease: time.Minute})
				if err != nil {
					t.Error(err)
					return
				}
				for _, d := range ds {
					if err := e.s.AckSucceeded(ctx, d.Lease()); err != nil {
						t.Error(err)
						return
					}
					acked.Add(1)
				}
				if len(ds) == 0 {
					if produced.Load() {
						idle++
						if idle > 20 {
							return
						}
					}
					time.Sleep(time.Millisecond)
				} else {
					idle = 0
				}
			}
		}()
	}
	wg.Wait()

	if acked.Load() != events {
		e.assertRing()
		t.Fatalf("acked %d of %d; %d stranded pending", acked.Load(), events, e.countStatus(store.StatusPending))
	}
	if e.queue("account-race").Active {
		t.Fatal("drained account still active")
	}
}

// ---- acks, reaper, replay --------------------------------------------------

func TestAckFencingRejectsStaleLease(t *testing.T) {
	e := newEnv(t)
	e.fanOut(e.events("account-1", "e", 1, now))
	first := e.claim(store.ClaimParams{Accounts: 1, Quantum: 1, Limit: 1, Lease: 50 * time.Millisecond})[0]
	if first.Attempts != 1 || first.Status != store.StatusInFlight || first.LeaseUntil == nil {
		t.Fatalf("claimed = %+v", first)
	}

	time.Sleep(100 * time.Millisecond)
	if res, err := e.s.ResetExpiredLeases(e.ctx, 10, 100); err != nil || res.Reset != 1 {
		t.Fatalf("reap = %+v, %v", res, err)
	}
	second := e.claim(store.ClaimParams{Accounts: 1, Quantum: 1, Limit: 1})[0]
	if second.ID != first.ID || second.Attempts != 2 {
		t.Fatalf("re-claim = %+v", second)
	}

	// The first worker finally answers: every kind of ack is rejected.
	if err := e.s.AckSucceeded(e.ctx, first.Lease()); !errors.Is(err, store.ErrStale) {
		t.Fatalf("stale succeeded ack: %v", err)
	}
	if err := e.s.AckRetry(e.ctx, first.Lease(), time.Minute, "x"); !errors.Is(err, store.ErrStale) {
		t.Fatalf("stale retry ack: %v", err)
	}
	if err := e.s.AckDead(e.ctx, first.Lease(), "x"); !errors.Is(err, store.ErrStale) {
		t.Fatalf("stale dead ack: %v", err)
	}
	if err := e.s.AckSucceeded(e.ctx, second.Lease()); err != nil {
		t.Fatalf("current ack: %v", err)
	}
	if err := e.s.AckSucceeded(e.ctx, second.Lease()); !errors.Is(err, store.ErrStale) {
		t.Fatalf("double ack: %v", err)
	}
	d, _ := e.s.GetDelivery(e.ctx, first.ID, first.EventTime)
	if d.Status != store.StatusSucceeded || d.DeliveredAt == nil || d.LeaseUntil != nil {
		t.Fatalf("final = %+v", d)
	}
}

func TestAckRetryReschedulesAndReactivates(t *testing.T) {
	e := newEnv(t)
	e.fanOut(e.events("account-1", "old", 1, now))
	d := e.claim(store.ClaimParams{Accounts: 1, Quantum: 10, Limit: 10})[0]
	if e.queue("account-1").Active {
		t.Fatal("account with only in-flight work should leave the ring")
	}

	before := time.Now()
	if err := e.s.AckRetry(e.ctx, d.Lease(), time.Hour, "HTTP 503"); err != nil {
		t.Fatal(err)
	}
	q := e.queue("account-1")
	if !q.Active || q.NextDueAt.Before(before.Add(59*time.Minute)) {
		t.Fatalf("after retry ack queue = %+v, want active and due in ~1h", q)
	}
	got, _ := e.s.GetDelivery(e.ctx, d.ID, d.EventTime)
	if got.Status != store.StatusPending || got.Attempts != 1 || got.LastError == nil || *got.LastError != "HTTP 503" {
		t.Fatalf("after retry ack delivery = %+v", got)
	}
	if ds := e.claim(store.ClaimParams{Accounts: 1, Quantum: 10, Limit: 10}); len(ds) != 0 {
		t.Fatalf("claimed %d deliveries before the retry is due", len(ds))
	}
	e.assertRing()

	// New work arrives: next_due_at moves forward to now, and only the new
	// delivery is claimed.
	e.fanOut(e.events("account-1", "new", 1, now))
	if q := e.queue("account-1"); q.NextDueAt.After(time.Now()) {
		t.Fatalf("fan-out did not pull next_due_at forward: %+v", q)
	}
	ds := e.claim(store.ClaimParams{Accounts: 1, Quantum: 10, Limit: 10})
	if len(ds) != 1 || ds[0].EventID != "new-0" {
		t.Fatalf("claimed %+v, want only new-0", ds)
	}
	if q := e.queue("account-1"); !q.Active || q.NextDueAt.Before(before.Add(59*time.Minute)) {
		t.Fatalf("account should wait for the retry: %+v", q)
	}
	e.assertRing()
}

func TestAckDeadAndReplay(t *testing.T) {
	e := newEnv(t)
	e.fanOut(e.events("account-1", "a", 2, now))
	e.fanOut(e.events("account-2", "b", 1, now))
	for _, d := range e.claim(store.ClaimParams{Accounts: 10, Quantum: 10, Limit: 10}) {
		if err := e.s.AckDead(e.ctx, d.Lease(), "HTTP 400"); err != nil {
			t.Fatal(err)
		}
	}
	if c := e.countStatus(store.StatusDead); c != 3 {
		t.Fatalf("dead = %d", c)
	}

	n, err := e.s.ReplayDead(e.ctx, store.ReplayFilter{AccountID: "account-1"})
	if err != nil || n != 2 {
		t.Fatalf("replay = %d, %v", n, err)
	}
	if !e.queue("account-1").Active || e.queue("account-2").Active {
		t.Fatal("replay should reactivate only account-1")
	}
	ds := e.claim(store.ClaimParams{Accounts: 10, Quantum: 10, Limit: 10})
	if len(ds) != 2 || ds[0].Attempts != 1 || ds[0].LastError != nil {
		t.Fatalf("replayed claim = %+v", ds)
	}

	// Filters: wrong webhook and out-of-range time match nothing.
	other := store.NewRandomUUID()
	if n, _ := e.s.ReplayDead(e.ctx, store.ReplayFilter{WebhookID: &other}); n != 0 {
		t.Fatalf("webhook filter replayed %d", n)
	}
	if n, _ := e.s.ReplayDead(e.ctx, store.ReplayFilter{To: now.Add(-time.Hour)}); n != 0 {
		t.Fatalf("time filter replayed %d", n)
	}
	hook := e.hook["account-2"]
	if n, _ := e.s.ReplayDead(e.ctx, store.ReplayFilter{WebhookID: &hook, From: now, To: now.Add(time.Second)}); n != 1 {
		t.Fatalf("matching filter replayed %d, want 1", n)
	}
	e.assertRing()
}

func TestReaperResetsExpiredLeasesAndKillsExhausted(t *testing.T) {
	e := newEnv(t)
	e.fanOut(e.events("account-1", "e", 3, now))
	short := e.claim(store.ClaimParams{Accounts: 1, Quantum: 2, Limit: 2, Lease: 50 * time.Millisecond})
	e.claim(store.ClaimParams{Accounts: 1, Quantum: 1, Limit: 1, Lease: time.Hour}) // stays leased
	if e.queue("account-1").Active {
		t.Fatal("account should be out of the ring while everything is in flight")
	}

	time.Sleep(100 * time.Millisecond)
	res, err := e.s.ResetExpiredLeases(e.ctx, 10, 100)
	if err != nil || res.Reset != 2 || res.Dead != 0 {
		t.Fatalf("reap = %+v, %v", res, err)
	}
	if !e.queue("account-1").Active {
		t.Fatal("reaper did not reactivate the account")
	}
	e.assertRing()
	got, _ := e.s.GetDelivery(e.ctx, short[0].ID, short[0].EventTime)
	if got.Status != store.StatusPending || got.LeaseUntil != nil || got.Attempts != 1 {
		t.Fatalf("reset delivery = %+v", got)
	}
	if c := e.countStatus(store.StatusInFlight); c != 1 {
		t.Fatalf("unexpired lease touched: in flight = %d", c)
	}

	// Second crash on the last allowed attempt: dead, not pending.
	e.claim(store.ClaimParams{Accounts: 1, Quantum: 2, Limit: 2, Lease: 50 * time.Millisecond})
	time.Sleep(100 * time.Millisecond)
	res, err = e.s.ResetExpiredLeases(e.ctx, 2, 100)
	if err != nil || res.Reset != 0 || res.Dead != 2 {
		t.Fatalf("second reap = %+v, %v", res, err)
	}
	if e.queue("account-1").Active {
		t.Fatal("account with only dead/in-flight rows should not be reactivated")
	}
}

// A source may hand the same event to the dispatcher twice in one batch;
// the single INSERT must absorb the duplicate instead of failing.
func TestFanOutAbsorbsDuplicatesWithinOneBatch(t *testing.T) {
	e := newEnv(t)
	ds := e.events("account-1", "evt", 1, now)
	if n := e.fanOut(append(ds, ds[0])); n != 1 {
		t.Fatalf("inserted %d, want 1", n)
	}
}
