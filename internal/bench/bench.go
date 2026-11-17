// Package bench runs the benchmark scenarios of docs/design.md section 6
// in-process: each run gets a throwaway database, a fake partner over real
// HTTP, and N notifier "replicas" (dispatcher + claim loop + workers +
// reaper, each with its own connection pool).
package bench

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"text/tabwriter"
	"time"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/partner"
	"webhook-notifier/internal/producer"
)

// Scale sizes the scenarios.
type Scale struct {
	Name string

	// Whale burst.
	WhaleEvents, SmallAccounts, SmallEvents int
	FairnessWorkers                         int           // per replica (2 replicas)
	FairnessLatency                         time.Duration // partner latency

	// Horizontal scaling.
	ScalingRate     int // events/s offered
	ScalingFor      time.Duration
	ScalingWorkers  int // per replica
	ScalingLatency  time.Duration
	ScalingReplicas []int

	// Reliability and crash.
	SteadyRate int
	SteadyFor  time.Duration
	Outage     time.Duration
	CrashAfter time.Duration

	// Timeout bounds the wait for a scenario's deliveries, so a broken run
	// fails instead of hanging.
	Timeout time.Duration
}

// Quick is small enough for the test suite.
var Quick = Scale{
	Name:        "quick",
	WhaleEvents: 2000, SmallAccounts: 50, SmallEvents: 4, FairnessWorkers: 8, FairnessLatency: 10 * time.Millisecond,
	ScalingRate: 400, ScalingFor: 2 * time.Second, ScalingWorkers: 4, ScalingLatency: 50 * time.Millisecond, ScalingReplicas: []int{1, 2},
	SteadyRate: 100, SteadyFor: 3 * time.Second, Outage: 3 * time.Second, CrashAfter: time.Second,
	Timeout: 90 * time.Second,
}

// Default is what make bench runs: a few minutes on a laptop.
var Default = Scale{
	Name:        "default",
	WhaleEvents: 50000, SmallAccounts: 500, SmallEvents: 10, FairnessWorkers: 32, FairnessLatency: 20 * time.Millisecond,
	ScalingRate: 1200, ScalingFor: 10 * time.Second, ScalingWorkers: 16, ScalingLatency: 50 * time.Millisecond, ScalingReplicas: []int{1, 2, 4},
	SteadyRate: 300, SteadyFor: 20 * time.Second, Outage: 15 * time.Second, CrashAfter: 7 * time.Second,
	Timeout: 15 * time.Minute,
}

// Full uses the sizes from docs/design.md (1M whale events, 1,000 small
// accounts, 1,000 events/s for 10 minutes). It takes a long time.
var Full = Scale{
	Name:        "full",
	WhaleEvents: 1000000, SmallAccounts: 1000, SmallEvents: 10, FairnessWorkers: 64, FairnessLatency: 20 * time.Millisecond,
	ScalingRate: 1000, ScalingFor: 10 * time.Minute, ScalingWorkers: 32, ScalingLatency: 50 * time.Millisecond, ScalingReplicas: []int{1, 2, 4},
	SteadyRate: 500, SteadyFor: 10 * time.Minute, Outage: 5 * time.Minute, CrashAfter: 3 * time.Minute,
	Timeout: 3 * time.Hour,
}

// FairnessResult is one mode of the whale-burst scenario.
type FairnessResult struct {
	Mode               string // "shared" or "fair"
	SmallP50, SmallP99 time.Duration
	SmallMax           time.Duration
	WhaleDone          time.Duration // event time -> whale's last delivery
	Expected           int
	Delivered          int
}

// ScalingResult is one replica count of the scaling scenario.
type ScalingResult struct {
	Replicas   int
	Expected   int
	Delivered  int
	Throughput float64 // deliveries per second, start to last delivery
	P99        time.Duration
}

// ReliabilityResult is the partner errors + outage scenario.
type ReliabilityResult struct {
	Published, Delivered, Dead int
	HealthyP99                 time.Duration // first attempt, healthy endpoints
	OutageDeliveredP99         time.Duration // event -> delivered, endpoint that was down
	Duplicates                 int
}

// CrashResult is the replica crash scenario.
type CrashResult struct {
	Published, Delivered, Lost, Duplicates int
	MaxDelivered                           time.Duration
}

// RunFairness runs the whale burst with a shared queue and with the fair
// scheduler.
func RunFairness(ctx context.Context, adminURL string, sc Scale) ([]FairnessResult, error) {
	var out []FairnessResult
	for _, mode := range []string{"shared", "fair"} {
		r, err := runFairness(ctx, adminURL, sc, mode)
		if err != nil {
			return out, fmt.Errorf("fairness (%s): %w", mode, err)
		}
		out = append(out, r)
	}
	return out, nil
}

func runFairness(ctx context.Context, adminURL string, sc Scale, mode string) (FairnessResult, error) {
	e, err := newEnv(ctx, adminURL)
	if err != nil {
		return FairnessResult{}, err
	}
	defer e.close()
	p := partner.New(partner.Options{Latency: sc.FairnessLatency, Secret: secret})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	small := make([]string, sc.SmallAccounts)
	for i := range small {
		small[i] = producer.SmallAccount(i)
	}
	url := func(a string) string { return srv.URL + "/hooks/" + a }
	if err := producer.SeedWebhooks(ctx, e.st, append([]string{producer.WhaleAccount}, small...), url, secret); err != nil {
		return FairnessResult{}, err
	}
	// The partner host is not the bottleneck here: this scenario isolates
	// the scheduler, so the per-host limit is as large as the pool.
	rs, err := startReplicas(e, 2, replicaConfig{mode: mode, workers: sc.FairnessWorkers, perHost: sc.FairnessWorkers})
	if err != nil {
		return FairnessResult{}, err
	}
	defer stopAll(rs)

	start := time.Now()
	whale := producer.WhaleBurst(sc.WhaleEvents, 0, 0, start).Whale
	if err := publishChunks(ctx, e.queue, whale); err != nil {
		return FairnessResult{}, err
	}
	// Small accounts keep producing events while the whale's backlog drains.
	for wave := range sc.SmallEvents {
		now := time.Now()
		batch := make([]event.Event, len(small))
		for i, a := range small {
			batch[i] = producer.NewEvent(fmt.Sprintf("%s-%d", a, wave), a, "small", now)
		}
		if err := e.queue.Publish(ctx, batch...); err != nil {
			return FairnessResult{}, err
		}
		time.Sleep(100 * time.Millisecond)
	}

	expected := sc.WhaleEvents + sc.SmallAccounts*sc.SmallEvents
	if _, err := waitDelivered(ctx, p, expected, start, sc.Timeout); err != nil {
		return FairnessResult{}, err
	}
	s := p.Stats()
	sm := s.FirstAttempt["small"]
	return FairnessResult{
		Mode: mode, SmallP50: seconds(sm.P50), SmallP99: seconds(sm.P99), SmallMax: seconds(sm.Max),
		WhaleDone: seconds(s.Delivered["large"].Max), Expected: expected, Delivered: s.Deliveries,
	}, nil
}

func publishChunks(ctx context.Context, q *event.PGQueue, es []event.Event) error {
	for len(es) > 0 {
		n := min(1000, len(es))
		if err := q.Publish(ctx, es[:n]...); err != nil {
			return err
		}
		es = es[n:]
	}
	return nil
}

// RunScaling runs steady load with each replica count.
func RunScaling(ctx context.Context, adminURL string, sc Scale) ([]ScalingResult, error) {
	var out []ScalingResult
	for _, n := range sc.ScalingReplicas {
		r, err := runScaling(ctx, adminURL, sc, n)
		if err != nil {
			return out, fmt.Errorf("scaling (%d replicas): %w", n, err)
		}
		out = append(out, r)
	}
	return out, nil
}

func smallAccounts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = producer.SmallAccount(i)
	}
	return out
}

func runScaling(ctx context.Context, adminURL string, sc Scale, replicas int) (ScalingResult, error) {
	e, err := newEnv(ctx, adminURL)
	if err != nil {
		return ScalingResult{}, err
	}
	defer e.close()
	p := partner.New(partner.Options{Latency: sc.ScalingLatency, Secret: secret})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	accounts := smallAccounts(100)
	if err := producer.SeedWebhooks(ctx, e.st, accounts, func(a string) string { return srv.URL + "/hooks/" + a }, secret); err != nil {
		return ScalingResult{}, err
	}
	// Workers are the per-replica bottleneck (partner latency bound), so
	// adding replicas adds capacity until the offered rate is reached.
	rs, err := startReplicas(e, replicas, replicaConfig{mode: "fair", workers: sc.ScalingWorkers, perHost: sc.ScalingWorkers * replicas})
	if err != nil {
		return ScalingResult{}, err
	}
	defer stopAll(rs)

	start := time.Now()
	n, err := producer.Steady(ctx, e.queue, sc.ScalingRate, sc.ScalingFor, accounts, func(string) string { return "small" })
	if err != nil {
		return ScalingResult{}, err
	}
	elapsed, err := waitDelivered(ctx, p, n, start, sc.Timeout)
	if err != nil {
		return ScalingResult{}, err
	}
	s := p.Stats()
	return ScalingResult{
		Replicas: replicas, Expected: n, Delivered: s.Deliveries,
		Throughput: float64(s.Deliveries) / elapsed.Seconds(), P99: seconds(s.FirstAttempt["small"].P99),
	}, nil
}

// RunReliability runs steady load with 5% partner errors and one endpoint
// down for sc.Outage.
func RunReliability(ctx context.Context, adminURL string, sc Scale) (ReliabilityResult, error) {
	e, err := newEnv(ctx, adminURL)
	if err != nil {
		return ReliabilityResult{}, err
	}
	defer e.close()
	p := partner.New(partner.Options{Latency: 20 * time.Millisecond, ErrorRate: 0.05, Secret: secret})
	// Two listeners = two partner hosts, since limits and circuits are per
	// host: the outage must not take the healthy endpoints down with it.
	healthy := httptest.NewServer(p.Handler())
	defer healthy.Close()
	down := httptest.NewServer(p.Handler())
	defer down.Close()

	var accounts []string
	tier := map[string]string{}
	for i := range 40 {
		h, d := fmt.Sprintf("account-healthy-%02d", i), fmt.Sprintf("account-outage-%02d", i)
		accounts = append(accounts, h, d)
		tier[h], tier[d] = "healthy", "outage"
	}
	url := func(a string) string {
		if tier[a] == "outage" {
			return down.URL + "/hooks/down"
		}
		return healthy.URL + "/hooks/" + a
	}
	if err := producer.SeedWebhooks(ctx, e.st, accounts, url, secret); err != nil {
		return ReliabilityResult{}, err
	}
	rs, err := startReplicas(e, 2, replicaConfig{mode: "fair", workers: 16, perHost: 8})
	if err != nil {
		return ReliabilityResult{}, err
	}
	defer stopAll(rs)

	start := time.Now()
	p.SetOutage("down", start.Add(sc.Outage))
	n, err := producer.Steady(ctx, e.queue, sc.SteadyRate, sc.SteadyFor, accounts, func(a string) string { return tier[a] })
	if err != nil {
		return ReliabilityResult{}, err
	}
	if _, err := waitDelivered(ctx, p, n, start, sc.Timeout); err != nil {
		return ReliabilityResult{}, err
	}
	dead, err := e.dead(ctx)
	if err != nil {
		return ReliabilityResult{}, err
	}
	s := p.Stats()
	return ReliabilityResult{
		Published: n, Delivered: s.Deliveries, Dead: dead, Duplicates: s.Duplicates,
		HealthyP99:         seconds(s.FirstAttempt["healthy"].P99),
		OutageDeliveredP99: seconds(s.Delivered["outage"].P99),
	}, nil
}

// RunCrash runs steady load on two replicas and crashes one mid-run.
func RunCrash(ctx context.Context, adminURL string, sc Scale) (CrashResult, error) {
	e, err := newEnv(ctx, adminURL)
	if err != nil {
		return CrashResult{}, err
	}
	defer e.close()
	p := partner.New(partner.Options{Latency: 20 * time.Millisecond, Secret: secret})
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()
	accounts := smallAccounts(100)
	if err := producer.SeedWebhooks(ctx, e.st, accounts, func(a string) string { return srv.URL + "/hooks/" + a }, secret); err != nil {
		return CrashResult{}, err
	}
	rs, err := startReplicas(e, 2, replicaConfig{mode: "fair", workers: 16, perHost: 32})
	if err != nil {
		return CrashResult{}, err
	}
	defer stopAll(rs)

	start := time.Now()
	go func() {
		select {
		case <-time.After(sc.CrashAfter):
			rs[0].crash()
		case <-ctx.Done():
		}
	}()
	n, err := producer.Steady(ctx, e.queue, sc.SteadyRate, sc.SteadyFor, accounts, func(string) string { return "small" })
	if err != nil {
		return CrashResult{}, err
	}
	// Recovery needs the lease to expire and the event visibility timeout
	// to pass; give it a generous bound, then report what arrived.
	_, waitErr := waitDelivered(ctx, p, n, start, sc.Timeout)
	s := p.Stats()
	r := CrashResult{
		Published: n, Delivered: s.Deliveries, Lost: n - s.Deliveries, Duplicates: s.Duplicates,
		MaxDelivered: seconds(s.Delivered["small"].Max),
	}
	if waitErr != nil && r.Lost == 0 {
		return r, waitErr
	}
	return r, nil
}

// Report writes the results as readable tables.
func Report(w io.Writer, sc Scale, f []FairnessResult, s []ScalingResult, r ReliabilityResult, c CrashResult) {
	ms := func(d time.Duration) string { return d.Round(time.Millisecond).String() }
	fmt.Fprintf(w, "Benchmark scale: %s (in-process replicas, real Postgres, partner over HTTP; timings compressed: lease %v, backoff from %v)\n\n",
		sc.Name, timing.lease, timing.backoff.Base)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Whale burst: %d whale events + %d small accounts x %d events, 2 replicas x %d workers\n",
		sc.WhaleEvents, sc.SmallAccounts, sc.SmallEvents, sc.FairnessWorkers)
	fmt.Fprintln(tw, "Mode\tSmall p50\tSmall p99\tSmall max\tWhale done\tDelivered\t")
	for _, x := range f {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d/%d\t\n", x.Mode, ms(x.SmallP50), ms(x.SmallP99), ms(x.SmallMax), ms(x.WhaleDone), x.Delivered, x.Expected)
	}
	tw.Flush()
	fmt.Fprintln(w, "(small = event to first attempt for small accounts; whale done = event to the whale's last delivery)")
	fmt.Fprintln(w)

	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Horizontal scaling: %d events/s offered for %v, %d workers per replica, partner latency %v\n",
		sc.ScalingRate, sc.ScalingFor, sc.ScalingWorkers, sc.ScalingLatency)
	fmt.Fprintln(tw, "Replicas\tThroughput (deliveries/s)\tFirst-attempt p99\tDelivered\t")
	for _, x := range s {
		fmt.Fprintf(tw, "%d\t%.0f\t%s\t%d/%d\t\n", x.Replicas, x.Throughput, ms(x.P99), x.Delivered, x.Expected)
	}
	tw.Flush()
	fmt.Fprintln(w)

	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Reliability: %d events/s for %v, 5%% partner errors, one endpoint down for %v\n", sc.SteadyRate, sc.SteadyFor, sc.Outage)
	fmt.Fprintln(tw, "Published\tDelivered\tDead\tHealthy first-attempt p99\tDown endpoint delivered p99\tDuplicates\t")
	fmt.Fprintf(tw, "%d\t%d\t%d\t%s\t%s\t%d\t\n", r.Published, r.Delivered, r.Dead, ms(r.HealthyP99), ms(r.OutageDeliveredP99), r.Duplicates)
	tw.Flush()
	fmt.Fprintln(w)

	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Crash: %d events/s for %v on 2 replicas, one killed after %v\n", sc.SteadyRate, sc.SteadyFor, sc.CrashAfter)
	fmt.Fprintln(tw, "Published\tDelivered\tLost\tDuplicates\tSlowest delivery\t")
	fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%s\t\n", c.Published, c.Delivered, c.Lost, c.Duplicates, ms(c.MaxDelivered))
	tw.Flush()
}
