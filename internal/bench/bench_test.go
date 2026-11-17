package bench

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func adminURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("TEST_DATABASE_URL")
	if u == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping benchmark scenario")
	}
	return u
}

func TestFairSchedulerProtectsSmallAccountsDuringWhaleBurst(t *testing.T) {
	res, err := RunFairness(context.Background(), adminURL(t), Quick)
	if err != nil {
		t.Fatal(err)
	}
	byMode := map[string]FairnessResult{}
	for _, r := range res {
		if r.Delivered != r.Expected || r.Expected == 0 {
			t.Fatalf("%s: delivered %d of %d", r.Mode, r.Delivered, r.Expected)
		}
		byMode[r.Mode] = r
	}
	shared, fair := byMode["shared"], byMode["fair"]
	t.Logf("small p99: shared %v, fair %v", shared.SmallP99, fair.SmallP99)
	if fair.SmallP99 == 0 || fair.SmallP99*2 > shared.SmallP99 {
		t.Fatalf("fair small p99 %v is not clearly better than shared %v", fair.SmallP99, shared.SmallP99)
	}
}

func TestScalingAddsThroughput(t *testing.T) {
	res, err := RunScaling(context.Background(), adminURL(t), Quick)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 || res[0].Delivered != res[0].Expected || res[1].Delivered != res[1].Expected {
		t.Fatalf("results = %+v", res)
	}
	t.Logf("throughput: 1 replica %.0f/s, 2 replicas %.0f/s", res[0].Throughput, res[1].Throughput)
	if res[1].Throughput < 1.4*res[0].Throughput {
		t.Fatalf("2 replicas %.0f/s vs 1 replica %.0f/s: not scaling", res[1].Throughput, res[0].Throughput)
	}
}

func TestReliabilityDeliversEverythingAndIsolatesTheOutage(t *testing.T) {
	r, err := RunReliability(context.Background(), adminURL(t), Quick)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
	if r.Published == 0 || r.Delivered != r.Published || r.Dead != 0 {
		t.Fatalf("published %d, delivered %d, dead %d: want every delivery to succeed eventually", r.Published, r.Delivered, r.Dead)
	}
	if r.HealthyP99 > time.Second {
		t.Fatalf("healthy endpoints' p99 %v: the outage leaked into them", r.HealthyP99)
	}
	if r.OutageDeliveredP99 < Quick.Outage/2 {
		t.Fatalf("outage endpoint p99 %v shorter than the outage: was it down?", r.OutageDeliveredP99)
	}
}

func TestCrashLosesNoDeliveries(t *testing.T) {
	r, err := RunCrash(context.Background(), adminURL(t), Quick)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
	if r.Published == 0 || r.Lost != 0 || r.Delivered != r.Published {
		t.Fatalf("published %d, delivered %d, lost %d", r.Published, r.Delivered, r.Lost)
	}
}

func TestReportPrintsEveryScenario(t *testing.T) {
	var buf bytes.Buffer
	Report(&buf, Quick,
		[]FairnessResult{{Mode: "shared", SmallP99: 30 * time.Second, Expected: 10, Delivered: 10}, {Mode: "fair", SmallP99: 400 * time.Millisecond, Expected: 10, Delivered: 10}},
		[]ScalingResult{{Replicas: 1, Throughput: 300}, {Replicas: 2, Throughput: 590}},
		ReliabilityResult{Published: 100, Delivered: 100, HealthyP99: 200 * time.Millisecond},
		CrashResult{Published: 50, Delivered: 50, Duplicates: 3})
	out := buf.String()
	for _, want := range []string{"Whale burst", "shared", "fair", "30s", "400ms", "Horizontal scaling", "590", "Reliability", "Crash", "Duplicates"} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q:\n%s", want, out)
		}
	}
}
