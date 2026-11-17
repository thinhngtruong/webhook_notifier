package replay_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/replay"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/testpg"
)

func seedDead(t *testing.T, st *store.Store, account string, n int) {
	t.Helper()
	ctx := context.Background()
	hook, err := st.InsertWebhook(ctx, store.Webhook{AccountID: account, PostURL: "https://partner.example", Events: []string{"subscriber.created"}, Secret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	ds := make([]store.NewDelivery, n)
	for i := range ds {
		ds[i] = store.NewDelivery{EventID: fmt.Sprintf("%s-%d", account, i), EventName: "subscriber.created",
			EventTime: time.Now().UTC(), AccountID: account, WebhookID: hook, Payload: []byte(`{}`)}
	}
	if _, err := st.FanOut(ctx, ds); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.Claim(ctx, store.ClaimParams{Accounts: 1, Quantum: n, Limit: n, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range claimed {
		if err := st.AckDead(ctx, d.Lease(), "HTTP 400"); err != nil {
			t.Fatal(err)
		}
	}
}

func countStatus(t *testing.T, st *store.Store, account, status string) int {
	t.Helper()
	var n int
	if err := st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM deliveries WHERE account_id = $1 AND status = $2`, account, status).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRunReplaysMatchingDeadDeliveries(t *testing.T) {
	st := store.New(testpg.New(t))
	seedDead(t, st, "account-a", 2)
	seedDead(t, st, "account-b", 1)

	var out bytes.Buffer
	if err := replay.Run(context.Background(), st, []string{"--account=account-a"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "replayed 2 dead deliveries") {
		t.Errorf("output = %q", out.String())
	}
	if countStatus(t, st, "account-a", store.StatusPending) != 2 || countStatus(t, st, "account-b", store.StatusDead) != 1 {
		t.Error("replay touched the wrong deliveries")
	}
}

func TestRunRejectsBadArgumentsWithoutTouchingData(t *testing.T) {
	st := store.New(testpg.New(t))
	seedDead(t, st, "account-a", 1)
	if err := replay.Run(context.Background(), st, nil, &bytes.Buffer{}); err == nil {
		t.Fatal("want an error for an empty filter")
	}
	if countStatus(t, st, "account-a", store.StatusDead) != 1 {
		t.Error("data changed despite the error")
	}
}
