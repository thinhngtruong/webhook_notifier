package replay

import (
	"strings"
	"testing"
	"time"

	"webhook-notifier/internal/store"
)

func TestParseFlags(t *testing.T) {
	hook := "00000000-0000-4000-8000-0000000000aa"
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

	f, err := ParseFlags([]string{"--account=account-1", "--webhook=" + hook,
		"--from=2026-01-01T00:00:00Z", "--to=2026-01-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if f.AccountID != "account-1" || f.WebhookID == nil || f.WebhookID.String() != hook || !f.From.Equal(from) || !f.To.Equal(to) {
		t.Fatalf("filter = %+v", f)
	}

	if f, err := ParseFlags([]string{"--all"}); err != nil || f != (store.ReplayFilter{}) {
		t.Fatalf("--all = %+v, %v; want empty filter", f, err)
	}

	errs := []struct {
		args []string
		want string
	}{
		{nil, "--all"},
		{[]string{"--webhook=nope"}, "webhook"},
		{[]string{"--from=yesterday"}, "from"},
		{[]string{"--from=2026-01-02T00:00:00Z", "--to=2026-01-01T00:00:00Z"}, "before"},
	}
	for _, e := range errs {
		if _, err := ParseFlags(e.args); err == nil || !strings.Contains(err.Error(), e.want) {
			t.Errorf("ParseFlags(%v) err = %v, want mention of %q", e.args, err, e.want)
		}
	}
}
