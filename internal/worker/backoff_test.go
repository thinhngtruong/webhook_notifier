package worker

import (
	"testing"
	"time"
)

var designBackoff = Backoff{Base: 30 * time.Second, Factor: 3, Max: 8 * time.Hour, Jitter: 0.2}

func TestBackoffDelayWithoutJitter(t *testing.T) {
	tests := []struct {
		failures int
		want     time.Duration
	}{
		{1, 30 * time.Second},
		{2, 90 * time.Second},
		{3, 270 * time.Second},
		{4, 810 * time.Second},
		{5, 2430 * time.Second},
		{6, 7290 * time.Second},
		{7, 21870 * time.Second},
		{8, 8 * time.Hour}, // 65610s capped
		{9, 8 * time.Hour},
		{200, 8 * time.Hour}, // no overflow
	}
	for _, tt := range tests {
		if got := designBackoff.Delay(tt.failures, 0.5); got != tt.want {
			t.Errorf("Delay(%d) = %v, want %v", tt.failures, got, tt.want)
		}
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	tests := []struct {
		name     string
		failures int
		r        float64
		want     time.Duration
	}{
		{"lowest", 1, 0, 24 * time.Second},
		{"highest", 1, 1, 36 * time.Second},
		{"capped then jittered low", 9, 0, time.Duration(0.8 * float64(8*time.Hour))},
		{"capped then jittered high", 9, 1, time.Duration(1.2 * float64(8*time.Hour))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := designBackoff.Delay(tt.failures, tt.r); got != tt.want {
				t.Errorf("Delay(%d, %v) = %v, want %v", tt.failures, tt.r, got, tt.want)
			}
		})
	}
}

// Ten attempts (nine retries) should span roughly the 24h retry window.
func TestBackoffTotalWindowIsAboutADay(t *testing.T) {
	var total time.Duration
	for n := 1; n <= 9; n++ {
		total += designBackoff.Delay(n, 0.5)
	}
	if total < 22*time.Hour || total > 27*time.Hour {
		t.Fatalf("total retry window = %v, want about 24h", total)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in     string
		want   time.Duration
		wantOK bool
	}{
		{"120", 2 * time.Minute, true},
		{"0", 0, true},
		{"Thu, 01 Jan 2026 12:05:00 GMT", 5 * time.Minute, true},
		{"Thu, 01 Jan 2026 11:00:00 GMT", 0, true}, // in the past: retry now
		{"", 0, false},
		{"-5", 0, false},
		{"soon", 0, false},
	}
	for _, tt := range tests {
		got, ok := ParseRetryAfter(tt.in, now)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("ParseRetryAfter(%q) = %v, %v; want %v, %v", tt.in, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestRetryDelayHonoursRetryAfter(t *testing.T) {
	tests := []struct {
		name       string
		failures   int
		retryAfter time.Duration
		want       time.Duration
	}{
		{"no header uses backoff", 1, 0, 30 * time.Second},
		{"shorter header keeps backoff", 2, 10 * time.Second, 90 * time.Second},
		{"longer header wins", 1, time.Hour, time.Hour},
		{"header capped at max", 1, 48 * time.Hour, 8 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := designBackoff.RetryDelay(tt.failures, 0.5, tt.retryAfter); got != tt.want {
				t.Errorf("RetryDelay = %v, want %v", got, tt.want)
			}
		})
	}
}
