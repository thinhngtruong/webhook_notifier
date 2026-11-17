package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaultsMatchDesign(t *testing.T) {
	c, err := Load(nil, env(nil))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AccountsPerTick != 50 || c.Quantum != 10 {
		t.Errorf("fairness defaults = K %d, Q %d; want 50, 10", c.AccountsPerTick, c.Quantum)
	}
	if c.BackoffBase != 30*time.Second || c.BackoffFactor != 3 || c.BackoffMax != 8*time.Hour {
		t.Errorf("backoff defaults = %v ×%v cap %v", c.BackoffBase, c.BackoffFactor, c.BackoffMax)
	}
	if c.MaxAttempts != 10 || c.JitterRatio != 0.2 {
		t.Errorf("attempts/jitter = %d/%v", c.MaxAttempts, c.JitterRatio)
	}
	if c.LeaseDuration != 30*time.Second || c.HTTPTimeout != 10*time.Second || c.ReaperInterval != 10*time.Second {
		t.Errorf("lease/timeout/reaper = %v/%v/%v", c.LeaseDuration, c.HTTPTimeout, c.ReaperInterval)
	}
}

func TestLoadPrecedence(t *testing.T) {
	c, err := Load(
		[]string{"--quantum=7", "--worker"},
		env(map[string]string{"QUANTUM": "3", "ACCOUNTS_PER_TICK": "20", "BACKOFF_MAX": "1h", "ROLE_REAPER": "true"}),
	)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Quantum != 7 {
		t.Errorf("flag should override env: quantum = %d", c.Quantum)
	}
	if c.AccountsPerTick != 20 || c.BackoffMax != time.Hour {
		t.Errorf("env not applied: K=%d max=%v", c.AccountsPerTick, c.BackoffMax)
	}
	if !c.Worker || !c.Reaper || c.Dispatcher {
		t.Errorf("roles = dispatcher %v worker %v reaper %v", c.Dispatcher, c.Worker, c.Reaper)
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"bad env int", nil, map[string]string{"QUANTUM": "ten"}, "QUANTUM"},
		{"bad env duration", nil, map[string]string{"LEASE_DURATION": "soon"}, "LEASE_DURATION"},
		{"zero quantum", []string{"--quantum=0"}, nil, "quantum"},
		{"lease not longer than timeout", []string{"--lease=10s", "--http-timeout=10s"}, nil, "lease duration"},
		{"jitter out of range", []string{"--jitter=1"}, nil, "jitter"},
		{"max below base", []string{"--backoff-base=1h", "--backoff-max=1m"}, nil, "backoff max"},
		{"unknown flag", []string{"--nope"}, nil, "nope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(tt.args, env(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want mention of %q", err, tt.want)
			}
		})
	}
}
