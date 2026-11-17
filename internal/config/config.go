// Package config loads notifier settings from environment variables, with
// command-line flags taking precedence. Defaults follow docs/design.md.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every tunable of the notifier. Durations are Go durations
// ("30s", "8h") in both env vars and flags.
type Config struct {
	DatabaseURL string
	HTTPAddr    string // /healthz and, later, /metrics

	// Roles enabled in this process.
	Dispatcher bool
	Worker     bool
	Reaper     bool

	// Dispatcher.
	DispatchBatchSize int           // events per fan-out transaction
	WebhookCacheTTL   time.Duration // per-account webhook cache

	// Fair scheduler.
	AccountsPerTick int           // K: accounts picked per claim pass
	Quantum         int           // Q: max deliveries per account per pass
	Workers         int           // worker pool size (bounds claims)
	ClaimInterval   time.Duration // idle sleep between empty claim passes
	LeaseDuration   time.Duration

	// Retries.
	BackoffBase   time.Duration
	BackoffFactor float64
	BackoffMax    time.Duration
	MaxAttempts   int
	JitterRatio   float64

	// HTTP delivery.
	HTTPTimeout time.Duration

	// Reaper.
	ReaperInterval time.Duration
}

// Default returns the configuration described in docs/design.md.
func Default() Config {
	return Config{
		DatabaseURL:       "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable",
		HTTPAddr:          ":8080",
		DispatchBatchSize: 500,
		WebhookCacheTTL:   30 * time.Second,
		AccountsPerTick:   50,
		Quantum:           10,
		Workers:           64,
		ClaimInterval:     100 * time.Millisecond,
		LeaseDuration:     30 * time.Second,
		BackoffBase:       30 * time.Second,
		BackoffFactor:     3,
		BackoffMax:        8 * time.Hour,
		MaxAttempts:       10,
		JitterRatio:       0.2,
		HTTPTimeout:       10 * time.Second,
		ReaperInterval:    10 * time.Second,
	}
}

// Load builds a Config from defaults, then the environment (via getenv),
// then flags parsed from args. It validates the result.
func Load(args []string, getenv func(string) string) (Config, error) {
	c := Default()
	if err := c.applyEnv(getenv); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("notifier", flag.ContinueOnError)
	fs.StringVar(&c.DatabaseURL, "database-url", c.DatabaseURL, "PostgreSQL connection string (DATABASE_URL)")
	fs.StringVar(&c.HTTPAddr, "http-addr", c.HTTPAddr, "listen address for /healthz and /metrics (HTTP_ADDR)")
	fs.BoolVar(&c.Dispatcher, "dispatcher", c.Dispatcher, "run the dispatcher role")
	fs.BoolVar(&c.Worker, "worker", c.Worker, "run the claim loop and worker pool")
	fs.BoolVar(&c.Reaper, "reaper", c.Reaper, "run the lease reaper")
	fs.IntVar(&c.DispatchBatchSize, "dispatch-batch", c.DispatchBatchSize, "events per fan-out transaction (DISPATCH_BATCH_SIZE)")
	fs.DurationVar(&c.WebhookCacheTTL, "webhook-cache-ttl", c.WebhookCacheTTL, "per-account webhook cache TTL (WEBHOOK_CACHE_TTL)")
	fs.IntVar(&c.AccountsPerTick, "accounts-per-tick", c.AccountsPerTick, "accounts picked per claim pass (ACCOUNTS_PER_TICK)")
	fs.IntVar(&c.Quantum, "quantum", c.Quantum, "max deliveries per account per pass (QUANTUM)")
	fs.IntVar(&c.Workers, "workers", c.Workers, "worker pool size (WORKERS)")
	fs.DurationVar(&c.ClaimInterval, "claim-interval", c.ClaimInterval, "sleep after an empty claim pass (CLAIM_INTERVAL)")
	fs.DurationVar(&c.LeaseDuration, "lease", c.LeaseDuration, "delivery lease duration (LEASE_DURATION)")
	fs.DurationVar(&c.BackoffBase, "backoff-base", c.BackoffBase, "first retry delay (BACKOFF_BASE)")
	fs.Float64Var(&c.BackoffFactor, "backoff-factor", c.BackoffFactor, "retry delay multiplier (BACKOFF_FACTOR)")
	fs.DurationVar(&c.BackoffMax, "backoff-max", c.BackoffMax, "retry delay cap (BACKOFF_MAX)")
	fs.IntVar(&c.MaxAttempts, "max-attempts", c.MaxAttempts, "attempts before a delivery is dead (MAX_ATTEMPTS)")
	fs.Float64Var(&c.JitterRatio, "jitter", c.JitterRatio, "retry delay jitter ratio, 0.2 = ±20% (JITTER_RATIO)")
	fs.DurationVar(&c.HTTPTimeout, "http-timeout", c.HTTPTimeout, "partner request timeout (HTTP_TIMEOUT)")
	fs.DurationVar(&c.ReaperInterval, "reaper-interval", c.ReaperInterval, "how often expired leases are reset (REAPER_INTERVAL)")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// FromOS is Load with the process arguments and environment.
func FromOS() (Config, error) {
	return Load(os.Args[1:], os.Getenv)
}

func (c *Config) applyEnv(getenv func(string) string) error {
	var errs []error
	str := func(key string, dst *string) {
		if v := getenv(key); v != "" {
			*dst = v
		}
	}
	boolean := func(key string, dst *bool) {
		if v := getenv(key); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = b
		}
	}
	integer := func(key string, dst *int) {
		if v := getenv(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = n
		}
	}
	float := func(key string, dst *float64) {
		if v := getenv(key); v != "" {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = f
		}
	}
	duration := func(key string, dst *time.Duration) {
		if v := getenv(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = d
		}
	}

	str("DATABASE_URL", &c.DatabaseURL)
	str("HTTP_ADDR", &c.HTTPAddr)
	boolean("ROLE_DISPATCHER", &c.Dispatcher)
	boolean("ROLE_WORKER", &c.Worker)
	boolean("ROLE_REAPER", &c.Reaper)
	integer("DISPATCH_BATCH_SIZE", &c.DispatchBatchSize)
	duration("WEBHOOK_CACHE_TTL", &c.WebhookCacheTTL)
	integer("ACCOUNTS_PER_TICK", &c.AccountsPerTick)
	integer("QUANTUM", &c.Quantum)
	integer("WORKERS", &c.Workers)
	duration("CLAIM_INTERVAL", &c.ClaimInterval)
	duration("LEASE_DURATION", &c.LeaseDuration)
	duration("BACKOFF_BASE", &c.BackoffBase)
	float("BACKOFF_FACTOR", &c.BackoffFactor)
	duration("BACKOFF_MAX", &c.BackoffMax)
	integer("MAX_ATTEMPTS", &c.MaxAttempts)
	float("JITTER_RATIO", &c.JitterRatio)
	duration("HTTP_TIMEOUT", &c.HTTPTimeout)
	duration("REAPER_INTERVAL", &c.ReaperInterval)

	if len(errs) > 0 {
		return fmt.Errorf("config: invalid environment: %w", errors.Join(errs...))
	}
	return nil
}

// Validate rejects configurations that would break invariants the rest of
// the system relies on.
func (c Config) Validate() error {
	var errs []error
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	check(c.DatabaseURL != "", "database URL is required")
	check(c.DispatchBatchSize > 0, "dispatch batch size must be > 0")
	check(c.WebhookCacheTTL > 0, "webhook cache TTL must be > 0")
	check(c.AccountsPerTick > 0, "accounts per tick must be > 0")
	check(c.Quantum > 0, "quantum must be > 0")
	check(c.Workers > 0, "workers must be > 0")
	check(c.ClaimInterval > 0, "claim interval must be > 0")
	check(c.BackoffBase > 0, "backoff base must be > 0")
	check(c.BackoffFactor >= 1, "backoff factor must be >= 1")
	check(c.BackoffMax >= c.BackoffBase, "backoff max must be >= backoff base")
	check(c.MaxAttempts > 0, "max attempts must be > 0")
	check(c.JitterRatio >= 0 && c.JitterRatio < 1, "jitter ratio must be in [0, 1)")
	check(c.HTTPTimeout > 0, "HTTP timeout must be > 0")
	check(c.ReaperInterval > 0, "reaper interval must be > 0")
	// A lease shorter than the HTTP timeout would let the reaper hand a
	// delivery to a second worker while the first is still sending it.
	check(c.LeaseDuration > c.HTTPTimeout, "lease duration must be longer than the HTTP timeout")
	if len(errs) > 0 {
		return fmt.Errorf("config: %w", errors.Join(errs...))
	}
	return nil
}
