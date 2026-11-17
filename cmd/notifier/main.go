// Command notifier runs the webhook delivery service. Flags pick the roles
// this process runs: --dispatcher, --worker (claim loop + worker pool) and
// --reaper. Any combination is allowed; replicas are stateless.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-notifier/internal/config"
	"webhook-notifier/internal/metrics"
	"webhook-notifier/internal/partnerguard"
	"webhook-notifier/internal/reaper"
	"webhook-notifier/internal/replay"
	"webhook-notifier/internal/scheduler"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/worker"
	"webhook-notifier/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Admin subcommand: notifier replay [--account ...] [--webhook ...] [--from ...] [--to ...] [--all]
	if len(os.Args) > 1 && os.Args[1] == "replay" {
		if err := runReplay(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "replay:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		logger.Error("notifier exited", "err", err)
		os.Exit(1)
	}
}

func runReplay(args []string) error {
	cfg, err := config.Load(nil, os.Getenv) // only DATABASE_URL is used
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	return replay.Run(ctx, store.New(pool), args, os.Stdout)
}

func run() error {
	cfg, err := config.FromOS()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrations.Apply(ctx, pool); err != nil {
		return err
	}

	m, metricsHandler, err := metrics.NewPrometheus(metrics.Options{Tier: cfg.Tier})
	if err != nil {
		return err
	}
	st := store.New(pool)
	sources := metrics.Sources{
		Backlog: func(ctx context.Context) (metrics.Backlog, error) {
			b, err := st.Backlog(ctx)
			return metrics.Backlog{Pending: b.Pending, ActiveAccounts: b.ActiveAccounts, OldestDueAge: b.OldestDueAge}, err
		},
		DBPoolInUse: func() int64 { return int64(pool.Stat().AcquiredConns()) },
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: httpHandler(pool, metricsHandler), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "err", err)
			stop()
		}
	}()

	slog.Info("notifier started",
		"dispatcher", cfg.Dispatcher, "worker", cfg.Worker, "reaper", cfg.Reaper, "http_addr", cfg.HTTPAddr)
	if cfg.Dispatcher {
		// The dispatcher needs an event source shared with the producer process;
		// the Postgres-backed queue arrives with the producer (step 8).
		slog.Warn("dispatcher role idle: no cross-process event source yet")
	}
	// Roles run until ctx is cancelled; Wait returns once each has finished
	// its in-flight work.
	var roles sync.WaitGroup
	if cfg.Worker {
		if cfg.AllowHTTP || cfg.AllowPrivateNetworks {
			slog.Warn("partner URL safety checks relaxed; local development only",
				"allow_http", cfg.AllowHTTP, "allow_private_networks", cfg.AllowPrivateNetworks)
		}
		breaker := partnerguard.NewBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown, nil)
		sender := worker.NewSender(worker.SenderOptions{
			Timeout: cfg.HTTPTimeout, AllowHTTP: cfg.AllowHTTP, AllowPrivateNetworks: cfg.AllowPrivateNetworks,
		})
		w := worker.New(st, sender, worker.Options{
			Backoff: worker.Backoff{
				Base: cfg.BackoffBase, Factor: cfg.BackoffFactor, Max: cfg.BackoffMax, Jitter: cfg.JitterRatio,
			},
			MaxAttempts: cfg.MaxAttempts,
			Limiter:     partnerguard.NewLimiter(cfg.PerHostConcurrency),
			Breaker:     breaker,
			Metrics:     m,
		})
		loop := scheduler.New(st, func(ctx context.Context, d store.Delivery) {
			if _, err := w.Process(ctx, d); err != nil {
				// The lease expires and the reaper retries the delivery.
				slog.Error("record delivery outcome", "delivery_id", d.ID.String(), "err", err)
			}
		}, scheduler.Options{
			Workers: cfg.Workers, AccountsPerTick: cfg.AccountsPerTick, Quantum: cfg.Quantum,
			Lease: cfg.LeaseDuration, Idle: cfg.ClaimInterval, Metrics: m,
		})
		sources.WorkerSlotsInUse = loop.InFlight
		sources.OpenCircuits = breaker.OpenHosts
		roles.Go(func() {
			if err := loop.Run(ctx); err != nil {
				slog.Error("claim loop", "err", err)
			}
		})
	}

	if cfg.Reaper {
		r := reaper.New(st, cfg.MaxAttempts, cfg.ReaperBatch, cfg.ReaperInterval, m)
		roles.Go(func() {
			if err := r.Run(ctx); err != nil {
				slog.Error("reaper", "err", err)
			}
		})
	}

	if err := m.Observe(sources); err != nil {
		return err
	}

	<-ctx.Done()
	slog.Info("shutting down: stopped claiming, waiting for in-flight deliveries")
	roles.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// connect retries until the database accepts connections, so replicas can
// start before Postgres is ready.
func connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return pool, nil
		}
		slog.Warn("waiting for database", "err", err)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("connect to database: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func httpHandler(pool *pgxpool.Pool, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metricsHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	return mux
}
