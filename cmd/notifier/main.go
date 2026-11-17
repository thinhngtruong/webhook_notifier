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
	"webhook-notifier/internal/scheduler"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/worker"
	"webhook-notifier/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(); err != nil {
		logger.Error("notifier exited", "err", err)
		os.Exit(1)
	}
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

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: healthHandler(pool), ReadHeaderTimeout: 5 * time.Second}
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
	if cfg.Reaper {
		slog.Warn("role not implemented yet", "role", "reaper")
	}

	// Roles run until ctx is cancelled; Wait returns once each has finished
	// its in-flight work.
	var roles sync.WaitGroup
	if cfg.Worker {
		st := store.New(pool)
		w := worker.New(st, worker.NewSender(cfg.HTTPTimeout, nil), worker.Options{
			Backoff: worker.Backoff{
				Base: cfg.BackoffBase, Factor: cfg.BackoffFactor, Max: cfg.BackoffMax, Jitter: cfg.JitterRatio,
			},
			MaxAttempts: cfg.MaxAttempts,
		})
		loop := scheduler.New(st, func(ctx context.Context, d store.Delivery) {
			if _, err := w.Process(ctx, d); err != nil {
				// The lease expires and the reaper retries the delivery.
				slog.Error("record delivery outcome", "delivery_id", d.ID.String(), "err", err)
			}
		}, scheduler.Options{
			Workers: cfg.Workers, AccountsPerTick: cfg.AccountsPerTick, Quantum: cfg.Quantum,
			Lease: cfg.LeaseDuration, Idle: cfg.ClaimInterval,
		})
		roles.Go(func() {
			if err := loop.Run(ctx); err != nil {
				slog.Error("claim loop", "err", err)
			}
		})
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

func healthHandler(pool *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()
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
