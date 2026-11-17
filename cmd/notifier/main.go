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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-notifier/internal/config"
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
	for role, on := range map[string]bool{"dispatcher": cfg.Dispatcher, "worker": cfg.Worker, "reaper": cfg.Reaper} {
		if on {
			slog.Warn("role not implemented yet", "role", role)
		}
	}

	<-ctx.Done()
	slog.Info("shutting down")

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
