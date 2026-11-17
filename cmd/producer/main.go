// Command producer is the fake event source. It seeds one webhook per
// account (pointing at the fake partner) and publishes subscriber events
// into the events queue: a steady rate (default) or one whale burst.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/producer"
	"webhook-notifier/internal/store"
	"webhook-notifier/internal/tracing"
	"webhook-notifier/migrations"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("producer", "err", err)
		os.Exit(1)
	}
}

func run() error {
	dbURL := flag.String("database-url", env("DATABASE_URL", "postgres://notifier:notifier@localhost:55432/notifier?sslmode=disable"), "PostgreSQL (DATABASE_URL)")
	partnerURL := flag.String("partner-url", env("PARTNER_URL", "http://localhost:9090"), "fake partner base URL (PARTNER_URL)")
	secret := flag.String("secret", env("WEBHOOK_SECRET", "local-secret"), "webhook secret to seed (WEBHOOK_SECRET)")
	mode := flag.String("mode", "steady", "steady or whale")
	rate := flag.Int("rate", 20, "steady: events per second")
	accounts := flag.Int("accounts", 50, "steady: small accounts; whale: small accounts beside the whale")
	duration := flag.Duration("duration", 0, "steady: how long (0 = until stopped)")
	whaleEvents := flag.Int("whale-events", 100000, "whale: events for the whale")
	smallEvents := flag.Int("small-events", 10, "whale: events per small account")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTracing, _, err := tracing.Setup(ctx, "webhook-producer", os.Getenv)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()
	pool, err := connect(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		return err
	}
	st := store.New(pool)
	queue := event.NewPGQueue(pool, 30*time.Second, 5)
	urlFor := func(account string) string { return strings.TrimSuffix(*partnerURL, "/") + "/hooks/" + account }

	switch *mode {
	case "whale":
		b := producer.WhaleBurst(*whaleEvents, *accounts, *smallEvents, time.Now())
		if err := producer.SeedWebhooks(ctx, st, append([]string{producer.WhaleAccount}, b.SmallAccounts...), urlFor, *secret); err != nil {
			return err
		}
		for _, batch := range chunks(append(b.Whale, b.Small...), 1000) {
			if err := queue.Publish(ctx, batch...); err != nil {
				return err
			}
		}
		slog.Info("whale burst published", "whale_events", len(b.Whale), "small_events", len(b.Small))
		return nil
	case "steady":
		names := make([]string, *accounts)
		for i := range names {
			names[i] = producer.SmallAccount(i)
		}
		if err := producer.SeedWebhooks(ctx, st, names, urlFor, *secret); err != nil {
			return err
		}
		d := *duration
		if d == 0 {
			d = 100 * 365 * 24 * time.Hour
		}
		slog.Info("publishing steady traffic", "rate", *rate, "accounts", *accounts)
		n, err := producer.Steady(ctx, queue, *rate, d, names, func(string) string { return "small" })
		slog.Info("producer stopped", "published", n)
		return err
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
}

func chunks(es []event.Event, size int) [][]event.Event {
	var out [][]event.Event
	for len(es) > 0 {
		n := min(size, len(es))
		out = append(out, es[:n])
		es = es[n:]
	}
	return out
}

func connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	for {
		if err := pool.Ping(ctx); err == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
