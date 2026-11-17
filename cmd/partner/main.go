// Command partner is the fake webhook receiver: POST /hooks/{endpoint}
// with configurable latency and error rate, GET /stats for what it saw,
// and POST /admin/outage?endpoint=<name>&for=<duration> to fail an endpoint.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhook-notifier/internal/partner"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	addr := flag.String("addr", ":9090", "listen address")
	latency := flag.Duration("latency", 50*time.Millisecond, "added to every response")
	errorRate := flag.Float64("error-rate", 0.01, "fraction of requests answered 503")
	secret := flag.String("secret", os.Getenv("WEBHOOK_SECRET"), "verify signatures with this secret (WEBHOOK_SECRET); empty: skip")
	flag.Parse()

	p := partner.New(partner.Options{Latency: *latency, ErrorRate: *errorRate, Secret: *secret})
	srv := &http.Server{Addr: *addr, Handler: p.Handler(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("partner listening", "addr", *addr, "latency", latency.String(), "error_rate", *errorRate, "verify_signatures", *secret != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("partner", "err", err)
		os.Exit(1)
	}
}
