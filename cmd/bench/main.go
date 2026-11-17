// Command bench runs the benchmark scenarios from docs/design.md section 6
// and prints a report. Each scenario uses its own throwaway database on the
// given Postgres server (the compose one by default).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"webhook-notifier/internal/bench"
)

func main() {
	dbURL := flag.String("database-url", envOr("DATABASE_URL", "postgres://notifier:notifier@localhost:55432/notifier?sslmode=disable"),
		"Postgres server; the user must be allowed to create databases (DATABASE_URL)")
	scaleName := flag.String("scale", "default", "quick, default or full (design.md sizes; takes hours)")
	verbose := flag.Bool("v", false, "show notifier logs")
	flag.Parse()

	if !*verbose {
		// Retries, postponements and the simulated crash log a lot; the
		// report is the output.
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	sc, ok := map[string]bench.Scale{"quick": bench.Quick, "default": bench.Default, "full": bench.Full}[*scaleName]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scale %q\n", *scaleName)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	step := func(name string) { fmt.Fprintf(os.Stderr, "running %s...\n", name) }
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
	step("whale burst (shared queue, then fair scheduler)")
	f, err := bench.RunFairness(ctx, *dbURL, sc)
	if err != nil {
		fail(err)
	}
	step("horizontal scaling")
	s, err := bench.RunScaling(ctx, *dbURL, sc)
	if err != nil {
		fail(err)
	}
	step("reliability (partner errors + outage)")
	r, err := bench.RunReliability(ctx, *dbURL, sc)
	if err != nil {
		fail(err)
	}
	step("replica crash")
	c, err := bench.RunCrash(ctx, *dbURL, sc)
	if err != nil {
		fail(err)
	}
	fmt.Println()
	bench.Report(os.Stdout, sc, f, s, r, c)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
