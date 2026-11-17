// Command partner is the fake webhook receiver with configurable latency, errors and outages.
// It is a placeholder until step 8 of the implementation plan.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	slog.Warn("partner is not implemented yet; idling until stopped")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
