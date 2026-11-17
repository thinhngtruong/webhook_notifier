// Package event defines the source of subscriber events and an in-memory
// implementation used by tests and single-process runs.
package event

import (
	"context"
	"time"
)

// Event is one subscriber event from the platform.
type Event struct {
	ID        string // unique per event; redeliveries keep it
	Name      string // e.g. "subscriber.created"
	AccountID string
	Time      time.Time // when the event happened; drives partitioning and latency
	Payload   []byte    // JSON body POSTed to partners
}

// Source is an at-least-once event queue. Received events that are not
// acked are delivered again later.
type Source interface {
	// Receive returns up to max events without blocking; empty means none
	// are available right now.
	Receive(ctx context.Context, max int) ([]Event, error)
	// Ack removes events (by ID) from the queue for good.
	Ack(ctx context.Context, ids []string) error
}
