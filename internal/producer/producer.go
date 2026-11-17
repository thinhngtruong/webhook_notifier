// Package producer generates fake subscriber events for local runs and
// benchmarks: a whale burst mixed with small accounts, or a steady rate.
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/store"
)

// WhaleAccount is the account ID the whale burst uses.
const WhaleAccount = "account-whale"

// Publisher accepts events (event.PGQueue in practice).
type Publisher interface {
	Publish(ctx context.Context, events ...event.Event) error
}

// NewEvent returns a subscriber.created event whose payload carries the
// event time and tier, which the fake partner reads to measure latency.
func NewEvent(id, accountID, tier string, at time.Time) event.Event {
	payload, _ := json.Marshal(map[string]any{
		"id":         id,
		"type":       "subscriber.created",
		"account_id": accountID,
		"event_time": at.UTC().Format(time.RFC3339Nano),
		"tier":       tier,
		"subscriber": map[string]string{"email": "subscriber-" + id + "@acme.example"},
	})
	return event.Event{ID: id, Name: "subscriber.created", AccountID: accountID, Time: at, Payload: payload}
}

// Burst is a whale burst plus small accounts' events.
type Burst struct {
	Whale []event.Event
	Small []event.Event
	// SmallAccounts lists the small account IDs, for seeding webhooks.
	SmallAccounts []string
}

// SmallAccount is the ID of the i-th small account.
func SmallAccount(i int) string { return fmt.Sprintf("account-%05d", i) }

// WhaleBurst builds whaleEvents for WhaleAccount (tier "large") and
// smallEvents for each of smallAccounts accounts (tier "small").
func WhaleBurst(whaleEvents, smallAccounts, smallEvents int, at time.Time) Burst {
	b := Burst{
		Whale:         make([]event.Event, whaleEvents),
		Small:         make([]event.Event, 0, smallAccounts*smallEvents),
		SmallAccounts: make([]string, smallAccounts),
	}
	for i := range b.Whale {
		b.Whale[i] = NewEvent(fmt.Sprintf("whale-%d", i), WhaleAccount, "large", at)
	}
	for a := range smallAccounts {
		account := SmallAccount(a)
		b.SmallAccounts[a] = account
		for i := range smallEvents {
			b.Small = append(b.Small, NewEvent(fmt.Sprintf("%s-%d", account, i), account, "small", at))
		}
	}
	return b
}

// tick is how often Steady publishes a batch.
const tick = 10 * time.Millisecond

// Steady publishes rate events per second for d (or until ctx ends),
// round-robin over accounts, and returns how many it published. Event
// times are the publish time, so latency includes any ingest backlog.
func Steady(ctx context.Context, pub Publisher, rate int, d time.Duration, accounts []string, tier func(string) string) (int, error) {
	start := time.Now()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	published, next := 0, 0
	for {
		elapsed := time.Since(start)
		if elapsed >= d {
			return published, nil
		}
		// Catch up to where the rate says we should be, so slow ticks do
		// not lower the rate.
		due := int(elapsed.Seconds()*float64(rate)) - published
		if due > 0 {
			now := time.Now()
			batch := make([]event.Event, due)
			for i := range batch {
				account := accounts[next%len(accounts)]
				batch[i] = NewEvent(fmt.Sprintf("steady-%d-%d", start.UnixNano(), published+i), account, tier(account), now)
				next++
			}
			if err := pub.Publish(ctx, batch...); err != nil {
				return published, err
			}
			published += due
		}
		select {
		case <-ctx.Done():
			return published, nil
		case <-ticker.C:
		}
	}
}

// SeedWebhooks registers one active webhook per account, subscribed to
// subscriber.created and pointing at urlFor(account).
func SeedWebhooks(ctx context.Context, st *store.Store, accounts []string, urlFor func(string) string, secret string) error {
	for _, a := range accounts {
		existing, err := st.ActiveWebhooks(ctx, a)
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			continue // already seeded: a second webhook would duplicate deliveries
		}
		if _, err := st.InsertWebhook(ctx, store.Webhook{
			AccountID: a, PostURL: urlFor(a), Events: []string{"subscriber.created"}, Secret: secret,
		}); err != nil {
			return fmt.Errorf("seed webhook for %s: %w", a, err)
		}
	}
	return nil
}
